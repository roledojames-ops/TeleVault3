package app.televault.data

import android.content.Context
import android.net.Uri
import android.provider.OpenableColumns
import android.webkit.MimeTypeMap
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.withContext
import java.io.File
import java.io.IOException
import java.io.OutputStream
import java.util.Locale
import java.util.UUID

/**
 * The "drive". Files live in your own Telegram chat/channel as ordinary documents
 * (big files are split into chunks); the folder tree and chunk list live in a small
 * JSON index that is kept on the phone and can be backed up to Telegram.
 */
object Vault {
    /** Telegram bots can only download files up to 20 MB, so stay safely below that. */
    const val CHUNK_SIZE = 18L * 1024 * 1024

    const val INDEX_NAME = "televault-index.json"
    private const val INDEX_TAG = "#televault_index"

    private lateinit var app: Context

    lateinit var settings: Settings
        private set

    private val _index = MutableStateFlow(VaultIndex())
    val index: StateFlow<VaultIndex> = _index.asStateFlow()

    private val fileLock = Any()
    private val indexFile: File get() = File(app.filesDir, "index.json")

    fun init(context: Context) {
        app = context.applicationContext
        settings = Settings(app)
        File(app.cacheDir, "open").deleteRecursively()
        File(app.cacheDir, "chunks").deleteRecursively()
        _index.value = try {
            if (indexFile.exists()) IndexJson.fromJson(indexFile.readText()) else VaultIndex()
        } catch (e: Exception) {
            VaultIndex()
        }
    }

    // ---------------------------------------------------------------- helpers

    private fun api(): TelegramApi = TelegramApi(settings.botToken)

    private fun cacheSub(name: String): File = File(app.cacheDir, name).apply { mkdirs() }

    fun openCacheDir(): File = cacheSub("open")

    fun openOutput(uri: Uri): OutputStream =
        app.contentResolver.openOutputStream(uri, "w") ?: throw IOException("Can't write to that location")

    private fun persist() {
        synchronized(fileLock) {
            val tmp = File(app.filesDir, "index.json.tmp")
            tmp.writeText(IndexJson.toJson(_index.value))
            if (!tmp.renameTo(indexFile)) {
                indexFile.delete()
                tmp.renameTo(indexFile)
            }
        }
    }

    private fun queryMeta(uri: Uri): Pair<String, Long> {
        var name: String? = null
        var size = -1L
        app.contentResolver.query(
            uri,
            arrayOf(OpenableColumns.DISPLAY_NAME, OpenableColumns.SIZE),
            null,
            null,
            null,
        )?.use { c ->
            if (c.moveToFirst()) {
                val ni = c.getColumnIndex(OpenableColumns.DISPLAY_NAME)
                if (ni >= 0 && !c.isNull(ni)) name = c.getString(ni)
                val si = c.getColumnIndex(OpenableColumns.SIZE)
                if (si >= 0 && !c.isNull(si)) size = c.getLong(si)
            }
        }
        val finalName = (name ?: uri.lastPathSegment ?: "file").replace('/', '_')
        return finalName to size
    }

    private fun guessMime(name: String): String {
        val ext = name.substringAfterLast('.', "").lowercase(Locale.ROOT)
        return MimeTypeMap.getSingleton().getMimeTypeFromExtension(ext) ?: "application/octet-stream"
    }

    private fun uniqueName(index: VaultIndex, folder: String, wanted: String): String {
        val taken = index.files.filter { it.folder == folder }.map { it.name.lowercase(Locale.ROOT) }.toSet()
        if (wanted.lowercase(Locale.ROOT) !in taken) return wanted
        val dot = wanted.lastIndexOf('.')
        val base = if (dot > 0) wanted.substring(0, dot) else wanted
        val ext = if (dot > 0) wanted.substring(dot) else ""
        var n = 1
        while (true) {
            val candidate = "$base ($n)$ext"
            if (candidate.lowercase(Locale.ROOT) !in taken) return candidate
            n++
        }
    }

    private fun shorten(s: String): String = if (s.length > 180) s.take(177) + "…" else s

    private fun sleepCancelable(ms: Long, cancel: CancelToken) {
        var left = ms
        while (left > 0) {
            if (cancel.cancelled) throw TransferCancelled()
            val step = minOf(200L, left)
            Thread.sleep(step)
            left -= step
        }
    }

    // ---------------------------------------------------------------- upload

    private fun sendWithRetry(
        api: TelegramApi,
        chat: String,
        file: File,
        name: String,
        caption: String,
        cancel: CancelToken,
        onSent: (Long) -> Unit,
    ): SentDoc {
        var attempt = 0
        while (true) {
            var sentThisTry = 0L
            try {
                return api.sendDocument(chat, file, name, caption, cancel) { n ->
                    sentThisTry += n
                    onSent(sentThisTry)
                }
            } catch (e: TelegramException) {
                if (cancel.cancelled) throw TransferCancelled()
                attempt++
                if (e.retryAfter > 0 && attempt <= 6) {
                    sleepCancelable(e.retryAfter * 1000L + 500L, cancel)
                    continue
                }
                throw e
            } catch (e: IOException) {
                if (cancel.cancelled) throw TransferCancelled()
                attempt++
                if (attempt > 4) throw e
                sleepCancelable(1500L * attempt, cancel)
            }
        }
    }

    suspend fun upload(
        uri: Uri,
        folder: String,
        cancel: CancelToken,
        onMeta: (String, Long) -> Unit,
        onProgress: (Long, Long) -> Unit,
    ) {
        withContext(Dispatchers.IO) {
            val (name, size) = queryMeta(uri)
            onMeta(name, size)
            if (size == 0L) throw IOException("\"$name\" is empty")
            val mime = app.contentResolver.getType(uri) ?: guessMime(name)
            val api = api()
            val chat = settings.chatId
            val fileId = UUID.randomUUID().toString().replace("-", "").take(12)
            val tmp = File(cacheSub("chunks"), "up_$fileId.tmp")
            val done = mutableListOf<Chunk>()
            var ok = false
            try {
                val input = app.contentResolver.openInputStream(uri) ?: throw IOException("Can't open \"$name\"")
                val parts = if (size > 0) ((size + CHUNK_SIZE - 1) / CHUNK_SIZE).toInt() else -1
                input.use { ins ->
                    var base = 0L
                    var idx = 0
                    val buf = ByteArray(256 * 1024)
                    while (true) {
                        if (cancel.cancelled) throw TransferCancelled()
                        var written = 0L
                        tmp.outputStream().use { out ->
                            while (written < CHUNK_SIZE) {
                                val want = minOf(buf.size.toLong(), CHUNK_SIZE - written).toInt()
                                val n = ins.read(buf, 0, want)
                                if (n < 0) break
                                out.write(buf, 0, n)
                                written += n
                            }
                        }
                        if (written == 0L) break
                        val single = parts == 1
                        val partName = if (single) name else "$name.part" + (idx + 1).toString().padStart(3, '0')
                        val label = shorten(name)
                        val caption = if (single) {
                            "TeleVault • $label"
                        } else {
                            "TeleVault • $label • part ${idx + 1}" + (if (parts > 0) "/$parts" else "")
                        }
                        val baseNow = base
                        val sent = sendWithRetry(api, chat, tmp, partName, caption, cancel) { s ->
                            onProgress(baseNow + s, size)
                        }
                        done.add(Chunk(idx, sent.messageId, sent.fileId, written))
                        base += written
                        idx++
                        onProgress(base, size)
                    }
                }
                if (done.isEmpty()) throw IOException("\"$name\" is empty")
                val realSize = done.sumOf { it.size }
                val newFile = VaultFile(
                    id = fileId,
                    name = name,
                    size = realSize,
                    mime = mime,
                    folder = folder,
                    createdAt = System.currentTimeMillis(),
                    chunks = done.toList(),
                )
                _index.update { cur ->
                    val unique = uniqueName(cur, folder, name)
                    cur.copy(files = cur.files + newFile.copy(name = unique))
                }
                persist()
                ok = true
            } finally {
                tmp.delete()
                if (!ok && done.isNotEmpty()) {
                    for (c in done) {
                        try {
                            api.deleteMessage(chat, c.messageId)
                        } catch (e: Exception) {
                            // best effort clean-up of a half-finished upload
                        }
                    }
                }
            }
            Unit
        }
    }

    // -------------------------------------------------------------- download

    private fun downloadChunk(
        api: TelegramApi,
        chunk: Chunk,
        dest: File,
        cancel: CancelToken,
        onBytes: (Long) -> Unit,
    ) {
        var attempt = 0
        while (true) {
            try {
                val path = api.getFilePath(chunk.fileId, cancel)
                var got = 0L
                dest.outputStream().use { o ->
                    api.download(path, o, cancel) { n ->
                        got += n
                        onBytes(got)
                    }
                }
                return
            } catch (e: TelegramException) {
                if (cancel.cancelled) throw TransferCancelled()
                attempt++
                if (e.retryAfter > 0 && attempt <= 6) {
                    sleepCancelable(e.retryAfter * 1000L + 500L, cancel)
                    continue
                }
                throw e
            } catch (e: IOException) {
                if (cancel.cancelled) throw TransferCancelled()
                attempt++
                if (attempt > 3) throw e
                sleepCancelable(1500L * attempt, cancel)
            }
        }
    }

    suspend fun download(
        file: VaultFile,
        out: OutputStream,
        cancel: CancelToken,
        onProgress: (Long, Long) -> Unit,
    ) {
        withContext(Dispatchers.IO) {
            val api = api()
            val dir = cacheSub("chunks")
            var base = 0L
            for (c in file.chunks.sortedBy { it.index }) {
                if (cancel.cancelled) throw TransferCancelled()
                val tmp = File(dir, "dl_${file.id}_${c.index}.tmp")
                try {
                    val baseNow = base
                    downloadChunk(api, c, tmp, cancel) { n -> onProgress(baseNow + n, file.size) }
                    tmp.inputStream().use { it.copyTo(out) }
                    base += c.size
                } finally {
                    tmp.delete()
                }
            }
            out.flush()
        }
    }

    // ------------------------------------------------------- index operations

    fun createFolder(parent: String, rawName: String): String? {
        val name = rawName.trim().replace("/", "")
        if (name.isEmpty()) return "Folder name can't be empty"
        val path = Paths.child(parent, name)
        if (_index.value.folders.any { it.equals(path, ignoreCase = true) }) return "That folder already exists"
        _index.update { it.copy(folders = it.folders + path) }
        persist()
        return null
    }

    fun renameFile(id: String, rawName: String): String? {
        val name = rawName.trim().replace("/", "")
        if (name.isEmpty()) return "Name can't be empty"
        _index.update { cur ->
            cur.copy(files = cur.files.map { if (it.id == id) it.copy(name = name) else it })
        }
        persist()
        return null
    }

    fun moveFile(id: String, folder: String) {
        _index.update { cur ->
            val target = cur.files.firstOrNull { it.id == id }
            if (target == null) {
                cur
            } else {
                val unique = uniqueName(cur, folder, target.name)
                cur.copy(files = cur.files.map { if (it.id == id) it.copy(folder = folder, name = unique) else it })
            }
        }
        persist()
    }

    /**
     * [blanked]: messages Telegram refused to delete (usually older than 48 h) whose file was
     * replaced by a tiny placeholder instead. [failed]: messages that are still untouched in the chat.
     */
    data class DeleteReport(val blanked: Int = 0, val failed: Int = 0)

    private fun deleteMessages(files: List<VaultFile>): DeleteReport {
        val api = api()
        val chat = settings.chatId
        var blanked = 0
        var failed = 0
        for (f in files) {
            for (c in f.chunks) {
                try {
                    api.deleteMessage(chat, c.messageId)
                } catch (e: Exception) {
                    try {
                        api.blankMessage(chat, c.messageId)
                        blanked++
                    } catch (e2: Exception) {
                        failed++
                    }
                }
            }
        }
        return DeleteReport(blanked, failed)
    }

    suspend fun deleteFile(id: String): DeleteReport = withContext(Dispatchers.IO) {
        val f = _index.value.files.firstOrNull { it.id == id } ?: return@withContext DeleteReport()
        val report = deleteMessages(listOf(f))
        _index.update { cur -> cur.copy(files = cur.files.filter { it.id != id }) }
        persist()
        report
    }

    suspend fun deleteFolder(path: String): DeleteReport = withContext(Dispatchers.IO) {
        if (path == Paths.ROOT) return@withContext DeleteReport()
        val doomed = _index.value.files.filter { Paths.isInside(it.folder, path) }
        val report = deleteMessages(doomed)
        val ids = doomed.map { it.id }.toSet()
        _index.update { cur ->
            cur.copy(
                folders = cur.folders.filter { !Paths.isInside(it, path) }.toSet(),
                files = cur.files.filter { it.id !in ids },
            )
        }
        persist()
        report
    }

    // ------------------------------------------------------------ index backup

    /** Uploads the index to Telegram and pins it. Returns a warning text if pinning failed. */
    suspend fun backupIndex(): String? = withContext(Dispatchers.IO) {
        val api = api()
        val chat = settings.chatId
        val tmp = File(cacheSub("chunks"), "index_backup.json")
        tmp.writeText(IndexJson.toJson(_index.value))
        try {
            val count = _index.value.files.size
            val sent = api.sendDocument(chat, tmp, INDEX_NAME, "$INDEX_TAG • $count files", null) { }
            var warning: String? = null
            var pinned = false
            try {
                api.pinMessage(chat, sent.messageId)
                pinned = true
            } catch (e: Exception) {
                warning = "Backup uploaded, but it couldn't be pinned (${e.message}). " +
                    "Give the bot permission to pin/edit messages so a reinstall can restore your files."
            }
            val old = settings.lastBackupMsgId
            if (pinned && old != 0L) {
                try {
                    api.deleteMessage(chat, old)
                } catch (e: Exception) {
                    // old backup may be older than 48 h – harmless
                }
            }
            settings.lastBackupMsgId = sent.messageId
            warning
        } finally {
            tmp.delete()
        }
    }

    /** Replaces the local index with the pinned backup from the chat. Returns the number of files. */
    suspend fun restoreIndex(): Int = withContext(Dispatchers.IO) {
        val api = api()
        val chat = settings.chatId
        val fileId = api.getPinnedDocumentId(chat, INDEX_NAME)
            ?: throw IOException("No index backup is pinned in this chat yet")
        val tmp = File(cacheSub("chunks"), "index_restore.json")
        try {
            val path = api.getFilePath(fileId, null)
            tmp.outputStream().use { o -> api.download(path, o, null) { } }
            val restored = IndexJson.fromJson(tmp.readText())
            _index.value = restored
            persist()
            restored.files.size
        } finally {
            tmp.delete()
        }
    }

    fun resetAll() {
        settings.clear()
        _index.value = VaultIndex()
        indexFile.delete()
    }
}
