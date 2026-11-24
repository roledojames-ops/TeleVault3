package app.televault.data

import android.content.Context

/** Small wrapper around app-private SharedPreferences. */
class Settings(context: Context) {
    private val sp = context.getSharedPreferences("televault", Context.MODE_PRIVATE)

    var botToken: String
        get() = sp.getString("token", "") ?: ""
        set(v) { sp.edit().putString("token", v).apply() }

    var chatId: String
        get() = sp.getString("chat", "") ?: ""
        set(v) { sp.edit().putString("chat", v).apply() }

    var botName: String
        get() = sp.getString("botName", "") ?: ""
        set(v) { sp.edit().putString("botName", v).apply() }

    var chatTitle: String
        get() = sp.getString("chatTitle", "") ?: ""
        set(v) { sp.edit().putString("chatTitle", v).apply() }

    var lockEnabled: Boolean
        get() = sp.getBoolean("lock", false)
        set(v) { sp.edit().putBoolean("lock", v).apply() }

    var autoBackup: Boolean
        get() = sp.getBoolean("autoBackup", true)
        set(v) { sp.edit().putBoolean("autoBackup", v).apply() }

    var lastBackupMsgId: Long
        get() = sp.getLong("lastBackupMsg", 0L)
        set(v) { sp.edit().putLong("lastBackupMsg", v).apply() }

    val isConfigured: Boolean
        get() = botToken.isNotBlank() && chatId.isNotBlank()

    fun clear() {
        sp.edit().clear().apply()
    }
}
