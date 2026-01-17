import os

class Config:
    BOT_TOKEN = (os.getenv("BOT_TOKEN") or os.getenv("DISCORD_BOT_TOKEN") or "").strip()
    GUILD_ID = int(os.getenv("GUILD_ID", "0"))
    ADMIN_ROLE_ID = int(os.getenv("ADMIN_ROLE_ID", "0"))
    AUDIT_CHANNEL_ID = int(os.getenv("AUDIT_CHANNEL_ID", "0"))
    API_BASE = os.getenv("API_BASE", "").strip()
    BOT_SECRET = os.getenv("BOT_SECRET", "").strip()

    @classmethod
    def validate(cls):
        if not cls.BOT_TOKEN:
            raise ValueError("BOT_TOKEN or DISCORD_BOT_TOKEN environment variable must be set")
        if not cls.API_BASE:
            raise ValueError("API_BASE environment variable must be set")
        if not cls.BOT_SECRET:
            raise ValueError("BOT_SECRET environment variable must be set")
        if not cls.GUILD_ID or not cls.ADMIN_ROLE_ID:
            raise ValueError("GUILD_ID and ADMIN_ROLE_ID environment variables must be set")
        return True
