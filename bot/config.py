import os
from dotenv import load_dotenv

load_dotenv()
class Config:
    BOT_TOKEN = os.getenv('DISCORD_BOT_TOKEN', '')
    GUILD_ID = int(os.getenv('GUILD_ID', ''))
    ADMIN_ROLE_ID = int(os.getenv('ADMIN_ROLE_ID', ''))
    AUDIT_CHANNEL_ID = int(os.getenv('AUDIT_CHANNEL_ID', '')) or None
    API_BASE = os.getenv('API_BASE', 'https://unknownhub.pythonanywhere.com')
    BOT_SECRET = os.getenv('BOT_SECRET', '')
    
    @classmethod
    def validate(cls):
        if not cls.BOT_TOKEN:
            raise ValueError("DISCORD_BOT_TOKEN is required")
        if not cls.BOT_SECRET:
            raise ValueError("BOT_SECRET is required")
        
        return True
