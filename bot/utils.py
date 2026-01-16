import aiohttp
import hmac
import hashlib
import json
import io
import sys
from typing import Optional, Dict, Any
from datetime import datetime

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

class APIClient:
    
    def __init__(self, base_url: str, secret_key: str, timeout: int = 10):
        self.base_url = base_url.rstrip('/')
        self.secret_key = secret_key
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def _ensure_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=self.timeout)
    
    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
    
    def _generate_signature(self, data: str) -> str:
        return hmac.new(
            self.secret_key.encode(),
            data.encode(),
            hashlib.sha256
        ).hexdigest()
    
    async def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        require_auth: bool = True
    ) -> Optional[Dict[str, Any]]:
        await self._ensure_session()
        
        url = f"{self.base_url}{endpoint}"
        headers = {'Content-Type': 'application/json'}
        
        if require_auth:
            json_data = json.dumps(data if data is not None else {})
            signature = self._generate_signature(json_data)
            headers['X-Signature'] = signature
        
        try:
            async with self.session.request(
                method,
                url,
                json=data,
                headers=headers
            ) as response:
                try:
                    response_data = await response.json()
                except:
                    response_data = {'text': await response.text()}
                
                if response.status >= 400:
                    print(f"[API] Error {response.status} on {method} {endpoint}")
                
                return response_data
        except asyncio.TimeoutError:
            print(f"[API] Timeout: {method} {endpoint}")
            return None
        except Exception as e:
            print(f"[API] Error {method} {endpoint}: {e}")
            return None
    
    async def create_key(
        self,
        duration_seconds: int,
        discord_user_id: str
    ) -> Optional[Dict[str, Any]]:
        data = {
            'duration_seconds': duration_seconds,
            'discord_user_id': discord_user_id
        }
        
        response = await self._request('POST', '/admin/create-key', data, require_auth=True)
        
        if response and response.get('key'):
            print(f"[API] Key created: {response['key'][:20]}... for user {discord_user_id}")
        
        return response
    
    async def suspend_key(self, key: str) -> Optional[Dict[str, Any]]:
        data = {'key': key}
        return await self._request('POST', '/admin/suspend-key', data, require_auth=True)
    
    async def unsuspend_key(self, key: str) -> Optional[Dict[str, Any]]:
        data = {'key': key}
        return await self._request('POST', '/admin/unsuspend-key', data, require_auth=True)
    
    async def delete_key(self, key: str) -> Optional[Dict[str, Any]]:
        data = {'key': key}
        return await self._request('POST', '/admin/delete-key', data, require_auth=True)
    
    async def clear_key(self, key: str) -> Optional[Dict[str, Any]]:
        data = {'key': key}
        return await self._request('POST', '/admin/clear-key', data, require_auth=True)

    async def key_info(self, key: str) -> Optional[Dict[str, Any]]:
        data = {'key': key}
        return await self._request('POST', '/admin/key-info', data, require_auth=True)

    async def modify_key(self, key: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        data = {'key': key}
        data.update(payload or {})
        return await self._request('POST', '/admin/modify-key', data, require_auth=True)

    async def merge_keys(self, source_key: str, target_key: str) -> Optional[Dict[str, Any]]:
        data = {'source_key': source_key, 'target_key': target_key}
        return await self._request('POST', '/admin/merge-keys', data, require_auth=True)

    async def manage_blacklist(self, action: str, discord_user_id: Optional[str] = None, duration_seconds: Optional[int] = None) -> Optional[Dict[str, Any]]:
        data: Dict[str, Any] = {'action': action}
        if discord_user_id:
            data['discord_user_id'] = discord_user_id
        if duration_seconds is not None:
            data['duration_seconds'] = duration_seconds
        return await self._request('POST', '/admin/blacklist', data, require_auth=True)

    async def key_stats(self) -> Optional[Dict[str, Any]]:
        data = {}
        return await self._request('POST', '/admin/key-stats', data, require_auth=True)

    async def health(self) -> Optional[Dict[str, Any]]:
        return await self._request('GET', '/health', None, require_auth=False)

    async def upload_script(self, script_text: str, filename: str) -> Optional[Dict[str, Any]]:
        data = {'script': script_text, 'filename': filename}
        return await self._request('POST', '/admin/script/upload', data, require_auth=True)

    async def delete_script(self, filename: str) -> Optional[Dict[str, Any]]:
        data = {'filename': filename}
        return await self._request('POST', '/admin/script/delete', data, require_auth=True)

    async def list_scripts(self) -> Optional[Dict[str, Any]]:
        return await self._request('POST', '/admin/script/list', {}, require_auth=True)

    async def list_keys(self, page_size: int = 100, continuation_token: str = None) -> Optional[Dict[str, Any]]:
        data: Dict[str, Any] = {"page_size": page_size}
        if continuation_token:
            data["continuation_token"] = continuation_token
        return await self._request('POST', '/admin/list-keys', data, require_auth=True)

    async def set_session_tokens(self, enabled: bool) -> Optional[Dict[str, Any]]:
        data = {'enabled': enabled}
        return await self._request('POST', '/admin/session-tokens', data, require_auth=True)

    async def get_session_tokens(self) -> Optional[Dict[str, Any]]:
        data = {}
        return await self._request('POST', '/admin/session-tokens', data, require_auth=True)

    async def prune_expired_keys(self) -> Optional[Dict[str, Any]]:
        data = {}
        return await self._request('POST', '/admin/prune-expired-keys', data, require_auth=True)

    async def update_settings(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return await self._request('POST', '/admin/settings', payload, require_auth=True)

def format_duration(seconds: int) -> str:
    if seconds < 60:
        return f'{seconds} second{"s" if seconds != 1 else ""}'
    elif seconds < 3600:
        minutes = seconds // 60
        return f'{minutes} minute{"s" if minutes != 1 else ""}'
    elif seconds < 86400:
        hours = seconds // 3600
        return f'{hours} hour{"s" if hours != 1 else ""}'
    elif seconds < 604800:
        days = seconds // 86400
        return f'{days} day{"s" if days != 1 else ""}'
    else:
        weeks = seconds // 604800
        return f'{weeks} week{"s" if weeks != 1 else ""}'

import asyncio
