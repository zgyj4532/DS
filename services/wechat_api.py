# services/wechat_api.py
import httpx, base64
from core.config import settings

async def get_access_token() -> str:
    """简易 access_token 缓存，7000s"""
    import time
    now = int(time.time())
    if not hasattr(get_access_token, "_cache") or now - get_access_token._cache[1] > 7000:
        url = ("https://api.weixin.qq.com/cgi-bin/token"
               "?grant_type=client_credential"
               f"&appid={settings.WECHAT_APP_ID}"
               f"&secret={settings.WECHAT_APP_SECRET}")
        async with httpx.AsyncClient() as cli:
            ret = await cli.get(url)
            ret.raise_for_status()
            get_access_token._cache = (ret.json()["access_token"], now)
    return get_access_token._cache[0]

async def get_wxacode(path: str, scene: str = "", width: int = 280) -> bytes:
    """获取小程序码二进制"""
    token = await get_access_token()
    url = f"https://api.weixin.qq.com/wxa/getwxacode?access_token={token}"
    body = {"path": path, "scene": scene, "width": width}
    async with httpx.AsyncClient() as cli:
        r = await cli.post(url, json=body)
        r.raise_for_status()
        return r.content