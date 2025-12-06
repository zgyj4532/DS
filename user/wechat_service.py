import requests
import hashlib
import jwt
import datetime
from fastapi import Request, HTTPException
from config import get_conn

# 微信小程序配置
WECHAT_APP_ID = "your_appid"
WECHAT_APP_SECRET = "your_appsecret"



async def wechat_login(request: Request):
    data = await request.json()
    code = data.get('code')
    user_info = data.get('userInfo')
    raw_data = data.get('rawData')
    signature = data.get('signature')

    if not code or not user_info or not raw_data or not signature:
        raise HTTPException(status_code=400, detail="缺少参数")

    # 验证签名（可选，用于确保数据未被篡改）
    expected_signature = hashlib.sha1((raw_data + WECHAT_APP_ID).encode()).hexdigest()
    if signature != expected_signature:
        raise HTTPException(status_code=400, detail="签名验证失败")

    # 调用微信接口，通过code换取openid和session_key
    url = f"https://api.weixin.qq.com/sns/jscode2session?appid={WECHAT_APP_ID}&secret={WECHAT_APP_SECRET}&js_code={code}&grant_type=authorization_code"
    response = requests.get(url)
    if response.status_code != 200:
        raise HTTPException(status_code=500, detail="微信接口调用失败")

    wechat_data = response.json()
    openid = wechat_data.get('openid')
    session_key = wechat_data.get('session_key')

    if not openid or not session_key:
        raise HTTPException(status_code=500, detail="无法获取openid或session_key")

    # 检查用户是否已注册
    user = await check_user_by_openid(openid)
    if not user:
        # 注册新用户
        user_id = await register_user(openid, user_info)
    else:
        user_id = user['id']

    # 生成token并返回
    token = generate_token(user_id)
    return {
        "success": True,
        "user_id": user_id,
        "token": token
    }

async def check_user_by_openid(openid):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM users WHERE openid=%s", (openid,))
            result = cur.fetchone()
            return result

async def register_user(openid, user_info):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users(openid, name, avatar_path, status) VALUES (%s, %s, %s, %s)",
                (openid, user_info['nickName'], user_info['avatarUrl'], 0)
            )
            return cur.lastrowid

def generate_token(user_id):
    payload = {
        "user_id": user_id,
        "exp": datetime.datetime.utcnow() + datetime.timedelta(days=1)
    }
    token = jwt.encode(payload, "your_secret_key", algorithm="HS256")
    return token