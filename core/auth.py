# core/auth.py
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import jwt
import datetime
import os
import uuid
import logging  # ✅ 新增：导入 logging 模块
from typing import Dict, Any
from core.database import get_conn
from core.config import JWT_SECRET_KEY, JWT_ALGORITHM, JWT_EXPIRE_MINUTES  # ✅ 新增：导入 JWT 配置

# ✅ 新增：配置 logger
logger = logging.getLogger(__name__)

security = HTTPBearer()

# 双认证开关
ENABLE_UUID_AUTH = int(os.getenv("ENABLE_UUID_AUTH", "0"))


async def get_current_user(
        credentials: HTTPAuthorizationCredentials = Depends(security)
) -> Dict[str, Any]:
    """
    用户认证（支持JWT和UUID双认证）
    """
    token = credentials.credentials

    # 开发环境：如果启用UUID认证且token不是JWT格式
    if ENABLE_UUID_AUTH and not token.startswith("eyJ"):
        return await _get_user_from_uuid(token)

    # 生产环境：强制JWT认证
    return await _get_user_from_jwt(token)


async def _get_user_from_jwt(token: str) -> Dict[str, Any]:
    """JWT认证逻辑"""
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=["HS256"])
        user_id = payload.get("user_id")

        if not user_id:
            raise HTTPException(status_code=401, detail="无效的Token")

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, mobile, name, avatar_path, is_merchant, wechat_sub_mchid, status 
                    FROM users WHERE id = %s
                """, (user_id,))
                user = cur.fetchone()

                if not user:
                    raise HTTPException(status_code=401, detail="用户不存在")

                if user.get("status") != 0:
                    raise HTTPException(status_code=401, detail="用户账户异常")

                return user

    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token已过期")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="无效的Token")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"认证失败: {str(e)}")


async def _get_user_from_uuid(token: str) -> Dict[str, Any]:
    """UUID认证逻辑（仅开发环境）"""
    try:
        # 验证UUID格式
        uuid_obj = uuid.UUID(token)

        with get_conn() as conn:
            with conn.cursor() as cur:
                # 查库验证token是否有效
                cur.execute("""
                    SELECT id, mobile, name, avatar_path, is_merchant, wechat_sub_mchid, status 
                    FROM users WHERE token = %s
                """, (str(token),))
                user = cur.fetchone()

                if not user:
                    raise HTTPException(status_code=401, detail="UUID Token无效")

                if user.get("status") != 0:
                    raise HTTPException(status_code=401, detail="用户账户异常")

                logger.info(f"开发环境：UUID认证成功 - 用户 {user['name']}")
                return user

    except ValueError:
        raise HTTPException(status_code=401, detail="UUID格式错误")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"UUID认证失败: {str(e)}")


async def get_current_active_user(current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    """
    获取当前激活状态的用户
    """
    if current_user.get("status") == 1:  # 1是冻结状态
        raise HTTPException(status_code=400, detail="用户已被冻结")
    return current_user


def create_access_token(user_id: int) -> str:
    """创建JWT token"""
    expire = datetime.datetime.utcnow() + datetime.timedelta(minutes=JWT_EXPIRE_MINUTES)
    to_encode = {"user_id": user_id, "exp": expire}
    return jwt.encode(to_encode, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)