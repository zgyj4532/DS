# core/push_service.py
"""
微信模板消息推送服务
用于状态变更实时通知
"""
import requests
import json
import datetime
import logging
from typing import Dict, Any, Optional
from core.config import WECHAT_APP_ID, WECHAT_APP_SECRET

logger = logging.getLogger(__name__)


class PushService:
    """推送服务类"""

    def __init__(self):
        self.access_token_url = "https://api.weixin.qq.com/cgi-bin/token"
        self.message_send_url = "https://api.weixin.qq.com/cgi-bin/message/subscribe/send"
        self._access_token = None
        self._token_expires_at = 0

    async def get_access_token(self) -> str:
        """获取小程序access_token"""
        import time
        if self._access_token and time.time() < self._token_expires_at:
            return self._access_token

        try:
            response = requests.get(
                self.access_token_url,
                params={
                    "grant_type": "client_credential",
                    "appid": WECHAT_APP_ID,
                    "secret": WECHAT_APP_SECRET
                },
                timeout=10
            )
            data = response.json()
            self._access_token = data.get("access_token")
            expires_in = data.get("expires_in", 7200)
            self._token_expires_at = time.time() + expires_in - 300  # 提前5分钟刷新
            return self._access_token
        except Exception as e:
            logger.error(f"获取access_token失败: {str(e)}")
            return ""

    async def send_template_message(self, user_id: int, template_id: str, data: Dict[str, Any]) -> bool:
        """发送模板消息"""
        try:
            # 查询用户的openid
            from core.database import get_conn
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT openid FROM users WHERE id = %s", (user_id,))
                    result = cur.fetchone()
                    if not result or not result.get('openid'):
                        logger.warning(f"用户 {user_id} 未绑定openid，无法推送")
                        return False
                    openid = result['openid']

            # 发送消息
            access_token = await self.get_access_token()
            if not access_token:
                return False

            message_data = {
                "touser": openid,
                "template_id": template_id,
                "data": data
            }

            response = requests.post(
                f"{self.message_send_url}?access_token={access_token}",
                json=message_data,
                timeout=10
            )

            result = response.json()
            if result.get("errcode") == 0:
                logger.info(f"推送成功: 用户 {user_id}")
                return True
            else:
                logger.error(f"推送失败: {result}")
                return False

        except Exception as e:
            logger.error(f"发送模板消息异常: {str(e)}")
            return False

    async def send_applyment_status_notification(self, user_id: int, status: str, remark: str):
        """发送进件状态通知"""
        from core.config import PUSH_TEMPLATE_ID_APPLYMENT
        if not PUSH_TEMPLATE_ID_APPLYMENT:
            logger.warning("未配置进件状态推送模板ID")
            return

        # 状态映射
        status_map = {
            "APPLYMENT_STATE_AUDITING": {"value": "审核中", "color": "#ffbe00"},
            "APPLYMENT_STATE_REJECTED": {"value": "已驳回", "color": "#f5222d"},
            "APPLYMENT_STATE_FINISHED": {"value": "已通过", "color": "#52c41a"},
        }

        status_info = status_map.get(status, {"value": "状态变更", "color": "#000000"})

        data = {
            "thing1": {"value": "微信进件审核"},  # 事项
            "phrase2": {"value": status_info["value"], "color": status_info["color"]},  # 审核状态
            "thing3": {"value": remark or "您的进件申请状态已更新"},  # 备注
            "date4": {"value": datetime.datetime.now().strftime("%Y-%m-%d %H:%M")}  # 更新时间
        }

        await self.send_template_message(user_id, PUSH_TEMPLATE_ID_APPLYMENT, data)

    def send_applyment_status_notification_sync(self, user_id: int, status: str, remark: str):
        """同步发送进件状态通知（供定时任务调用）"""
        import asyncio
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        loop.run_until_complete(
            self.send_applyment_status_notification(user_id, status, remark)
        )


# 全局推送服务实例
push_service = PushService()