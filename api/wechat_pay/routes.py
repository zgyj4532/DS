# api/wechat_pay/routes.py
from fastapi import APIRouter, Request, HTTPException
from core.wx_pay_client import WeChatPayClient  # ✅ 修复：wechat_pay_client → wx_pay_client，WechatPayClient → WeChatPayClient
from core.response import success_response
from core.database import get_conn
from services.wechat_applyment_service import WechatApplymentService
import json
import logging

router = APIRouter(prefix="/wechat-pay", tags=["微信支付"])

logger = logging.getLogger(__name__)
pay_client = WeChatPayClient()  # ✅ 修复：WechatPayClient → WeChatPayClient


@router.post("/notify", summary="微信支付回调通知")
async def wechat_pay_notify(request: Request):
    """
    处理微信支付异步通知
    1. 验证签名
    2. 解密回调数据
    3. 更新订单/进件状态
    4. 返回成功响应
    """
    try:
        # 获取原始请求体
        body = await request.body()
        headers = request.headers

        # 验证签名
        signature = headers.get("Wechatpay-Signature")
        timestamp = headers.get("Wechatpay-Timestamp")
        nonce = headers.get("Wechatpay-Nonce")
        serial = headers.get("Wechatpay-Serial")

        if not pay_client.verify_signature(signature, timestamp, nonce, body.decode()):
            raise HTTPException(status_code=403, detail="签名验证失败")

        # 解密回调数据
        data = json.loads(body)
        decrypted_data = pay_client.decrypt_callback_data(data.get("resource", {}))

        # 根据事件类型处理
        event_type = decrypted_data.get("event_type")

        if event_type == "APPLYMENT_STATE_CHANGE":
            # 进件状态变更
            await handle_applyment_state_change(decrypted_data)
        elif event_type == "TRANSACTION.SUCCESS":
            # 支付成功
            await handle_transaction_success(decrypted_data)

        return success_response(message="处理成功")

    except Exception as e:
        logger.error(f"微信支付回调处理失败: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


async def handle_applyment_state_change(data: dict):
    """处理进件状态变更回调"""
    applyment_id = data.get("applyment_id")
    state = data.get("applyment_state")

    service = WechatApplymentService()
    await service.handle_applyment_state_change(
        applyment_id,
        state,
        {
            "state_msg": data.get("state_msg"),
            "sub_mchid": data.get("sub_mchid")
        }
    )


async def handle_transaction_success(data: dict):
    """处理支付成功回调"""
    # 实现支付成功后的业务逻辑
    logger.info(f"支付成功回调: {data}")
    pass


def register_wechat_pay_routes(app):
    app.include_router(router)