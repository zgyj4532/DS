# api/wechat_pay/routes.py
from fastapi import APIRouter, Request, HTTPException
from core.wx_pay_client import WeChatPayClient
from core.response import success_response
from core.database import get_conn
from services.wechat_applyment_service import WechatApplymentService
import json
import logging
import xml.etree.ElementTree as ET  # ✅ 新增：用于生成XML响应

router = APIRouter(prefix="/wechat-pay", tags=["微信支付"])  # ✅ 确认：tags已正确设置

logger = logging.getLogger(__name__)
pay_client = WeChatPayClient()


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
        # ==================== ✅ 关键修复：获取并验证请求体 ====================
        body = await request.body()

        # 检查请求体是否为空（防止JSONDecodeError）
        if not body or len(body.strip()) == 0:
            logger.warning("收到空请求体，返回错误响应")
            return _xml_response("FAIL", "Empty request body")
        # ===================================================================

        headers = request.headers

        # 验证签名
        signature = headers.get("Wechatpay-Signature")
        timestamp = headers.get("Wechatpay-Timestamp")
        nonce = headers.get("Wechatpay-Nonce")
        serial = headers.get("Wechatpay-Serial")

        if not all([signature, timestamp, nonce, serial]):
            logger.error("缺少必要的回调头信息")
            return _xml_response("FAIL", "Missing callback headers")

        # ==================== ✅ 修改：增加签名验证保护 ====================
        try:
            if not pay_client.verify_signature(signature, timestamp, nonce, body.decode()):
                logger.error("签名验证失败")
                return _xml_response("FAIL", "Signature verification failed")
        except Exception as e:
            logger.error(f"签名验证异常: {str(e)}")
            return _xml_response("FAIL", f"Signature error: {str(e)}")
        # ===================================================================

        # ==================== ✅ 新增：支持XML和JSON格式 ====================
        content_type = headers.get("content-type", "")

        # 解析回调数据（真实微信通知是XML格式，MOCK测试时可能是JSON）
        if "xml" in content_type:
            import xmltodict  # 需要安装: pip install xmltodict
            data_dict = xmltodict.parse(body)
            # 提取resource对象（微信XML格式）
            data = data_dict.get("xml", {})
            if "resource" in data:
                resource = data["resource"]
                # 将字符串转为字典
                if isinstance(resource, str):
                    data = json.loads(resource)
                else:
                    # 已经是字典格式
                    data = {"resource": resource}
            else:
                data = {"resource": data}
        else:
            # JSON格式（MOCK模式）
            data = json.loads(body)
        # ===================================================================

        # 解密回调数据
        resource = data.get("resource", {})
        if not resource:
            logger.error("回调数据中缺少resource字段")
            return _xml_response("FAIL", "Missing resource")

        decrypted_data = pay_client.decrypt_callback_data(resource)

        # 根据事件类型处理
        event_type = decrypted_data.get("event_type")

        if event_type == "APPLYMENT_STATE_CHANGE":
            # 进件状态变更
            await handle_applyment_state_change(decrypted_data)
            return _xml_response("SUCCESS", "OK")  # ✅ 修改：返回微信要求的XML格式
        elif event_type == "TRANSACTION.SUCCESS":
            # 支付成功
            await handle_transaction_success(decrypted_data)
            return _xml_response("SUCCESS", "OK")  # ✅ 修改：返回微信要求的XML格式
        else:
            logger.warning(f"未知的事件类型: {event_type}")
            return _xml_response("FAIL", f"Unknown event_type: {event_type}")

    except json.JSONDecodeError as e:
        logger.error(f"JSON解析失败: {str(e)}")
        return _xml_response("FAIL", "Invalid JSON format")
    except Exception as e:
        logger.error(f"微信支付回调处理失败: {str(e)}", exc_info=True)
        return _xml_response("FAIL", str(e))


# ==================== ✅ 新增：生成微信要求的XML响应 ====================
def _xml_response(code: str, message: str) -> str:
    """
    生成微信支付回调要求的XML格式响应
    微信要求返回格式：
    <xml>
        <return_code><![CDATA[SUCCESS/FAIL]]></return_code>
        <return_msg><![CDATA[OK/错误信息]]></return_msg>
    </xml>
    """
    return f"""<xml>
<return_code><![CDATA[{code}]]></return_code>
<return_msg><![CDATA[{message}]]></return_msg>
</xml>"""


# ===================================================================


async def handle_applyment_state_change(data: dict):
    """处理进件状态变更回调"""
    try:
        applyment_id = data.get("applyment_id")
        state = data.get("applyment_state")

        if not applyment_id or not state:
            logger.error("进件回调缺少必要字段")
            return

        service = WechatApplymentService()
        await service.handle_applyment_state_change(
            applyment_id,
            state,
            {
                "state_msg": data.get("state_msg"),
                "sub_mchid": data.get("sub_mchid")
            }
        )
        logger.info(f"进件状态更新成功: {applyment_id} -> {state}")
    except Exception as e:
        logger.error(f"进件状态处理失败: {str(e)}", exc_info=True)
        # 不抛出异常，避免影响主流程


async def handle_transaction_success(data: dict):
    """处理支付成功回调"""
    try:
        # ==================== ✅ 新增：安全提取支付数据 ====================
        out_trade_no = data.get("out_trade_no")
        transaction_id = data.get("transaction_id")
        amount = data.get("amount", {}).get("total")

        if not out_trade_no:
            logger.error("支付回调缺少out_trade_no")
            return

        logger.info(f"支付成功: 订单号={out_trade_no}, 微信流水号={transaction_id}, 金额={amount}")
        # ===================================================================

        # TODO: 实现支付成功后的业务逻辑
        # 1. 更新订单状态为已支付
        # 2. 如果是会员商品，触发星级升级
        # 3. 发放积分与奖励
        # 4. 记录积分流水

    except Exception as e:
        logger.error(f"支付成功回调处理失败: {str(e)}", exc_info=True)
        # 不抛出异常，避免影响主流程


def register_wechat_pay_routes(app):
    """
    注册微信支付路由
    注意：prefix 已在 router 中定义，这里不需要重复
    """
    app.include_router(router)