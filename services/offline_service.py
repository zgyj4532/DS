# services/offline_service.py
from __future__ import annotations
from decimal import Decimal
from datetime import datetime, timedelta
from typing import Optional, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from wechatpayv3 import WeChatPay      # 仅为静态检查服务

from core.database import get_conn
from core.config import settings
from core.logging import get_logger
from services.finance_service import FinanceService
from services.notify_service import notify_merchant
from pathlib import Path
import pymysql
import xmltodict
import base64
from services.wechat_api import get_wxacode

logger = get_logger(__name__)

# -------------- 运行时 wxpay 初始化 --------------
if not settings.WX_MOCK_MODE:
    from wechatpayv3 import WeChatPay, WeChatPayType
    priv_path = Path(settings.WECHAT_PAY_API_KEY_PATH)
    if not priv_path.exists():
        raise RuntimeError(f"WeChat private key file not found: {priv_path}")
    private_key = priv_path.read_text(encoding="utf-8")

    public_key = None
    if settings.WECHAT_PAY_PUBLIC_KEY_PATH:
        pub_path = Path(settings.WECHAT_PAY_PUBLIC_KEY_PATH)
        if pub_path.exists():
            public_key = pub_path.read_text(encoding="utf-8")
        else:
            logger.warning(f"WeChat public key file not found: {pub_path}")

    wxpay: WeChatPay = WeChatPay(
        wechatpay_type=WeChatPayType.MINIPROG,
        mchid=settings.WECHAT_PAY_MCH_ID,
        private_key=private_key,
        cert_serial_no=settings.WECHAT_CERT_SERIAL_NO,
        apiv3_key=settings.WECHAT_PAY_API_V3_KEY,
        appid=settings.WECHAT_APP_ID,
        public_key=public_key,
        public_key_id=settings.WECHAT_PAY_PUB_KEY_ID,
    )
else:
    wxpay: WeChatPay | None = None


class OfflineService:
    # ---------- 1. 创建线下支付单 ----------
    @staticmethod
    async def create_order(
        merchant_id: int,
        store_name: str,
        amount: int,
        product_name: str = "",
        remark: str = "",
        invite_code: str = "",
        user_id: Optional[int] = None,
    ) -> dict:
        import uuid
        # 当前登录用户（UUID 字符串）即为商户号
        current_user_id = str(user_id)  # Bearer UUID
        order_no = f"OFF{datetime.now().strftime('%Y%m%d%H%M%S')}{uuid.uuid4().hex[:6]}"
        expire = datetime.now() + timedelta(seconds=settings.qrcode_expire_seconds)
        path = f"pages/offline/pay?orderNo={order_no}&channel=1"
        scene = f"o={order_no}"
        qrcode_b64 = base64.b64encode(await get_wxacode(path=path, scene=scene)).decode()
        qrcode_url = f"data:image/png;base64,{qrcode_b64}"  

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO offline_order "
                    "(order_no,merchant_id,user_id,store_name,amount,product_name,remark,"
                    "qrcode_url,qrcode_expire,status) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,1)",
                    (order_no, current_user_id, user_id, store_name, amount,
                     product_name, remark, qrcode_url, expire)
                )
                conn.commit()

        logger.info(f"[Offline] 创建订单 {order_no} 金额 {amount} 商户={current_user_id}")
        return {"order_no": order_no, "qrcode_b64": qrcode_b64, "expire_at": expire}

    # ---------- 2. 刷新收款码（限 1 次） ----------
    @staticmethod
    async def refresh_qrcode(order_no: str, user_id: int) -> dict:
        expire = datetime.now() + timedelta(seconds=settings.qrcode_expire_seconds)
        current_user_id = str(user_id)

        with get_conn() as conn:
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                # 1. 查询当前状态（注意 status 后面要有空格或换行）
                cur.execute(
                    "SELECT refresh_count, status "  # ← 注意这里加了一个空格
                    "FROM offline_order "
                    "WHERE order_no=%s AND merchant_id=%s",
                    (order_no, current_user_id)
                )
                row = cur.fetchone()
                
                if not row or row["status"] != 1:
                    raise ValueError("订单不存在或状态异常")
                if row["refresh_count"] >= 1:
                    raise ValueError("收款码已刷新一次，请重新创建订单")

                # 2. 生成新二维码
                path = f"pages/offline/pay?orderNo={order_no}&channel=1"  # ← 修正了括号错误 ${...} → {...}
                scene = f"o={order_no}"
                new_qrcode_b64 = base64.b64encode(await get_wxacode(path=path, scene=scene)).decode()

                # 3. 【关键缺失】更新数据库
                cur.execute(
                    "UPDATE offline_order "
                    "SET qrcode_url=%s, qrcode_expire=%s, refresh_count=refresh_count+1 "
                    "WHERE order_no=%s AND merchant_id=%s",
                    (f"data:image/png;base64,{new_qrcode_b64}", expire, order_no, current_user_id)
                )
                conn.commit()

        return {"qrcode_b64": new_qrcode_b64, "expire_at": expire}

    # ---------- 3. 订单详情 + 可用优惠券 ----------
    @staticmethod
    async def get_order_detail(order_no: str, user_id: int) -> dict:
        current_user_id = str(user_id)
        with get_conn() as conn:
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute(
                    "SELECT order_no,amount,store_name,product_name,status "
                    "FROM offline_order WHERE order_no=%s AND merchant_id=%s",
                    (order_no, current_user_id)
                )
                order = cur.fetchone()
                if not order:
                    raise ValueError("订单不存在")

                svc = FinanceService()
                coupons = svc.list_available(user_id, order["amount"])
                for c in coupons:
                    c["amount"] = float(c["amount"])
                    c["threshold"] = float(c["threshold"])

        return {**order, "coupons": coupons}

    # ---------- 4. 统一下单（核销优惠券 + 调起支付）----------
    @staticmethod
    async def unified_order(
        order_no: str,
        coupon_id: Optional[int],
        user_id: int,
        openid: str,  # 新增参数：支付用户的微信 openid
    ) -> dict:
        current_user_id = str(user_id)
        with get_conn() as conn:
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                # 1. 查询订单原始金额
                cur.execute(
                    "SELECT amount, status, merchant_id, user_id FROM offline_order WHERE order_no=%s AND merchant_id=%s",
                    (order_no, current_user_id)
                )
                row = cur.fetchone()
                if not row or row["status"] != 1:
                    raise ValueError("订单不可支付")
                
                original_amount: int = row["amount"]
                final_amount = original_amount
                coupon_discount = 0

                # 2. 验证并应用优惠券
                if coupon_id:
                    fs = FinanceService()
                    coupons = fs.get_user_coupons(user_id=user_id, status='unused')
                    target_coupon = next((c for c in coupons if c['id'] == coupon_id), None)
                    
                    if not target_coupon:
                        raise ValueError("优惠券无效或已被使用")
                    if target_coupon.get('applicable_product_type') == 'member_only':
                        raise ValueError("该优惠券仅限会员商品使用")
                    
                    coupon_discount = int(target_coupon['amount'] * 100)
                    if coupon_discount > original_amount:
                        raise ValueError("优惠券金额大于订单金额")
                    
                    final_amount = original_amount - coupon_discount

                # 3. 更新订单：保存优惠券ID和实付金额
                cur.execute(
                    """UPDATE offline_order 
                    SET coupon_id=%s, 
                        paid_amount=%s,
                        updated_at=NOW()
                    WHERE order_no=%s AND merchant_id=%s""",
                    (coupon_id, final_amount, order_no, current_user_id)
                )
                conn.commit()

        # 4. ====== 关键：使用传入的 openid 调用微信支付 ======
        try:
            from services.notify_service import async_unified_order
            
            pay_result = await async_unified_order({
                'out_trade_no': order_no,
                'amount': {'total': final_amount},  # 单位：分
                'payer': {'openid': openid},  # 使用当前登录用户的 openid
                'description': f'线下订单-{row.get("store_name", "")}'
            })
            
            prepay_id = pay_result.get('prepay_id')
            if not prepay_id:
                logger.error(f"微信支付返回异常: {pay_result}")
                raise ValueError("获取支付参数失败")
            
            # 5. 生成前端需要的支付参数
            import uuid, time
            timestamp = str(int(time.time()))
            nonce_str = uuid.uuid4().hex
            
            # 使用微信支付 SDK 生成签名（确保签名正确）
            if wxpay:
                pay_params = wxpay.get_jsapi_params(prepay_id=prepay_id, timestamp=timestamp, nonce_str=nonce_str)
            else:
                # Mock 模式下的降级
                pay_params = {
                    "appId": settings.WECHAT_APP_ID,
                    "timeStamp": timestamp,
                    "nonceStr": nonce_str,
                    "package": f"prepay_id={prepay_id}",
                    "signType": "RSA",
                    "paySign": "mock_sign",
                }
            
            return {
                "pay_params": pay_params,
                "original_amount": original_amount,
                "coupon_discount": coupon_discount,
                "final_amount": final_amount
            }
            
        except Exception as e:
            logger.error(f"微信支付调用失败: {e}", exc_info=True)
            raise ValueError(f"支付调用失败: {str(e)}")

    
    # ---------- 5. 订单列表 ----------
    @staticmethod
    async def list_orders(merchant_id: int, page: int, size: int):
        current_user_id = str(merchant_id)  # merchant_id 即当前登录用户 UUID
        offset = (page - 1) * size
        with get_conn() as conn:
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute(
                    "SELECT order_no,store_name,amount,status,created_at "
                    "FROM offline_order WHERE merchant_id=%s "
                    "ORDER BY id DESC LIMIT %s OFFSET %s",
                    (current_user_id, size, offset)
                )
                rows = cur.fetchall()
        return {"list": rows, "page": page, "size": size}

    # ---------- 6. 退款 ----------
    @staticmethod
    async def refund(order_no: str, refund_amount: Optional[int], user_id: int):
        current_user_id = str(user_id)
        with get_conn() as conn:
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute(
                    "SELECT id,amount,status FROM offline_order WHERE order_no=%s AND merchant_id=%s",
                    (order_no, current_user_id)
                )
                row = cur.fetchone()
                if not row or row["status"] != 2:
                    raise ValueError("订单未支付")
                amount = row["amount"]
                money = refund_amount or amount

                cur.execute(
                    "UPDATE offline_order SET status=4 WHERE order_no=%s AND merchant_id=%s",
                    (order_no, current_user_id)
                )
                conn.commit()

        await FinanceService.refund_order(order_no)
        logger.info(f"[Offline] 退款 {order_no} 金额 {money} 商户={current_user_id}")
        return {"refund_no": f"REF{order_no}"}

    # ---------- 7. 收款码状态 ----------
    @staticmethod
    async def qrcode_status(order_no: str, merchant_id: int):
        # 直接拿传入的 merchant_id（当前登录用户）
        current_user_id = str(merchant_id)
        with get_conn() as conn:
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute(
                    "SELECT status,qrcode_expire FROM offline_order "
                    "WHERE order_no=%s AND merchant_id=%s",
                    (order_no, current_user_id)
                )
                row = cur.fetchone()
                if not row:
                    raise ValueError("订单不存在")
                now = datetime.now()
                if row["status"] != 1:
                    return {"status": "paid" if row["status"] == 2 else "closed"}
                if row["qrcode_expire"] < now:
                    return {"status": "expired"}
                return {"status": "valid"}


    # ---------- 8. 供优惠券接口调用的原始订单 ----------
    @staticmethod
    async def get_raw_order(order_no: str, merchant_id: str):
        with get_conn() as conn:
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute(
                    "SELECT order_no,amount,status FROM offline_order WHERE order_no=%s AND merchant_id=%s",
                    (order_no, merchant_id)
                )
                return cur.fetchone()
            
    @staticmethod
    async def on_paid(order_no: str, amount: Decimal, coupon_discount: Decimal = Decimal(0)):
        """
        线下订单支付成功后的资金分账（独立简化版）
        【与线上订单完全隔离，仅处理资金池分配】
        """
        from services.finance_service import FinanceService
        
        with get_conn() as conn:
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                # 查询订单信息
                cur.execute(
                    "SELECT merchant_id, store_name, user_id FROM offline_order WHERE order_no=%s",
                    (order_no,)
                )
                order = cur.fetchone()
                if not order:
                    logger.error(f"[on_paid] 订单不存在: {order_no}")
                    return

                # 1️⃣ 插入平台订单表（仅用于财务对账，status=completed 表示直接完成）
                # （如果不需要对账可删除此段）
                cur.execute(
                    """INSERT INTO orders (order_number, user_id, merchant_id, total_amount, status,
                    offline_order_flag, pay_way, created_at, coupon_discount) 
                    VALUES (%s, %s, %s, %s, 'completed', 1, 'wechat', NOW(), %s)""",
                    (order_no, order["user_id"], order["merchant_id"], amount, coupon_discount)
                )
                
                # 2️⃣ 资金分账（简化版：只分池子，不发奖励）
                finance = FinanceService()
                allocs = finance.get_pool_allocations()
                merchant_ratio = allocs.get('merchant_balance', Decimal('0.80'))
                merchant_amount = amount * merchant_ratio
                
                # 平台收入池记账（100%）
                finance._add_pool_balance(
                    cur, 'platform_revenue_pool', amount,
                    f"线下订单收入: {order_no}", order["merchant_id"]
                )
                
                # 各子池分配（20%）
                for pool_type, ratio in allocs.items():
                    if pool_type == 'merchant_balance' or ratio <= 0:
                        continue
                    alloc_amount = amount * ratio
                    # 从平台池扣减
                    finance._add_pool_balance(
                        cur, 'platform_revenue_pool', -alloc_amount,
                        f"线下订单分配: {order_no} -> {pool_type}", order["merchant_id"]
                    )
                    # 子池增加
                    finance._add_pool_balance(
                        cur, pool_type, alloc_amount,
                        f"线下订单收入: {order_no}", order["merchant_id"]
                    )
                
                conn.commit()
            
            # 3️⃣ 商户实时到账（调用已有逻辑）
            # 注意：这里转的是扣除平台抽成后的金额
            await notify_merchant(
                merchant_id=order["merchant_id"],
                order_no=order_no,
                amount=int(merchant_amount * 100)  # 转为分
            )
            
            logger.info(f"[on_paid] 线下订单完成: {order_no}, 金额: {amount}, 商户实收: {merchant_amount}")