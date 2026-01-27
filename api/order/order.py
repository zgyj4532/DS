from services.finance_service import FinanceService
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any, cast
from core.config import Settings, settings
from core.database import get_conn
from services.finance_service import split_order_funds
from core.config import VALID_PAY_WAYS, POINTS_DISCOUNT_RATE
from core.table_access import build_dynamic_select, get_table_structure, _quote_identifier
from decimal import Decimal
import uuid
from datetime import datetime, timedelta
from enum import Enum
import json
import threading
import time
from core.logging import get_logger
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter
from io import BytesIO
from typing import List, Dict, Any
from fastapi.responses import StreamingResponse

logger = get_logger(__name__)
router = APIRouter()

# 在 order.py 的 _cancel_expire_orders 函数中
def _cancel_expire_orders():
    """每分钟扫描一次，把过期的 pending_pay 订单取消"""
    while True:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    now = datetime.now()
                    cur.execute("""
                        SELECT id, order_number
                        FROM orders
                        WHERE status='pending_pay'
                          AND expire_at IS NOT NULL
                          AND expire_at <= %s
                    """, (now,))
                    for o in cur.fetchall():
                        oid, ono = o["id"], o["order_number"]

                        # ✅ 新增：删除该订单的待发放奖励记录
                        cur.execute(
                            "SELECT id FROM pending_rewards WHERE order_id = %s AND status = 'pending'",
                            (oid,)
                        )
                        rewards = cur.fetchall()
                        for reward in rewards:
                            cur.execute(
                                "DELETE FROM pending_rewards WHERE id = %s",
                                (reward['id'],)
                            )
                            print(f"[expire] 删除订单 {ono} 的待发放奖励记录: ID={reward['id']}")

                        # 回滚库存
                        cur.execute(
                            "SELECT product_id,quantity FROM order_items WHERE order_id=%s",
                            (oid,)
                        )
                        for it in cur.fetchall():
                            cur.execute(
                                "UPDATE product_skus SET stock=stock+%s WHERE product_id=%s",
                                (it["quantity"], it["product_id"])
                            )

                        # 改状态
                        cur.execute(
                            "UPDATE orders SET status='cancelled',updated_at=NOW() WHERE id=%s",
                            (oid,)
                        )
                        print(f"[expire] 订单 {ono} 已自动取消")
                    conn.commit()
        except Exception as e:
            print(f"[expire] error: {e}")
        time.sleep(60)

def start_order_expire_task():
    """由 api.order 包初始化时调用一次即可"""
    t = threading.Thread(target=_cancel_expire_orders, daemon=True)
    t.start()
    print("[expire] 订单过期守护线程已启动")

class OrderManager:
    @staticmethod
    def _build_orders_select(cursor) -> str:
        structure = get_table_structure(cursor, "orders")
        select_parts = []
        for field in structure['fields']:
            if field in structure['asset_fields']:
                select_parts.append(f"COALESCE({_quote_identifier(field)}, 0) AS {_quote_identifier(field)}")
            else:
                select_parts.append(_quote_identifier(field))
        return ", ".join(select_parts)

    @staticmethod
    def create(
            user_id: int,
            address_id: Optional[int],
            custom_addr: Optional[dict],
            specifications: Optional[str] = None,
            buy_now: bool = False,
            buy_now_items: Optional[List[Dict[str, Any]]] = None,
            delivery_way: str = "platform"
    ) -> Optional[str]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # ---------- 1. 组装订单明细 ----------
                if buy_now:
                    if not buy_now_items:
                        raise HTTPException(status_code=422, detail="立即购买时 buy_now_items 不能为空")
                    items = []
                    for it in buy_now_items:
                        cur.execute("SELECT is_member_product FROM products WHERE id = %s", (it["product_id"],))
                        prod = cur.fetchone()
                        if not prod:
                            raise HTTPException(status_code=404, detail=f"products 表中不存在 id={it['product_id']}")

                        sku_id = it.get("sku_id")
                        if not sku_id:
                            cur.execute("SELECT id FROM product_skus WHERE product_id = %s LIMIT 1", (it['product_id'],))
                            sku_row = cur.fetchone()
                            if sku_row:
                                sku_id = sku_row.get('id')
                            else:
                                raise HTTPException(status_code=422, detail=f"商品 {it['product_id']} 无可用 SKU，请提供 sku_id")

                        if "price" not in it:
                            raise HTTPException(status_code=422, detail=f"buy_now_items 必须包含 price 字段：product_id={it['product_id']}")

                        items.append({
                            "sku_id": sku_id,
                            "product_id": it["product_id"],
                            "quantity": it["quantity"],
                            "price": Decimal(str(it["price"])),
                            "is_vip": prod["is_member_product"]
                        })
                else:
                    cur.execute("""
                        SELECT c.product_id,
                            c.sku_id,
                            c.quantity,
                            s.price,
                            p.is_member_product AS is_vip,
                            c.specifications
                        FROM cart c
                        JOIN product_skus s ON s.id = c.sku_id
                        JOIN products p ON p.id = c.product_id
                        WHERE c.user_id = %s AND c.selected = 1
                    """, (user_id,))
                    items = cur.fetchall()
                    if not items:
                        return None

                # ---------- 2. 地址信息 ----------
                if delivery_way == "pickup":
                    consignee_name = consignee_phone = province = city = district = shipping_address = ""
                elif custom_addr:
                    consignee_name = custom_addr.get("consignee_name")
                    consignee_phone = custom_addr.get("consignee_phone")
                    province = custom_addr.get("province", "")
                    city = custom_addr.get("city", "")
                    district = custom_addr.get("district", "")
                    shipping_address = custom_addr.get("detail", "")
                else:
                    raise HTTPException(status_code=422, detail="必须上传收货地址或选择自提")

                # ---------- 3. 订单主表 ----------
                total = sum(Decimal(str(i["quantity"])) * Decimal(str(i["price"])) for i in items)
                has_vip = any(i["is_vip"] for i in items)
                order_number = datetime.now().strftime("%Y%m%d%H%M%S") + str(user_id) + str(uuid.uuid4().int)[:6]
                init_status = "pending_pay"  # 统一待支付

                cur.execute("""
                    INSERT INTO orders(
                        user_id, order_number, total_amount, status, is_vip_item,
                        consignee_name, consignee_phone,
                        province, city, district, shipping_address, delivery_way,
                        pay_way, auto_recv_time, refund_reason, expire_at)
                    VALUES (%s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s,
                            'wechat', %s, %s, %s)
                """, (
                    user_id, order_number, total, init_status, has_vip,
                    consignee_name, consignee_phone,
                    province, city, district, shipping_address, delivery_way,
                    datetime.now() + timedelta(days=7),
                    specifications,
                    datetime.now() + timedelta(hours=12) if init_status == "pending_pay" else None
                ))
                oid = cur.lastrowid

                # ---------- 4. 库存校验 & 扣减 ----------
                structure = get_table_structure(cur, "product_skus")
                has_stock_field = 'stock' in structure['fields']
                stock_select = (
                    f"COALESCE({_quote_identifier('stock')}, 0) AS {_quote_identifier('stock')}"
                    if has_stock_field and 'stock' in structure['asset_fields']
                    else _quote_identifier('stock')
                ) if has_stock_field else "0 AS stock"

                for i in items:
                    cur.execute(f"SELECT {stock_select} FROM {_quote_identifier('product_skus')} WHERE id=%s", (i['sku_id'],))
                    result = cur.fetchone()
                    current_stock = result.get('stock', 0) if result else 0
                    if current_stock < i["quantity"]:
                        raise HTTPException(status_code=400, detail=f"SKU {i['sku_id']} 库存不足")

                # ---------- 5. 写订单明细 ----------
                for i in items:
                    cur.execute("""
                        INSERT INTO order_items(order_id, product_id, sku_id, quantity, unit_price, total_price)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        oid, i["product_id"], i["sku_id"], i["quantity"],
                        i["price"], Decimal(str(i["quantity"])) * Decimal(str(i["price"]))
                    ))

                # ---------- 6. 扣库存 ----------
                if has_stock_field:
                    for i in items:
                        cur.execute("UPDATE product_skus SET stock = stock - %s WHERE id = %s", (i["quantity"], i["sku_id"]))

                # ---------- 7. 清空购物车（仅购物车结算场景） ----------
                if not buy_now:
                    cur.execute("DELETE FROM cart WHERE user_id = %s AND selected = 1", (user_id,))

                # ---------- 8. 资金拆分 ----------
                split_order_funds(order_number, total, has_vip, cursor=cur)

                conn.commit()
                return order_number

    @staticmethod
    def list_by_user(user_id: int, status: Optional[str] = None):
        """按用户查询订单列表，附带首件商品和规格字段。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                select_fields = OrderManager._build_orders_select(cur)
                sql = f"SELECT {select_fields} FROM orders WHERE user_id = %s"
                params = [user_id]
                if status:
                    sql += " AND status = %s"
                    params.append(status)
                sql += " ORDER BY created_at DESC"
                cur.execute(sql, tuple(params))
                orders = cur.fetchall()

                for o in orders:
                    cur.execute(
                        """
                        SELECT oi.*, p.name
                        FROM order_items oi
                        JOIN products p ON oi.product_id = p.id
                        WHERE oi.order_id = %s
                        LIMIT 1
                        """,
                        (o["id"],)
                    )
                    first_item = cur.fetchone()
                    o["first_product"] = first_item
                    o["specifications"] = o.get("refund_reason")

                return orders

    @staticmethod
    def detail(order_number: str) -> Optional[dict]:
        """查询单个订单详情（含用户、地址、商品明细）。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                select_fields = OrderManager._build_orders_select(cur)
                cur.execute(
                    f"SELECT {select_fields} FROM orders WHERE order_number=%s LIMIT 1",
                    (order_number,)
                )
                order = cur.fetchone()
                if not order:
                    return None

                order_id = order.get("id")
                user_id = order.get("user_id")

                # 商品明细
                cur.execute(
                    """
                    SELECT oi.*, p.name AS product_name, p.is_member_product, p.cover AS product_cover
                    FROM order_items oi
                    LEFT JOIN products p ON oi.product_id = p.id
                    WHERE oi.order_id = %s
                    """,
                    (order_id,)
                )
                items = cur.fetchall()

                # 用户信息
                user_info = None
                if user_id:
                    cur.execute(
                        "SELECT id, name, mobile, avatar, member_level, member_points FROM users WHERE id=%s",
                        (user_id,)
                    )
                    user_info = cur.fetchone()

                # 地址信息直接取订单中的收货字段
                address = {
                    "consignee_name": order.get("consignee_name"),
                    "consignee_phone": order.get("consignee_phone"),
                    "province": order.get("province"),
                    "city": order.get("city"),
                    "district": order.get("district"),
                    "detail": order.get("shipping_address"),
                }

                return {
                    "order_info": order,
                    "user": user_info,
                    "address": address,
                    "items": items,
                    "specifications": order.get("refund_reason"),
                }

    @staticmethod
    def export_to_excel(order_numbers: List[str]) -> bytes:
        """
        导出订单详情（包含资金拆分明细）
        生成两个工作表：订单详情、资金拆分明细
        """
        # 账户类型中英文映射
        account_type_map = {
            "merchant_balance": "商家余额",
            "public_welfare": "公益基金",
            "maintain_pool": "平台维护",
            "subsidy_pool": "周补贴池",
            "director_pool": "联创奖励",
            "shop_pool": "社区店",
            "city_pool": "城市运营中心",
            "branch_pool": "大区分公司",
            "fund_pool": "事业发展基金",
            "company_points": "公司积分账户",
            "company_balance": "公司余额账户",
            "platform_revenue_pool": "平台收入池（会员商品）",
            "wx_applyment_fee": "微信进件手续费",
            "income": "收入",
            "expense": "支出"
        }

        wb = Workbook()

        # ========== 第一个工作表：订单详情 ==========
        ws1 = wb.active
        ws1.title = "订单详情"

        headers1 = [
            "订单号", "订单状态", "总金额", "原始金额", "积分抵扣", "实付金额",
            "支付方式", "配送方式", "是否会员订单",
            "用户ID", "用户姓名", "用户手机号",
            "收货人", "收货电话", "省份", "城市", "区县", "详细地址",
            "商品信息", "商品规格", "下单时间", "支付时间", "发货时间"
        ]

        # 设置表头样式
        header_font = Font(bold=True, color="FFFFFF")
        header_fill = PatternFill(start_color="2C3E50", end_color="2C3E50", fill_type="solid")
        header_alignment = Alignment(horizontal="center", vertical="center")
        thin_border = Border(
            left=Side(style='thin'), right=Side(style='thin'),
            top=Side(style='thin'), bottom=Side(style='thin')
        )

        for col_idx, header in enumerate(headers1, 1):
            cell = ws1.cell(row=1, column=col_idx, value=header)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = header_alignment
            cell.border = thin_border

        # ========== 第二个工作表：资金拆分明细 ==========
        ws2 = wb.create_sheet(title="资金拆分")
        headers2 = [
            "订单号", "账户类型", "变动金额", "变动后余额",
            "流水类型", "备注", "创建时间"
        ]

        for col_idx, header in enumerate(headers2, 1):
            cell = ws2.cell(row=1, column=col_idx, value=header)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = header_alignment
            cell.border = thin_border

        # 查询数据并填充
        row_idx1 = 2
        row_idx2 = 2

        with get_conn() as conn:
            with conn.cursor() as cur:
                for order_number in order_numbers:
                    # 1. 查询订单详情
                    order_data = OrderManager.detail(order_number)
                    if not order_data:
                        continue

                    order_info = order_data["order_info"]
                    user_info = order_data["user"]
                    address = order_data["address"] or {}
                    items = order_data["items"]
                    specifications = order_data.get("specifications") or {}

                    # 计算实付金额（用于计算20%平台抽成）
                    total = Decimal(str(order_info.get("total_amount", 0)))
                    points_discount = Decimal(str(order_info.get("points_discount", 0)))
                    actual_pay = total - points_discount

                    # 商品信息拼接
                    product_info = "\n".join([
                        f"{item.get('product_name', '')} x{item.get('quantity', 0)} @¥{item.get('unit_price', 0)}"
                        for item in items
                    ])

                    # 规格信息
                    spec_str = ""
                    if isinstance(specifications, dict):
                        spec_str = "\n".join([f"{k}: {v}" for k, v in specifications.items()])

                    # 判断发货时间（自提订单=支付时间）
                    shipped_at = order_info.get("shipped_at", "")
                    if order_info.get("delivery_way") == "pickup":
                        shipped_at = order_info.get("paid_at", "")

                    # 填充订单详情行
                    row_data1 = [
                        order_info.get("order_number", ""),
                        order_info.get("status", ""),
                        float(total),
                        float(order_info.get("original_amount", 0)),
                        float(points_discount),
                        float(actual_pay),
                        order_info.get("pay_way", "wechat"),
                        order_info.get("delivery_way", "platform"),
                        "是" if order_info.get("is_member_order") else "否",
                        user_info.get("id", ""),
                        user_info.get("name", ""),
                        user_info.get("mobile", ""),
                        address.get("consignee_name", ""),
                        address.get("consignee_phone", ""),
                        address.get("province", ""),
                        address.get("city", ""),
                        address.get("district", ""),
                        address.get("detail", ""),
                        product_info,
                        spec_str,
                        order_info.get("created_at", ""),
                        order_info.get("paid_at", ""),
                        shipped_at  # ← 使用修改后的发货时间
                    ]

                    for col_idx, value in enumerate(row_data1, 1):
                        cell = ws1.cell(row=row_idx1, column=col_idx, value=value)
                        cell.alignment = Alignment(vertical="center", wrap_text=True)
                        cell.border = thin_border
                        if col_idx in [3, 4, 5, 6]:  # 金额列
                            cell.number_format = '¥#,##0.00'

                    row_idx1 += 1

                    # 2. 查询资金拆分明细（account_flow表）
                    cur.execute("""
                        SELECT account_type, change_amount, balance_after, 
                               flow_type, remark, created_at
                        FROM account_flow 
                        WHERE remark LIKE %s
                        ORDER BY created_at ASC
                    """, (f"%{order_number}%",))

                    flows = cur.fetchall()

                    # 计算平台抽成总额（20%）
                    platform_fee = float(actual_pay) * 0.2

                    for flow in flows:
                        account_type_en = flow.get("account_type", "")

                        # 特殊处理：商家余额显示为X雨点（20%）
                        if account_type_en == "merchant_balance":
                            display_amount = f"{int(platform_fee)}雨点"
                            account_type_cn = "商家余额"  # ← 改回"商家余额"
                            balance_after_display = "-"
                            is_platform_fee_row = True
                        else:
                            # 其他账户类型正常显示
                            account_type_cn = account_type_map.get(account_type_en, account_type_en)
                            display_amount = float(flow.get("change_amount", 0))
                            balance_after_display = float(flow.get("balance_after", 0))
                            is_platform_fee_row = False

                        row_data2 = [
                            order_number,
                            account_type_cn,
                            display_amount,
                            balance_after_display,
                            flow.get("flow_type", ""),
                            flow.get("remark", ""),
                            flow.get("created_at", "")
                        ]

                        for col_idx, value in enumerate(row_data2, 1):
                            cell = ws2.cell(row=row_idx2, column=col_idx, value=value)
                            cell.alignment = Alignment(vertical="center")
                            cell.border = thin_border

                            # 只有非商家余额行才设置货币格式
                            if col_idx in [3, 4] and not is_platform_fee_row:
                                cell.number_format = '¥#,##0.00'

                        row_idx2 += 1

        # 调整列宽（订单详情表）
        for column in ws1.columns:
            max_length = 0
            column_letter = get_column_letter(column[0].column)
            for cell in column:
                try:
                    if cell.value and len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            ws1.column_dimensions[column_letter].width = min(max_length + 2, 50)

        # 调整列宽（资金拆分表）
        for column in ws2.columns:
            max_length = 0
            column_letter = get_column_letter(column[0].column)
            for cell in column:
                try:
                    if cell.value and len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            ws2.column_dimensions[column_letter].width = min(max_length + 2, 50)

        # 保存到内存
        excel_data = BytesIO()
        wb.save(excel_data)
        excel_data.seek(0)
        return excel_data.getvalue()
# ---------------- 请求模型 ----------------
class DeliveryWay(str, Enum):
    platform = "platform"  # 平台配送
    pickup = "pickup"


class OrderCreate(BaseModel):
    user_id: int
    delivery_way: DeliveryWay = DeliveryWay.platform  # 新增
    address_id: Optional[int] = None
    custom_address: Optional[dict] = None
    specifications: Optional[str] = None
    buy_now: bool = False
    buy_now_items: Optional[List[Dict[str, Any]]] = None


class OrderPay(BaseModel):
    order_number: str
    pay_way: str
    coupon_id: Optional[int] = None
    points_to_use: Optional[Decimal] = Decimal('0')


class StatusUpdate(BaseModel):
    order_number: str
    new_status: str
    reason: Optional[str] = None

class WechatPayParams(BaseModel):
    appId: str
    timeStamp: str
    nonceStr: str
    package: str
    signType: str
    paySign: str


# ---------------- 路由 ----------------
@router.post("/create", summary="创建订单")
def create_order(body: OrderCreate):
    no = OrderManager.create(
        body.user_id,
        body.address_id,
        body.custom_address,
        specifications=body.specifications,  # 透传
        buy_now=body.buy_now,
        buy_now_items=body.buy_now_items,
        delivery_way=body.delivery_way
    )
    if not no:
        raise HTTPException(status_code=422, detail="购物车为空或地址缺失")
    return {"order_number": no}


@router.get("/{user_id}", summary="查询用户订单列表")
def list_orders(user_id: int, status: Optional[str] = None):
    return OrderManager.list_by_user(user_id, status)

@router.get("/detail/{order_number}", summary="查询订单详情")
def order_detail(order_number: str):
    d = OrderManager.detail(order_number)
    if not d:
        raise HTTPException(status_code=404, detail="订单不存在")
    return d

@router.post("/status", summary="更新订单状态")
def update_status(body: StatusUpdate):
    return {"ok": OrderManager.update_status(body.order_number, body.new_status, body.reason)}

def auto_receive_task(db_cfg: dict = None):
    """自动收货守护进程（不再发放积分）"""
    import threading
    import time
    from datetime import datetime

    def run():
        while True:
            try:
                from core.database import get_conn
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        now = datetime.now()
                        cur.execute(
                            "SELECT id, order_number, total_amount FROM orders "
                            "WHERE status='pending_recv' AND auto_recv_time<=%s",
                            (now,)
                        )
                        for row in cur.fetchall():
                            order_id = row["id"]
                            order_number = row["order_number"]

                            # 更新订单状态为已完成
                            cur.execute(
                                "UPDATE orders SET status='completed' WHERE id=%s",
                                (order_id,)
                            )

                            # ✅ **移除**：积分已在支付时发放，自动收货不再发放
                            # try:
                            #     fs = FinanceService()
                            #     fs.grant_points_on_receive(order_number, external_conn=conn)
                            # except Exception as e:
                            #     logger.error(f"[auto_receive] 订单{order_number}积分发放失败: {e}", exc_info=True)

                            conn.commit()
                            logger.debug(f"[auto_receive] 订单 {order_number} 已自动完成。")
            except Exception as e:
                logger.error(f"[auto_receive] 异常: {e}")
            time.sleep(3600)  # 每小时检查一次

    t = threading.Thread(target=run, daemon=True)
    t.start()
    logger.info("自动收货守护进程已启动（不再发放积分）")




class OrderExportRequest(BaseModel):
    order_numbers: List[str]

class OrderExportByTimeRequest(BaseModel):
    start_time: str  # 格式：2025-01-01 00:00:00
    end_time: str    # 格式：2025-01-31 23:59:59
    status: Optional[str] = None  # 可选：按订单状态筛选（如 pending_ship, completed 等）


@router.post("/export", summary="导出订单详情到Excel")
def export_orders(body: OrderExportRequest):
    """
    批量导出订单详情为Excel文件
    请求示例: {"order_numbers": ["20250101120000", "20250101120001"]}
    """
    if not body.order_numbers:
        raise HTTPException(status_code=422, detail="订单号列表不能为空")

    # 限制一次最多导出1000个订单
    if len(body.order_numbers) > 1000:
        raise HTTPException(status_code=422, detail="单次导出订单数不能超过1000个")

    try:
        excel_data = OrderManager.export_to_excel(body.order_numbers)
        return StreamingResponse(
            BytesIO(excel_data),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=orders_export.xlsx"}
        )
    except Exception as e:
        logger.error(f"导出订单失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"导出失败: {str(e)}")


@router.post("/export/by-time", summary="按时间范围导出订单")
def export_orders_by_time(body: OrderExportByTimeRequest):
    """
    按时间范围批量导出订单详情
    请求示例: {
        "start_time": "2025-01-01 00:00:00",
        "end_time": "2025-01-31 23:59:59",
        "status": "completed"  // 可选，不填则导出所有状态
    }
    """
    # 1. 时间格式校验
    try:
        start = datetime.strptime(body.start_time, "%Y-%m-%d %H:%M:%S")
        end = datetime.strptime(body.end_time, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        raise HTTPException(
            status_code=422,
            detail="时间格式错误，请使用：YYYY-MM-DD HH:MM:SS"
        )

    if end < start:
        raise HTTPException(
            status_code=422,
            detail="结束时间不能早于开始时间"
        )

    # 2. 限制时间范围（防止导出数据量过大，最多31天）
    if (end - start).days > 31:
        raise HTTPException(
            status_code=422,
            detail="时间范围不能超过31天"
        )

    # 3. 查询时间范围内的订单号
    with get_conn() as conn:
        with conn.cursor() as cur:
            if body.status:
                # 按状态筛选
                sql = """
                    SELECT order_number 
                    FROM orders 
                    WHERE created_at >= %s 
                      AND created_at <= %s 
                      AND status = %s 
                    ORDER BY created_at DESC 
                    LIMIT 500
                """
                cur.execute(sql, (body.start_time, body.end_time, body.status))
            else:
                # 不筛选状态，导出全部
                sql = """
                    SELECT order_number 
                    FROM orders 
                    WHERE created_at >= %s 
                      AND created_at <= %s 
                    ORDER BY created_at DESC 
                    LIMIT 500
                """
                cur.execute(sql, (body.start_time, body.end_time))

            rows = cur.fetchall()
            order_numbers = [row["order_number"] for row in rows]

    # 4. 检查是否有数据
    if not order_numbers:
        raise HTTPException(
            status_code=404,
            detail="该时间段内没有符合条件的订单"
        )

    # 5. 调用现有的导出方法生成Excel
    try:
        excel_data = OrderManager.export_to_excel(order_numbers)

        # 生成带时间范围的文件名
        start_str = body.start_time[:10].replace("-", "")
        end_str = body.end_time[:10].replace("-", "")
        filename = f"orders_{start_str}_to_{end_str}.xlsx"
        if body.status:
            filename = f"orders_{body.status}_{start_str}_to_{end_str}.xlsx"

        return StreamingResponse(
            BytesIO(excel_data),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        logger.error(f"按时间导出订单失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"导出失败: {str(e)}")

# 模块被导入时自动启动守护线程
start_order_expire_task()