from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from database_setup import get_conn
from .finance import split_order_funds
from config import VALID_PAY_WAYS
from decimal import Decimal
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any

router = APIRouter()

class OrderManager:
    @staticmethod
    def create(user_id: int, address_id: Optional[int], custom_addr: Optional[dict]) -> Optional[str]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 选中的购物车
                cur.execute("""SELECT c.product_id,c.quantity,p.price,p.is_vip 
                                 FROM Cart c JOIN Products p ON c.product_id=p.id
                                 WHERE c.user_id=%s AND c.selected=1""", user_id)
                items = cur.fetchall()
                if not items:
                    return None
                total = sum(Decimal(i["quantity"]) * i["price"] for i in items)
                has_vip = any(i["is_vip"] for i in items)
                order_number = datetime.now().strftime("%Y%m%d%H%M%S") + str(user_id) + str(uuid.uuid4().int)[:6]
                # 写订单
                cur.execute("""INSERT INTO Orders(user_id,order_number,total_amount,status,is_vip_item,auto_recv_time)
                               VALUES(%s,%s,%s,'pending_pay',%s,%s)""",
                            (user_id, order_number, total, has_vip, datetime.now() + timedelta(days=7)))
                oid = cur.lastrowid
                # 明细
                for i in items:
                    cur.execute("""INSERT INTO Order_Items(order_id,product_id,quantity,unit_price,total_price)
                                   VALUES(%s,%s,%s,%s,%s)""",
                                (oid, i["product_id"], i["quantity"], i["price"], i["quantity"] * i["price"]))
                # 扣库存
                for i in items:
                    cur.execute("UPDATE Products SET stock=stock-%s WHERE id=%s", (i["quantity"], i["product_id"]))
                # 清空已选
                cur.execute("DELETE FROM Cart WHERE user_id=%s AND selected=1", user_id)
                conn.commit()
                split_order_funds(order_number, total, has_vip)
                return order_number

    @staticmethod
    def list_by_user(user_id: int, status: Optional[str] = None):
        with get_conn() as conn:
            with conn.cursor() as cur:
                sql = "SELECT * FROM Orders WHERE user_id=%s"
                params = [user_id]
                if status:
                    sql += " AND status=%s"
                    params.append(status)
                sql += " ORDER BY created_at DESC"
                cur.execute(sql, params)
                return cur.fetchall()

    @staticmethod
    def detail(order_number: str) -> Optional[dict]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM Orders WHERE order_number=%s", order_number)
                order_info = cur.fetchone()
                if not order_info:
                    return None
                cur.execute("""SELECT oi.*,p.name FROM Order_Items oi JOIN Products p ON oi.product_id=p.id
                               WHERE oi.order_id=%s""", order_info["id"])
                items = cur.fetchall()
                return {"order_info": order_info, "items": items}

    @staticmethod
    def update_status(order_number: str, new_status: str, reason: Optional[str] = None) -> bool:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE Orders SET status=%s,refund_reason=%s WHERE order_number=%s",
                            (new_status, reason, order_number))
                conn.commit()
                return True

# ---------------- 路由 ----------------
class OrderCreate(BaseModel):
    user_id: int
    address_id: Optional[int] = None
    custom_address: Optional[dict] = None

class OrderPay(BaseModel):
    order_number: str
    pay_way: str

class StatusUpdate(BaseModel):
    order_number: str
    new_status: str
    reason: Optional[str] = None

@router.post("/create")
def create_order(body: OrderCreate):
    no = OrderManager.create(body.user_id, body.address_id, body.custom_address)
    if not no:
        raise HTTPException(422, "购物车为空或地址缺失")
    return {"order_number": no}

@router.post("/pay")
def order_pay(body: OrderPay):
    if body.pay_way not in VALID_PAY_WAYS:
        raise HTTPException(422, "非法支付方式")
    return {"ok": OrderManager.update_status(body.order_number, "pending_ship")}

@router.get("/{user_id}")
def list_orders(user_id: int, status: Optional[str] = None):
    return OrderManager.list_by_user(user_id, status)

@router.get("/detail/{order_number}")
def order_detail(order_number: str):
    d = OrderManager.detail(order_number)
    if not d:
        raise HTTPException(404, "订单不存在")
    return d

@router.post("/status")
def update_status(body: StatusUpdate):
    return {"ok": OrderManager.update_status(body.order_number, body.new_status, body.reason)}