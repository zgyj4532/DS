from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from core.database import get_conn
from core.table_access import build_dynamic_select
from services.finance_service import reverse_split_on_refund
from typing import Dict, Any

router = APIRouter()

class RefundManager:
    @staticmethod
    def apply(order_number: str, refund_type: str, reason_code: str) -> bool:
        with get_conn() as conn:
            with conn.cursor() as cur:
                select_sql = build_dynamic_select(
                    cur,
                    "refunds",
                    where_clause="order_number=%s",
                    select_fields=["id"]
                )
                cur.execute(select_sql, (order_number,))
                if cur.fetchone():
                    return False
                cur.execute("""INSERT INTO refunds(order_number,refund_type,reason,status)
                               VALUES(%s,%s,%s,'applied')""", (order_number, refund_type, reason_code))
                conn.commit()
                return True

    @staticmethod
    def audit(
        order_number: str,
        approve: bool = True,
        reject_reason: Optional[str] = None,
        merchant_address: Optional[str] = None
    ) -> bool:
        with get_conn() as conn:
            with conn.cursor() as cur:

                # 1️⃣ 查询退款类型
                cur.execute(
                    "SELECT refund_type FROM refunds WHERE order_number=%s",
                    (order_number,)
                )
                row = cur.fetchone()
                if not row:
                    return False

                refund_type = row["refund_type"]

                # 2️⃣ 仅「同意 + 退货退款」才强制要求地址
                if approve and refund_type == "return_refund":
                    if not merchant_address:
                        raise HTTPException(
                            status_code=400,
                            detail="同意退货退款时必须填写商家地址"
                        )

                new_status = "refund_success" if approve else "rejected"

                # 3️⃣ 更新 refunds
                cur.execute(
                    """
                    UPDATE refunds
                    SET status=%s,
                        reject_reason=%s,
                        merchant_address=%s
                    WHERE order_number=%s
                    """,
                    (new_status, reject_reason, merchant_address, order_number)
                )

                if cur.rowcount == 0:
                    return False

                # 4️⃣ 回写订单退款状态
                cur.execute(
                    "UPDATE orders SET refund_status=%s WHERE order_number=%s",
                    (new_status, order_number)
                )

                if approve:
                    cur.execute(
                        "UPDATE orders SET status='refund' WHERE order_number=%s",
                        (order_number,)
                    )
                    reverse_split_on_refund(order_number)
                else:
                    cur.execute(
                        "UPDATE orders SET status='completed' WHERE order_number=%s",
                        (order_number,)
                    )

                conn.commit()
                return True

    @staticmethod
    def progress(order_number: str) -> Optional[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                select_sql = build_dynamic_select(
                    cur,
                    "refunds",
                    where_clause="order_number=%s"
                )
                cur.execute(select_sql, (order_number,))
                return cur.fetchone()

# ---------------- 路由 ----------------
class RefundApply(BaseModel):
    order_number: str
    refund_type: str
    reason_code: str

class RefundAudit(BaseModel):
    order_number: str
    approve: bool
    reject_reason: Optional[str] = None

class RefundAudit(BaseModel):
    order_number: str
    approve: bool
    reject_reason: Optional[str] = None
    merchant_address: Optional[str] = None

@router.post("/apply", summary="申请退款")
def refund_apply(body: RefundApply):
    ok = RefundManager.apply(body.order_number, body.refund_type, body.reason_code)
    if not ok:
        raise HTTPException(status_code=400, detail="该订单已申请过退款")
    return {"ok": True}

@router.post("/audit", summary="审核退款申请")
def refund_audit(body: RefundAudit):
    RefundManager.audit(
        body.order_number,
        body.approve,
        body.reject_reason,
        body.merchant_address
    )
    return {"ok": True}

@router.get("/progress/{order_number}", summary="查询退款进度")
def refund_progress(order_number: str):
    return RefundManager.progress(order_number) or {}
