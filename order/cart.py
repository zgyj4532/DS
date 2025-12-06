from fastapi import APIRouter
from pydantic import BaseModel
from database_setup import get_conn
from typing import List, Dict, Any

router = APIRouter()

class CartManager:
    @staticmethod
    def list_items(user_id: int) -> List[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                sql = """
                    SELECT c.*, p.name AS product_name, p.price AS unit_price,
                           (c.quantity * p.price) AS total_price
                    FROM Cart c JOIN Products p ON c.product_id = p.id
                    WHERE c.user_id = %s
                    ORDER BY c.added_at DESC
                """
                cur.execute(sql, user_id)
                return cur.fetchall()

    @staticmethod
    def add(user_id: int, product_id: int, quantity: int = 1) -> bool:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT quantity FROM Cart WHERE user_id=%s AND product_id=%s", (user_id, product_id))
                row = cur.fetchone()
                if row:
                    cur.execute("UPDATE Cart SET quantity=quantity+%s WHERE user_id=%s AND product_id=%s",
                                (quantity, user_id, product_id))
                else:
                    cur.execute("INSERT INTO Cart(user_id,product_id,quantity) VALUES(%s,%s,%s)",
                                (user_id, product_id, quantity))
                conn.commit()
                return True

    @staticmethod
    def remove(user_id: int, product_id: int) -> bool:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM Cart WHERE user_id=%s AND product_id=%s", (user_id, product_id))
                conn.commit()
                return True

# ---------------- 路由 ----------------
class CartAdd(BaseModel):
    user_id: int
    product_id: int
    quantity: int = 1

@router.get("/{user_id}")
def get_cart(user_id: int):
    return CartManager.list_items(user_id)

@router.post("/add")
def cart_add(body: CartAdd):
    return {"ok": CartManager.add(body.user_id, body.product_id, body.quantity)}

@router.delete("/{user_id}/{product_id}")
def cart_remove(user_id: int, product_id: int):
    return {"ok": CartManager.remove(user_id, product_id)}