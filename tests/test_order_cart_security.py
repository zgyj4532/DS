from contextlib import contextmanager
from copy import deepcopy
from decimal import Decimal
from threading import RLock

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from api.order import cart, order


class CommerceDB:
    """Transactional SQL double; real InnoDB isolation still requires integration validation."""

    def __init__(self):
        self.skus = {
            11: {"id": 11, "product_id": 1, "price": Decimal("20"), "stock": 5},
            12: {"id": 12, "product_id": 1, "price": Decimal("30"), "stock": 5},
            21: {"id": 21, "product_id": 2, "price": Decimal("40"), "stock": 5},
        }
        self.cart = {}
        self.orders = []
        self.items = []
        self.lock = RLock()
        self.statements = []

    @contextmanager
    def connection(self):
        with self.lock:
            snapshot = deepcopy((self.skus, self.cart, self.orders, self.items))
            try:
                yield self
            except Exception:
                self.skus, self.cart, self.orders, self.items = snapshot
                raise

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def commit(self):
        pass

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows

    def execute(self, sql, params=()):
        sql = " ".join(sql.split())
        self.statements.append((sql, params))
        self.rows = []
        self.rowcount = 0
        if sql.startswith("SELECT 1 FROM users") or sql.startswith("SELECT 1 FROM products"):
            self.rows = [{"1": 1}]
        elif sql.startswith("SELECT id FROM product_skus WHERE product_id"):
            self.rows = [{"id": sku["id"]} for sku in self.skus.values() if sku["product_id"] == params[0]][:2]
        elif sql.startswith("SELECT id FROM product_skus WHERE id"):
            sku = self.skus.get(params[0])
            self.rows = [{"id": sku["id"]}] if sku and sku["product_id"] == params[1] else []
        elif sql.startswith("SELECT stock FROM product_skus"):
            sku = self.skus.get(params[0])
            self.rows = [deepcopy(sku)] if sku and sku["product_id"] == params[1] else []
        elif sql.startswith("INSERT INTO cart"):
            user, product, sku, qty, specs = params
            key = (user, product, sku)
            self.cart.setdefault(key, {"id": sku, "quantity": 0, "specifications": specs})
            self.cart[key]["quantity"] += qty
        elif sql.startswith("SELECT id, quantity FROM cart"):
            row = self.cart.get(params)
            self.rows = [deepcopy(row)] if row else []
        elif sql.startswith("DELETE FROM cart WHERE user_id"):
            if len(params) == 3:
                self.cart.pop(params, None)
            else:
                self.cart = {k: v for k, v in self.cart.items() if k[0] != params[0]}
        elif sql.startswith("DELETE FROM cart WHERE id"):
            self.cart = {k: v for k, v in self.cart.items() if v["id"] != params[0]}
        elif sql.startswith("UPDATE cart SET quantity"):
            for row in self.cart.values():
                if row["id"] == params[1]:
                    row["quantity"] = params[0]
        elif sql.startswith("SELECT is_member_product"):
            self.rows = [{"is_member_product": 0, "user_id": 0, "cash_only": 0}]
        elif sql.startswith("SELECT c.product_id"):
            self.rows = [dict(product_id=k[1], sku_id=k[2], quantity=v["quantity"], price=None,
                              is_vip=0, merchant_id=0, cash_only=0) for k, v in self.cart.items() if k[0] == params[0]]
        elif sql.startswith("SELECT price, stock"):
            sku = self.skus.get(params[0])
            self.rows = [deepcopy(sku)] if sku and sku["product_id"] == params[1] else []
        elif sql.startswith("UPDATE product_skus SET stock = stock -"):
            qty, sku_id, product_id, guard = params
            sku = self.skus[sku_id]
            if sku["product_id"] == product_id and sku["stock"] >= guard:
                sku["stock"] -= qty
                self.rowcount = 1
        elif sql.startswith("INSERT INTO orders"):
            self.orders.append(params)
            self.lastrowid = len(self.orders)
        elif sql.startswith("INSERT INTO order_items"):
            self.items.append(params)
        elif sql.startswith("SELECT order_number, status"):
            pass
        else:
            raise AssertionError(f"Unexpected SQL: {sql}")


@pytest.fixture
def db(monkeypatch):
    database = CommerceDB()
    monkeypatch.setattr(cart, "get_conn", database.connection)
    monkeypatch.setattr(order, "get_conn", database.connection)
    monkeypatch.setattr(order, "redis_client", None)
    return database


def create(items, **kwargs):
    return order.OrderManager.create(7, None, None, buy_now=True, buy_now_items=items,
                                     delivery_way="pickup", **kwargs)


@pytest.mark.parametrize("quantity", [0, -1, True, 1.5, "2", 2147483648])
@pytest.mark.parametrize("model", [cart.CartAdd, cart.CartDecrease, order.BuyNowItem])
def test_quantity_is_strict_positive_integer(model, quantity):
    with pytest.raises(ValidationError):
        model(user_id=7, product_id=1, sku_id=11, quantity=quantity)


def test_cart_distinguishes_skus_and_scopes_decrease_delete(db):
    cart.CartManager.add(7, 1, 2, sku_id=11)
    cart.CartManager.add(7, 1, 3, sku_id=12)
    cart.CartManager.add(7, 1, 1, sku_id=11)
    cart.CartManager.decrease(7, 1, 1, sku_id=12)
    assert db.cart[(7, 1, 11)]["quantity"] == 3
    assert db.cart[(7, 1, 12)]["quantity"] == 2
    cart.CartManager.remove(7, 1, sku_id=11)
    assert set(db.cart) == {(7, 1, 12)}


def test_cart_missing_sku_rejects_ambiguity_and_foreign_sku(db):
    for sku_id in (None, 21):
        with pytest.raises(HTTPException) as error:
            cart.CartManager.add(7, 1, sku_id=sku_id)
        assert error.value.status_code == 422
    assert db.cart == {}
    cart.CartManager.add(7, 2)
    assert set(db.cart) == {(7, 2, 21)}


@pytest.mark.parametrize("client_price", [None, 0, -100, "0.01", 999999])
def test_buy_now_ignores_client_price(db, client_price):
    item = {"product_id": 1, "sku_id": 12, "quantity": 2}
    if client_price is not None:
        item["price"] = client_price
    result = create([item])
    assert result["need_pay"] is True
    assert db.orders[0][3:5] == (Decimal("60"), Decimal("60"))
    assert db.items[0][4:] == (Decimal("30"), Decimal("60"))
    assert db.skus[12]["stock"] == 3
    assert db.skus[11]["stock"] == 5


def test_buy_now_rejects_wrong_sku_and_missing_multisku(db):
    for sku_id in (None, 21):
        with pytest.raises(HTTPException) as error:
            create([{"product_id": 1, "sku_id": sku_id, "quantity": 1}])
        assert error.value.status_code == 422
    assert db.orders == []
    assert db.skus[21]["stock"] == 5


def test_duplicate_lines_cannot_overdraw_and_transaction_rolls_back(db):
    item = {"product_id": 1, "sku_id": 11, "quantity": 3}
    with pytest.raises(HTTPException) as error:
        create([item, item])
    assert error.value.status_code == 400
    assert db.skus[11]["stock"] == 5
    assert db.orders == db.items == []


def test_cart_checkout_validates_legacy_quantity_and_sku(db):
    for sku_id, quantity in [(11, 0), (21, 1)]:
        db.cart = {(7, 1, sku_id): {"id": 1, "quantity": quantity}}
        with pytest.raises(HTTPException) as error:
            order.OrderManager.create(7, None, None, delivery_way="pickup")
        assert error.value.status_code == 422
    assert db.orders == []




def test_later_validation_failure_releases_reserved_stock(db):
    with pytest.raises(HTTPException):
        order.OrderManager.create(7, None, None, buy_now=True,
                                  buy_now_items=[{"product_id": 1, "sku_id": 11, "quantity": 1}])
    assert db.skus[11]["stock"] == 5
    assert db.orders == db.items == []


@pytest.mark.parametrize("quantity", [False, "1", 0, -1, 1.5])
def test_order_request_validates_nested_quantity(quantity):
    with pytest.raises(ValidationError):
        order.OrderCreate(user_id=7, buy_now=True,
                          buy_now_items=[{"product_id": 1, "sku_id": 11, "quantity": quantity}])




def test_cart_rejects_cumulative_overstock_without_mutation(db):
    cart.CartManager.add(7, 1, 4, sku_id=11)
    with pytest.raises(HTTPException) as error:
        cart.CartManager.add(7, 1, 2, sku_id=11)
    assert error.value.status_code == 400
    assert db.cart[(7, 1, 11)]["quantity"] == 4
    assert db.skus[11]["stock"] == 5


def test_explicit_delete_removes_missing_sku_only_for_target_user(db):
    cart.CartManager.add(7, 1, 1, sku_id=11)
    cart.CartManager.add(8, 1, 1, sku_id=11)
    cart.CartManager.add(7, 1, 1, sku_id=12)
    del db.skus[11]
    cart.CartManager.remove(7, 1, sku_id=11)
    assert set(db.cart) == {(8, 1, 11), (7, 1, 12)}
    result = order.OrderManager.create(7, None, None, delivery_way="pickup")
    assert result["need_pay"] is True
    assert db.items[0][2] == 12
