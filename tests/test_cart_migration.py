"""Opt-in real MySQL tests; create/drop only a fresh random pr8_test_* database."""
import os
import uuid
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from threading import Event, current_thread

import pymysql
import pytest
from fastapi import HTTPException

from api.order import cart, order
from scripts import migrate


@pytest.fixture
def mysql_db(monkeypatch):
    if os.getenv("PR8_MYSQL_TEST_ENABLE") != "1":
        pytest.skip("Requires explicit PR8_MYSQL_TEST_ENABLE=1 and isolated PR8_MYSQL_TEST_* settings")
    host = os.environ["PR8_MYSQL_TEST_HOST"]
    config = dict(host=host, port=int(os.environ["PR8_MYSQL_TEST_PORT"]),
                  user=os.environ["PR8_MYSQL_TEST_USER"], password=os.environ["PR8_MYSQL_TEST_PASSWORD"],
                  charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor, autocommit=False)
    name = "pr8_test_" + uuid.uuid4().hex
    admin = pymysql.connect(**config)
    with admin.cursor() as cur:
        cur.execute(f"CREATE DATABASE `{name}`")

    @contextmanager
    def connection():
        conn = pymysql.connect(database=name, **config)
        with conn.cursor() as cur:
            cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        try:
            yield conn
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def execute(sql, params=()):
        with connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                conn.commit()
                return rows

    try:
        for statement in [
            "CREATE TABLE users (id BIGINT UNSIGNED PRIMARY KEY)",
            "CREATE TABLE products (id BIGINT UNSIGNED PRIMARY KEY, is_member_product INT DEFAULT 0, user_id BIGINT DEFAULT 0, cash_only INT DEFAULT 0)",
            "CREATE TABLE product_skus (id BIGINT UNSIGNED PRIMARY KEY, product_id BIGINT UNSIGNED NOT NULL, price DECIMAL(12,2) NOT NULL, stock INT)",
            "CREATE TABLE cart (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, user_id BIGINT UNSIGNED NOT NULL, product_id BIGINT UNSIGNED NOT NULL, sku_id BIGINT UNSIGNED NULL, quantity INT DEFAULT 1, specifications JSON, selected INT DEFAULT 1, UNIQUE KEY old_unique (user_id,product_id))",
            "CREATE TABLE coupons (id BIGINT UNSIGNED PRIMARY KEY)",
            "CREATE TABLE orders (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, user_id BIGINT, merchant_id BIGINT, order_number VARCHAR(64), total_amount DECIMAL(12,2), original_amount DECIMAL(12,2), status VARCHAR(32), is_vip_item INT, consignee_name VARCHAR(64), consignee_phone VARCHAR(32), province VARCHAR(64), city VARCHAR(64), district VARCHAR(64), shipping_address VARCHAR(255), delivery_way VARCHAR(32), pay_way VARCHAR(32), auto_recv_time DATETIME, refund_reason TEXT, expire_at DATETIME, pending_points DECIMAL(12,4), pending_coupon_id BIGINT, pending_coupon_ids JSON, points_discount DECIMAL(12,2), coupon_discount DECIMAL(12,2), created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP)",
            "CREATE TABLE order_items (id BIGINT AUTO_INCREMENT PRIMARY KEY, order_id BIGINT, product_id BIGINT, sku_id BIGINT NULL, quantity INT, unit_price DECIMAL(12,2), total_price DECIMAL(12,2))",
            "CREATE TABLE pending_rewards (id BIGINT AUTO_INCREMENT PRIMARY KEY, order_id BIGINT, status VARCHAR(32))",
            "ALTER TABLE orders MODIFY user_id BIGINT UNSIGNED, ADD FOREIGN KEY (user_id) REFERENCES users(id)",
            "ALTER TABLE cart ADD FOREIGN KEY (user_id) REFERENCES users(id)",
            "INSERT INTO users VALUES (7),(8)",
            "INSERT INTO products (id) VALUES (1),(2)",
            "INSERT INTO product_skus VALUES (11,1,20,5),(12,1,30,5),(21,2,40,5)",
        ]:
            execute(statement)
        monkeypatch.setattr(migrate, "get_conn", connection)
        monkeypatch.setattr(cart, "get_conn", connection)
        monkeypatch.setattr(order, "get_conn", connection)
        monkeypatch.setattr(order, "redis_client", None)
        yield execute
    finally:
        with admin.cursor() as cur:
            cur.execute(f"DROP DATABASE `{name}`")
        admin.close()


def test_real_migrations_repeat_and_preserve_data(mysql_db):
    mysql_db("INSERT INTO cart(user_id,product_id,quantity) VALUES (7,2,2)")
    path = migrate.ROOT / "migrations" / "003_rain_point_monthly_coupon.sql"
    for _ in range(2):
        migrate.run_sql_file(path, False)
        migrate.ensure_cart_sku_key(False)
    assert mysql_db("SELECT sku_id,quantity FROM cart") == [{"sku_id": 21, "quantity": 2}]
    mysql_db("INSERT INTO coupons(id,rain_settlement_id) VALUES (1,99)")
    assert mysql_db("SELECT rain_settlement_id FROM coupons WHERE id=1") == [{"rain_settlement_id": 99}]
    mysql_db("INSERT INTO cart(user_id,product_id,sku_id,quantity) VALUES (7,1,11,1),(7,1,12,2)")
    assert len(mysql_db("SELECT * FROM cart WHERE user_id=7 AND product_id=1")) == 2


@pytest.mark.parametrize("sku,quantity", [(None,1),(99,1),(11,0),(11,-1)])
def test_real_invalid_legacy_rows_block_without_changes(mysql_db, sku, quantity):
    mysql_db("INSERT INTO cart(user_id,product_id,sku_id,quantity) VALUES (7,1,%s,%s)", (sku, quantity))
    before = mysql_db("SELECT * FROM cart")
    with pytest.raises(RuntimeError):
        migrate.ensure_cart_sku_key(False)
    assert mysql_db("SELECT * FROM cart") == before
    mysql_db("INSERT INTO cart(user_id,product_id,sku_id,quantity) VALUES (8,2,NULL,1)")
    assert mysql_db("SELECT sku_id FROM cart WHERE user_id=8") == [{"sku_id": None}]


def test_real_duplicate_resolved_skus_block_unchanged(mysql_db):
    mysql_db("CREATE INDEX idx_cart_user_id ON cart(user_id)")
    mysql_db("ALTER TABLE cart DROP INDEX old_unique")
    mysql_db("INSERT INTO cart(user_id,product_id,sku_id,quantity) VALUES (7,2,NULL,1),(7,2,21,2)")
    before = mysql_db("SELECT * FROM cart ORDER BY id")
    with pytest.raises(RuntimeError):
        migrate.ensure_cart_sku_key(False)
    assert mysql_db("SELECT * FROM cart ORDER BY id") == before


def purchase(quantity, sku=11):
    return order.OrderManager.create(7, None, None, buy_now=True,
                                    buy_now_items=[{"product_id": 1,"sku_id": sku,"quantity": quantity}],
                                    delivery_way="pickup")


def test_real_last_unit_competing_orders(mysql_db):
    migrate.ensure_cart_sku_key(False)
    mysql_db("UPDATE product_skus SET stock=1 WHERE id=11")

    def reserve(_):
        try:
            purchase(1)
            return 200
        except HTTPException as exc:
            return exc.status_code

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(reserve, range(2))) == [200,400]
    assert mysql_db("SELECT stock FROM product_skus WHERE id=11") == [{"stock": 0}]
    assert mysql_db("SELECT COUNT(*) AS n FROM orders") == [{"n": 1}]


def test_real_duplicate_lines_and_later_failure_rollback(mysql_db):
    item = {"product_id": 1,"sku_id": 11,"quantity": 3}
    with pytest.raises(HTTPException):
        order.OrderManager.create(7,None,None,buy_now=True,buy_now_items=[item,item],delivery_way="pickup")
    with pytest.raises(HTTPException):
        order.OrderManager.create(7,None,None,buy_now=True,buy_now_items=[item])
    assert mysql_db("SELECT stock FROM product_skus WHERE id=11") == [{"stock": 5}]
    assert mysql_db("SELECT COUNT(*) AS n FROM orders") == [{"n": 0}]


def test_real_cumulative_cart_concurrent_adds(mysql_db):
    migrate.ensure_cart_sku_key(False)
    cart.CartManager.add(7,1,3,sku_id=11)

    def add(_):
        try:
            cart.CartManager.add(7,1,2,sku_id=11)
            return 200
        except HTTPException as exc:
            return exc.status_code

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(add, range(2))) == [200,400]
    assert mysql_db("SELECT quantity FROM cart") == [{"quantity": 5}]
    assert mysql_db("SELECT stock FROM product_skus WHERE id=11") == [{"stock": 5}]


def test_real_removed_sku_expiry_does_not_block_healthy_order(mysql_db):
    lost = purchase(1,11)
    healthy = purchase(2,12)
    mysql_db("UPDATE orders SET expire_at=DATE_SUB(NOW(),INTERVAL 1 MINUTE)")
    mysql_db("DELETE FROM product_skus WHERE id=11")
    assert order._cancel_expire_orders_once() == 1
    states = {row["order_number"]: row["status"] for row in mysql_db("SELECT order_number,status FROM orders")}
    assert states[lost["order_number"]] == "pending_pay"
    assert states[healthy["order_number"]] == "cancelled"
    assert mysql_db("SELECT stock FROM product_skus WHERE id=12") == [{"stock": 5}]
    assert order._cancel_expire_orders_once() == 0
    assert mysql_db("SELECT stock FROM product_skus WHERE id=12") == [{"stock": 5}]


def test_real_missing_cart_sku_cleanup_allows_checkout(mysql_db):
    migrate.ensure_cart_sku_key(False)
    cart.CartManager.add(7,1,1,sku_id=11)
    cart.CartManager.add(8,1,1,sku_id=11)
    cart.CartManager.add(7,1,1,sku_id=12)
    mysql_db("DELETE FROM product_skus WHERE id=11")
    cart.CartManager.remove(7,1,11)
    assert mysql_db("SELECT user_id FROM cart WHERE sku_id=11") == [{"user_id": 8}]
    result = order.OrderManager.create(7,None,None,delivery_way="pickup")
    assert result["need_pay"] is True
    assert mysql_db("SELECT sku_id,unit_price FROM order_items") == [{"sku_id":12,"unit_price":Decimal("30")}]


def test_real_first_cart_add_read_committed_interleaving(mysql_db, monkeypatch):
    migrate.ensure_cart_sku_key(False)
    original_connection = cart.get_conn
    first_at_stock = Event()
    release_first = Event()

    class PausedCursor:
        def __init__(self, cursor):
            self.cursor = cursor

        def __enter__(self):
            self.cursor.__enter__()
            return self

        def __exit__(self, *args):
            return self.cursor.__exit__(*args)

        def __getattr__(self, name):
            return getattr(self.cursor, name)

        def execute(self, sql, params=()):
            # Pause the first transaction after its empty-cart read, before SKU locking.
            if current_thread().name.startswith("first_add") and sql.startswith("SELECT stock FROM product_skus"):
                first_at_stock.set()
                if not release_first.wait(15):
                    raise TimeoutError("first add was not released")
            return self.cursor.execute(sql, params)

    class PausedConnection:
        def __init__(self, conn):
            self.conn = conn

        def cursor(self):
            return PausedCursor(self.conn.cursor())

        def __getattr__(self, name):
            return getattr(self.conn, name)

    @contextmanager
    def controlled_connection():
        with original_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SET SESSION innodb_lock_wait_timeout=1")
            yield PausedConnection(conn)

    monkeypatch.setattr(cart, "get_conn", controlled_connection)
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="first_add") as pool:
        first = pool.submit(cart.CartManager.add, 7, 1, 3, None, 11)
        try:
            assert first_at_stock.wait(10)
            # With the owner lock, this transaction must time out before reading cart.
            # Without it, it inserts 3 while the paused transaction retains a stale 0.
            second_waited = False
            try:
                cart.CartManager.add(7,1,3,sku_id=11)
            except pymysql.err.OperationalError as exc:
                if exc.args[0] != 1205:
                    raise
                second_waited = True
        finally:
            release_first.set()
        assert first.result(timeout=10) is True
    if second_waited:
        with pytest.raises(HTTPException) as error:
            cart.CartManager.add(7,1,3,sku_id=11)
        assert error.value.status_code == 400
    assert mysql_db("SELECT quantity FROM cart WHERE user_id=7 AND sku_id=11") == [{"quantity":3}]
    assert mysql_db("SELECT stock FROM product_skus WHERE id=11") == [{"stock":5}]


def test_real_buy_now_and_cart_add_fk_lock_order(mysql_db, monkeypatch):
    migrate.ensure_cart_sku_key(False)
    original_connection = cart.get_conn
    add_ready = Event()
    release_add = Event()

    class Cursor:
        def __init__(self, cursor):
            self.cursor = cursor

        def __enter__(self):
            self.cursor.__enter__()
            return self

        def __exit__(self, *args):
            return self.cursor.__exit__(*args)

        def __getattr__(self, name):
            return getattr(self.cursor, name)

        def execute(self, sql, params=()):
            if current_thread().name.startswith("held_add") and sql.startswith("SELECT stock FROM product_skus"):
                add_ready.set()
                if not release_add.wait(15):
                    raise TimeoutError("add was not released")
            if not current_thread().name.startswith("held_add") and "INSERT INTO orders(" in sql:
                # A regressed buy-now owns SKU here: let add request it while buy
                # inserts its FK child, exposing the actual InnoDB lock cycle.
                release_add.set()
            return self.cursor.execute(sql, params)

    class Connection:
        def __init__(self, conn):
            self.conn = conn

        def cursor(self):
            return Cursor(self.conn.cursor())

        def __getattr__(self, name):
            return getattr(self.conn, name)

    @contextmanager
    def controlled():
        with original_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SET SESSION innodb_lock_wait_timeout=2")
            yield Connection(conn)

    monkeypatch.setattr(cart, "get_conn", controlled)
    monkeypatch.setattr(order, "get_conn", controlled)

    def add():
        return cart.CartManager.add(7,1,1,sku_id=11)

    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="held_add") as pool:
        future = pool.submit(add)
        try:
            assert add_ready.wait(10)
            try:
                purchase(1)
            except pymysql.err.OperationalError as exc:
                # Owner contention may time out; a deadlock victim (1213) is not acceptable.
                assert exc.args[0] == 1205
        finally:
            release_add.set()
        assert future.result(timeout=10) is True
    purchase(1)
    assert mysql_db("SELECT quantity FROM cart WHERE user_id=7 AND sku_id=11") == [{"quantity":1}]
    assert mysql_db("SELECT stock FROM product_skus WHERE id=11") == [{"stock":4}]
    assert mysql_db("SELECT COUNT(*) AS n FROM orders") == [{"n":1}]
