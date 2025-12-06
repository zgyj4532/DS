# database_setup.py - 表结构与项目2完全一致
import logging
#import pymysql
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
from config import get_db_config, PLATFORM_MERCHANT_ID, MEMBER_PRODUCT_PRICE

logger = logging.getLogger(__name__)

_engine = None
_SessionFactory = None

def get_engine():
    global _engine
    if _engine is None:
        try:
            cfg = get_db_config()
            connection_url = (
                f"mysql+pymysql://{cfg['user']}:{cfg['password']}"
                f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
                f"?charset={cfg['charset']}"
            )
            _engine = create_engine(
                connection_url,
                poolclass=QueuePool,
                pool_size=20,
                max_overflow=30,
                pool_timeout=30,
                pool_pre_ping=True,
                echo=False
            )
            logger.info("✅ SQLAlchemy 引擎已创建 (pool_size=20)")
        except Exception as e:
            logger.error(f"❌ SQLAlchemy 引擎创建失败: {e}")
            raise
    return _engine

def get_session_factory():
    global _SessionFactory
    if _SessionFactory is None:
        engine = get_engine()
        _SessionFactory = sessionmaker(
            bind=engine,
            autocommit=False,
            autoflush=False,
            expire_on_commit=False
        )
        logger.info("✅ 会话工厂已创建")
    return _SessionFactory

def get_db_session():
    factory = get_session_factory()
    db = scoped_session(factory)()
    try:
        yield db
    finally:
        db.close()

def get_conn(**kw):
    """获取 pymysql 数据库连接（兼容 order 模块）
    
    参数:
        **kw: 可选的数据库配置参数，会覆盖默认配置
        
    返回:
        pymysql.Connection: 数据库连接对象
    """
    import pymysql
    cfg = get_db_config().copy()
    cfg.update(kw)
    return pymysql.connect(
        **cfg,
        cursorclass=pymysql.cursors.DictCursor
    )

class DatabaseManager:
    def __init__(self):
        self._ensure_database_exists()

    def _ensure_database_exists(self):
        try:
            temp_config = get_db_config().copy()
            database = temp_config.pop('database')
            import pymysql
            conn = pymysql.connect(**temp_config)
            cursor = conn.cursor()
            cursor.execute(
                f"CREATE DATABASE IF NOT EXISTS `{database}` "
                f"DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
            conn.commit()
            conn.close()
            logger.info(f"✅ 数据库 `{database}` 已就绪")
        except Exception as e:
            logger.error(f"❌ 数据库初始化失败: {e}")
            raise

    def init_all_tables(self, conn):
        logger.info("\n=== 初始化数据库表结构 ===")

        tables = {
            'users': """
                CREATE TABLE IF NOT EXISTS users (
                    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    mobile VARCHAR(30) UNIQUE NOT NULL,
                    password_hash CHAR(60) NOT NULL,
                    name VARCHAR(50) NOT NULL,
                    member_level TINYINT NOT NULL DEFAULT 0,
                    points BIGINT NOT NULL DEFAULT 0,
                    promotion_balance DECIMAL(14,2) NOT NULL DEFAULT 0.00,
                    merchant_points BIGINT NOT NULL DEFAULT 0,
                    merchant_balance DECIMAL(14,2) NOT NULL DEFAULT 0.00,
                    status TINYINT NOT NULL DEFAULT 1,
                    level_changed_at DATETIME NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_mobile (mobile),
                    INDEX idx_member_level (member_level)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'products': """
                CREATE TABLE IF NOT EXISTS products (
                    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    sku VARCHAR(64) UNIQUE NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    price DECIMAL(12,2) NOT NULL,
                    stock INT NOT NULL DEFAULT 0,
                    is_member_product TINYINT(1) NOT NULL DEFAULT 0,
                    status TINYINT NOT NULL DEFAULT 1,
                    merchant_id BIGINT UNSIGNED NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_is_member_product (is_member_product),
                    INDEX idx_merchant (merchant_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'orders': """
                CREATE TABLE IF NOT EXISTS orders (
                    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    order_no VARCHAR(64) UNIQUE NOT NULL,
                    user_id BIGINT UNSIGNED NOT NULL,
                    merchant_id BIGINT UNSIGNED NOT NULL,
                    total_amount DECIMAL(12,2) NOT NULL,
                    original_amount DECIMAL(12,2) NOT NULL,
                    points_discount DECIMAL(12,2) NOT NULL DEFAULT 0.00,
                    is_member_order TINYINT(1) NOT NULL DEFAULT 0,
                    status VARCHAR(30) NOT NULL DEFAULT 'completed',
                    refund_status VARCHAR(30) DEFAULT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_user (user_id),
                    INDEX idx_order_no (order_no),
                    INDEX idx_created_at (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'order_items': """
                CREATE TABLE IF NOT EXISTS order_items (
                    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    order_id BIGINT UNSIGNED NOT NULL,
                    product_id BIGINT UNSIGNED NOT NULL,
                    quantity INT NOT NULL DEFAULT 1,
                    unit_price DECIMAL(12,2) NOT NULL,
                    total_price DECIMAL(12,2) NOT NULL,
                    INDEX idx_order (order_id),
                    INDEX idx_product (product_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'finance_accounts': """
                CREATE TABLE IF NOT EXISTS finance_accounts (
                    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    account_name VARCHAR(100) NOT NULL,
                    account_type VARCHAR(50) UNIQUE NOT NULL,
                    balance DECIMAL(14,2) NOT NULL DEFAULT 0.00,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uk_account_type (account_type)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'account_flow': """
                CREATE TABLE IF NOT EXISTS account_flow (
                    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    account_id BIGINT UNSIGNED,
                    related_user BIGINT UNSIGNED,
                    account_type VARCHAR(50),
                    change_amount DECIMAL(14,2) NOT NULL,
                    balance_after DECIMAL(14,2),
                    flow_type VARCHAR(50),
                    remark VARCHAR(255),
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_account (account_id),
                    INDEX idx_related_user (related_user),
                    INDEX idx_created_at (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'points_log': """
                CREATE TABLE IF NOT EXISTS points_log (
                    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT UNSIGNED NOT NULL,
                    change_amount BIGINT NOT NULL,
                    balance_after BIGINT NOT NULL,
                    type ENUM('member','merchant') NOT NULL,
                    reason VARCHAR(255),
                    related_order BIGINT UNSIGNED,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_user (user_id),
                    INDEX idx_order (related_order)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'user_referrals': """
                CREATE TABLE IF NOT EXISTS user_referrals (
                    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT UNSIGNED UNIQUE NOT NULL,
                    referrer_id BIGINT UNSIGNED,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_referrer (referrer_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'pending_rewards': """
                CREATE TABLE IF NOT EXISTS pending_rewards (
                    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT UNSIGNED NOT NULL,
                    reward_type ENUM('referral','team') NOT NULL,
                    amount DECIMAL(12,2) NOT NULL,
                    order_id BIGINT UNSIGNED NOT NULL,
                    layer TINYINT DEFAULT NULL,
                    status ENUM('pending','approved','rejected') DEFAULT 'pending',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_user_status (user_id, status),
                    INDEX idx_order_id (order_id),
                    INDEX idx_status (status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'coupons': """
                CREATE TABLE IF NOT EXISTS coupons (
                    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT UNSIGNED NOT NULL,
                    coupon_type ENUM('user','merchant') NOT NULL,
                    amount DECIMAL(14,2) NOT NULL,
                    status ENUM('unused','used','expired') NOT NULL DEFAULT 'unused',
                    valid_from DATE NOT NULL,
                    valid_to DATE NOT NULL,
                    used_at DATETIME DEFAULT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_user_status (user_id, status),
                    INDEX idx_valid_to (valid_to)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'withdrawals': """
                CREATE TABLE IF NOT EXISTS withdrawals (
                    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT UNSIGNED NOT NULL,
                    amount DECIMAL(14,2) NOT NULL,
                    tax_amount DECIMAL(14,2) NOT NULL DEFAULT 0.00,
                    actual_amount DECIMAL(14,2) NOT NULL DEFAULT 0.00,
                    status VARCHAR(30) NOT NULL DEFAULT 'pending_auto',
                    audit_remark VARCHAR(255) DEFAULT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    processed_at DATETIME DEFAULT NULL,
                    INDEX idx_user_status (user_id, status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'team_rewards': """
                CREATE TABLE IF NOT EXISTS team_rewards (
                    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT UNSIGNED NOT NULL,
                    from_user_id BIGINT UNSIGNED NOT NULL,
                    order_id BIGINT UNSIGNED,
                    layer TINYINT NOT NULL,
                    reward_amount DECIMAL(12,2) NOT NULL DEFAULT 0.00,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_user_id (user_id),
                    INDEX idx_from_user_id (from_user_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'weekly_subsidy_records': """
                CREATE TABLE IF NOT EXISTS weekly_subsidy_records (
                    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT UNSIGNED NOT NULL,
                    week_start DATE NOT NULL,
                    subsidy_amount DECIMAL(14,2) NOT NULL,
                    points_before BIGINT NOT NULL,
                    points_deducted BIGINT NOT NULL,
                    coupon_id BIGINT UNSIGNED,
                    INDEX idx_user_week (user_id, week_start)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'director_dividends': """
                CREATE TABLE IF NOT EXISTS director_dividends (
                    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT UNSIGNED NOT NULL,
                    period_date DATE NOT NULL,
                    dividend_amount DECIMAL(14,2) NOT NULL DEFAULT 0.00,
                    status VARCHAR(30) NOT NULL DEFAULT 'pending',
                    paid_at DATETIME DEFAULT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_user_id (user_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            # ========== 订单系统相关表（来自 order/database_setup1.py） ==========
            'merchants': """
                CREATE TABLE IF NOT EXISTS Merchants (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    login_user VARCHAR(50) UNIQUE NOT NULL,
                    login_pwd VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'merchant_balance': """
                CREATE TABLE IF NOT EXISTS Merchant_Balance (
                    merchant_id INT PRIMARY KEY,
                    balance DECIMAL(10,2) NOT NULL DEFAULT 0,
                    bank_name VARCHAR(100) DEFAULT '',
                    bank_account VARCHAR(50) DEFAULT '',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY (merchant_id) REFERENCES Merchants(id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'order_users': """
                CREATE TABLE IF NOT EXISTS Users (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    phone VARCHAR(20),
                    points INT DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'order_products': """
                CREATE TABLE IF NOT EXISTS Products (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    sku VARCHAR(50) UNIQUE NOT NULL,
                    price DECIMAL(10,2) NOT NULL,
                    stock INT DEFAULT 0,
                    category VARCHAR(100),
                    image_url VARCHAR(500),
                    description TEXT,
                    is_vip TINYINT(1) DEFAULT 0 COMMENT '1=会员商品',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'cart': """
                CREATE TABLE IF NOT EXISTS Cart (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    user_id INT NOT NULL,
                    product_id INT NOT NULL,
                    quantity INT DEFAULT 1,
                    selected TINYINT DEFAULT 1,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES Users(id) ON DELETE CASCADE,
                    FOREIGN KEY (product_id) REFERENCES Products(id) ON DELETE CASCADE,
                    UNIQUE KEY uk_user_product (user_id, product_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'order_orders': """
                CREATE TABLE IF NOT EXISTS Orders (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    user_id INT NOT NULL,
                    order_number VARCHAR(50) UNIQUE NOT NULL,
                    total_amount DECIMAL(10,2) NOT NULL,
                    status ENUM('pending_pay','pending_ship','pending_recv','refund','completed') DEFAULT 'pending_pay',
                    consignee_name VARCHAR(100) NOT NULL,
                    consignee_phone VARCHAR(20) NOT NULL,
                    province VARCHAR(20) NOT NULL DEFAULT '',
                    city VARCHAR(20) NOT NULL DEFAULT '',
                    district VARCHAR(20) NOT NULL DEFAULT '',
                    shipping_address TEXT NOT NULL,
                    pay_way ENUM('alipay','wechat','card','wx_pub','wx_app') DEFAULT 'alipay',
                    is_vip_item TINYINT(1) DEFAULT 0 COMMENT '1=含会员商品',
                    refund_reason TEXT,
                    auto_recv_time DATETIME NULL COMMENT '7 天后自动收货',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES Users(id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'order_items_extended': """
                CREATE TABLE IF NOT EXISTS Order_Items (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    order_id INT NOT NULL,
                    product_id INT NOT NULL,
                    quantity INT NOT NULL,
                    unit_price DECIMAL(10,2) NOT NULL,
                    total_price DECIMAL(10,2) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (order_id) REFERENCES Orders(id) ON DELETE CASCADE,
                    FOREIGN KEY (product_id) REFERENCES Products(id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'refunds': """
                CREATE TABLE IF NOT EXISTS Refunds (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    order_number VARCHAR(50) NOT NULL,
                    refund_type ENUM('return','refund_only') NOT NULL COMMENT 'return=退货退款，refund_only=仅退款',
                    reason TEXT NOT NULL,
                    status ENUM('applied','seller_ok','success','rejected') DEFAULT 'applied',
                    reject_reason TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY (order_number) REFERENCES Orders(order_number) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'user_addresses': """
                CREATE TABLE IF NOT EXISTS User_Addresses (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    user_id INT NOT NULL,
                    label VARCHAR(20) NOT NULL COMMENT '家/公司/朋友',
                    consignee_name VARCHAR(100) NOT NULL,
                    consignee_phone VARCHAR(20) NOT NULL,
                    province VARCHAR(20) NOT NULL DEFAULT '',
                    city VARCHAR(20) NOT NULL DEFAULT '',
                    district VARCHAR(20) NOT NULL DEFAULT '',
                    detail TEXT NOT NULL,
                    lng DECIMAL(10,6) NULL,
                    lat DECIMAL(10,6) NULL,
                    is_default TINYINT(1) DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES Users(id) ON DELETE CASCADE,
                    INDEX idx_user (user_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'order_split': """
                CREATE TABLE IF NOT EXISTS order_split (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    order_number VARCHAR(50) NOT NULL,
                    item_type ENUM('merchant','pool') NOT NULL,
                    amount DECIMAL(10,2) NOT NULL,
                    pool_type VARCHAR(20) NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'merchant_statement': """
                CREATE TABLE IF NOT EXISTS merchant_statement (
                    merchant_id INT NOT NULL,
                    date DATE NOT NULL,
                    opening_balance DECIMAL(10,2) NOT NULL,
                    income DECIMAL(10,2) NOT NULL,
                    withdraw DECIMAL(10,2) NOT NULL,
                    closing_balance DECIMAL(10,2) NOT NULL,
                    PRIMARY KEY (merchant_id, date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'alert_order': """
                CREATE TABLE IF NOT EXISTS alert_order (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    order_number VARCHAR(50) NOT NULL,
                    alert_type VARCHAR(50) NOT NULL,
                    detail TEXT NOT NULL,
                    is_handled TINYINT(1) DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
        }

        for table_name, sql in tables.items():
            conn.execute(text(sql))
            logger.info(f"✅ 表 `{table_name}` 已创建/确认")

        self._init_finance_accounts(conn)
        logger.info("✅ 所有表结构初始化完成")

    def _init_finance_accounts(self, conn):
        accounts = [
            ('周补贴池', 'subsidy_pool'),
            ('公益基金', 'public_welfare'),
            ('平台维护', 'platform'),
            ('荣誉董事分红', 'honor_director'),
            ('社区店', 'community'),
            ('城市运营中心', 'city_center'),
            ('大区分公司', 'region_company'),
            ('事业发展基金', 'development'),
            ('公司积分账户', 'company_points'),
            ('公司余额账户', 'company_balance'),
            ('平台收入池（会员商品）', 'platform_revenue_pool'),
        ]

        conn.execute(text("DELETE FROM finance_accounts"))
        for name, acc_type in accounts:
            conn.execute(
                text("INSERT INTO finance_accounts (account_name, account_type, balance) VALUES (:name, :type, 0)"),
                {"name": name, "type": acc_type}
            )
        logger.info(f"✅ 初始化 {len(accounts)} 个资金池账户")

    def create_test_data(self, conn) -> int:
        logger.info("\n--- 创建测试数据 ---")

        pwd_hash = '$2b$12$9LjsHS5r4u1M9K4nG5KZ7e6zZxZn7qZ'

        mobile = '13800138004'
        # 幂等处理：如果手机号已存在则复用该用户，否则插入新用户
        existing = conn.execute(
            text("SELECT id FROM users WHERE mobile = :mobile"),
            {"mobile": mobile}
        ).fetchone()
        if existing and getattr(existing, 'id', None):
            merchant_id = existing.id
            logger.info(f"ℹ️ 测试商家手机号已存在，复用商家ID: {merchant_id}")
        else:
            result = conn.execute(
                text("INSERT INTO users (mobile, password_hash, name, status) VALUES (:mobile, :pwd, :name, 1)"),
                {"mobile": mobile, "pwd": pwd_hash, "name": '优质商家'}
            )
            merchant_id = result.lastrowid

        # 创建会员商品（若 SKU 已存在则跳过）
        sku_member = 'SKU-MEMBER-001'
        existing_prod = conn.execute(
            text("SELECT id FROM products WHERE sku = :sku"),
            {"sku": sku_member}
        ).fetchone()
        if existing_prod and getattr(existing_prod, 'id', None):
            logger.info(f"ℹ️ 会员商品 SKU 已存在，跳过插入，product_id={existing_prod.id}")
        else:
            conn.execute(
                text("""INSERT INTO products (sku, name, price, stock, is_member_product, merchant_id, status)
                        VALUES (:sku, :name, :price, 100, 1, :merchant_id, 1)"""),
                {"sku": sku_member, "name": '会员星卡', "price": float(MEMBER_PRODUCT_PRICE), "merchant_id": PLATFORM_MERCHANT_ID}
            )

        # 创建普通商品（若 SKU 已存在则跳过）
        sku_normal = 'SKU-NORMAL-001'
        existing_normal = conn.execute(
            text("SELECT id FROM products WHERE sku = :sku"),
            {"sku": sku_normal}
        ).fetchone()
        if existing_normal and getattr(existing_normal, 'id', None):
            logger.info(f"ℹ️ 普通商品 SKU 已存在，跳过插入，product_id={existing_normal.id}")
        else:
            conn.execute(
                text("""INSERT INTO products (sku, name, price, stock, is_member_product, merchant_id, status)
                        VALUES (:sku, :name, 500.00, 200, 0, :merchant_id, 1)"""),
                {"sku": sku_normal, "name": '普通商品', "merchant_id": merchant_id}
            )

        conn.commit()
        logger.info(f"✅ 测试数据创建完成 | 商家ID: {merchant_id}")
        return merchant_id


# ==================== 数据库初始化函数 ====================

def create_database():
    """根据 `.env` 中的配置，创建 MySQL 数据库（如果不存在）。

    该函数会加载环境变量，然后以不指定数据库的方式连接 MySQL，执行
    `CREATE DATABASE IF NOT EXISTS ...`。调用者可在程序启动时先调用此函数。
    """
    import pymysql
    cfg = get_db_config()
    host = cfg['host']
    port = cfg['port']
    user = cfg['user']
    password = cfg['password']
    dbname = cfg['database']

    conn = pymysql.connect(host=host, port=port, user=user, password=password, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{dbname}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
            )
    finally:
        conn.close()

    print("数据库创建完成（若不存在）。")


def initialize_database():
    """初始化数据库表结构（如果尚未创建）。

    先确保数据库存在（调用 `create_database()`），再通过 SQLAlchemy 创建表结构。
    """
    print("正在检查数据库表结构...")
    # 确保数据库已创建
    create_database()

    engine = get_engine()
    with engine.connect() as conn:
        with conn.begin():
            db_manager = DatabaseManager()
            db_manager.init_all_tables(conn)

    print("数据库表结构初始化完成。")


def create_test_data():
    """创建测试数据（可选）"""
    print("正在创建测试数据...")
    engine = get_engine()
    with engine.connect() as conn:
        with conn.begin():
            db_manager = DatabaseManager()
            db_manager.create_test_data(conn)
    print("测试数据创建完成。")


# ==================== 订单系统相关函数（来自 order/database_setup1.py） ====================

def init_db():
    """一键初始化数据库（兼容 order 模块的接口）
    
    这是 initialize_database() 的别名，用于保持与 order 模块的兼容性。
    """
    initialize_database()
    print("[database_setup] 初始化完成！")


def auto_receive_task(db_cfg: dict = None):
    """自动收货和结算守护进程
    
    该函数会启动一个后台线程，每小时检查一次待收货订单，
    如果订单超过自动收货时间，则自动完成订单并结算给商家。
    
    参数:
        db_cfg: 数据库配置字典（可选，默认使用 get_db_config()）
    
    注意:
        这是一个守护进程，会在后台持续运行。
        需要在应用启动时调用此函数。
    """
    import threading
    import time
    from datetime import datetime
    
    def run():
        while True:
            try:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        now = datetime.now()
                        cur.execute(
                            "SELECT id, order_number, total_amount FROM Orders "
                            "WHERE status='pending_recv' AND auto_recv_time<=%s",
                            (now,)
                        )
                        for row in cur.fetchall():
                            cur.execute(
                                "UPDATE Orders SET status='completed' WHERE id=%s",
                                (row["id"],)
                            )
                            # 注意：settle_to_merchant 函数需要从 order 模块导入
                            # 这里只做订单状态更新，结算逻辑需要单独处理
                            conn.commit()
                            logger.info(f"[auto_receive] 订单 {row['order_number']} 已自动完成。")
            except Exception as e:
                logger.error(f"[auto_receive] 异常: {e}")
            time.sleep(3600)  # 每小时检查一次
    
    t = threading.Thread(target=run, daemon=True)
    t.start()
    logger.info("✅ 自动收货守护进程已启动")