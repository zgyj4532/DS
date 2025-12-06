# config.py - 融合 config1.py 和 config2.py
from decimal import Decimal
from enum import StrEnum, IntEnum
from typing import Final
import os
import pymysql
from contextlib import contextmanager
from dotenv import load_dotenv


# ==================== 数据库配置 ====================
load_dotenv()

def get_db_config():
    """获取数据库配置字典"""
    cfg = {
        'host': os.getenv('MYSQL_HOST', '127.0.0.1'),
        'port': int(os.getenv('MYSQL_PORT', 3306)),
        'user': os.getenv('MYSQL_USER'),
        'password': os.getenv('MYSQL_PASSWORD'),
        'database': os.getenv('MYSQL_DATABASE'),
        'charset': 'utf8mb4',
    }
    missing = [k for k in ('user', 'password', 'database') if not cfg.get(k)]
    if missing:
        raise RuntimeError(f"缺少必要的数据库环境变量: {', '.join(missing)}\n")
    return cfg

# 兼容 config2.py 的 CFG 格式
CFG = {
    "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
    "port": int(os.getenv("MYSQL_PORT", 3306)),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "rootpass"),
    "db": os.getenv("MYSQL_DATABASE", "userdb"),
    "charset": "utf8mb4",
    "autocommit": True,
}

# 兼容 order/config.py 的 DB_CONFIG 格式
DB_CONFIG = {
    "host": os.getenv("DB_HOST", os.getenv("MYSQL_HOST", "127.0.0.1")),
    "port": int(os.getenv("DB_PORT", os.getenv("MYSQL_PORT", 3306))),
    "user": os.getenv("DB_USER", os.getenv("MYSQL_USER", "root")),
    "password": os.getenv("DB_PWD", os.getenv("MYSQL_PASSWORD", "123456")),
    "database": os.getenv("DB_NAME", os.getenv("MYSQL_DATABASE", "ecommerce")),
    "charset": "utf8mb4",
}

@contextmanager
def get_conn():
    """获取数据库连接的上下文管理器（兼容 config2.py）"""
    cfg = get_db_config()
    conn = pymysql.connect(
        host=cfg['host'],
        port=cfg['port'],
        user=cfg['user'],
        password=cfg['password'],
        database=cfg['database'],
        charset=cfg['charset'],
        cursorclass=pymysql.cursors.DictCursor
    )
    try:
        yield conn
    finally:
        conn.close()


# ==================== 平台常量 ====================
PLATFORM_MERCHANT_ID: Final[int] = 0
MEMBER_PRODUCT_PRICE: Final[Decimal] = Decimal('1980.00')

# ==================== 业务规则枚举 ====================
class AllocationKey(StrEnum):
    PUBLIC_WELFARE = 'public_welfare'
    PLATFORM = 'platform'
    SUBSIDY_POOL = 'subsidy_pool'
    HONOR_DIRECTOR = 'honor_director'
    COMMUNITY = 'community'
    CITY_CENTER = 'city_center'
    REGION_COMPANY = 'region_company'
    DEVELOPMENT = 'development'
    PLATFORM_REVENUE_POOL = 'platform_revenue_pool'
    COMPANY_POINTS = 'company_points'
    COMPANY_BALANCE = 'company_balance'

# ==================== 资金分配比例 ====================
ALLOCATIONS: Final[dict[AllocationKey, Decimal]] = {
    AllocationKey.PUBLIC_WELFARE: Decimal('0.01'),
    AllocationKey.PLATFORM: Decimal('0.01'),
    AllocationKey.SUBSIDY_POOL: Decimal('0.12'),
    AllocationKey.HONOR_DIRECTOR: Decimal('0.02'),
    AllocationKey.COMMUNITY: Decimal('0.01'),
    AllocationKey.CITY_CENTER: Decimal('0.01'),
    AllocationKey.REGION_COMPANY: Decimal('0.005'),
    AllocationKey.DEVELOPMENT: Decimal('0.015'),
}

# ==================== 其他业务常量 ====================
MAX_POINTS_VALUE: Final[Decimal] = Decimal('0.02')
TAX_RATE: Final[Decimal] = Decimal('0.06')
POINTS_DISCOUNT_RATE: Final[Decimal] = Decimal('1.0')
COUPON_VALID_DAYS: Final[int] = 30
MAX_PURCHASE_PER_DAY: Final[int] = 2
MAX_TEAM_LAYER: Final[int] = 6

# ==================== 用户状态 ====================
class UserStatus(IntEnum):
    NORMAL = 1
    HONOR_DIRECTOR = 9

# ==================== 奖励类型 ====================
class RewardType(StrEnum):
    REFERRAL = 'referral'
    TEAM = 'team'

class RewardStatus(StrEnum):
    PENDING = 'pending'
    APPROVED = 'approved'
    REJECTED = 'rejected'

# ==================== 优惠券类型 ====================
class CouponType(StrEnum):
    USER = 'user'
    MERCHANT = 'merchant'

class CouponStatus(StrEnum):
    UNUSED = 'unused'
    USED = 'used'
    EXPIRED = 'expired'

# ==================== 提现状态 ====================
class WithdrawalStatus(StrEnum):
    PENDING_AUTO = 'pending_auto'
    PENDING_MANUAL = 'pending_manual'
    APPROVED = 'approved'
    REJECTED = 'rejected'

# ==================== 退款状态（订单系统） ====================
class RefundStatus(StrEnum):
    APPLIED = 'applied'
    SELLER_OK = 'seller_ok'
    SUCCESS = 'success'
    REJECTED = 'rejected'

# 兼容 order/config.py 的 RStatus 枚举（别名）
RStatus = RefundStatus

# ==================== 订单状态 ====================
class OrderStatus(StrEnum):
    PENDING_PAY = 'pending_pay'
    PENDING_SHIP = 'pending_ship'
    PENDING_RECV = 'pending_recv'
    COMPLETED = 'completed'
    REFUND = 'refund'
    REFUNDED = 'refunded'

# 兼容 order/config.py 的 OStatus 枚举（别名）
OStatus = OrderStatus

# ==================== 支付配置 ====================
VALID_PAY_WAYS: Final[set[str]] = {"alipay", "wechat", "card", "wx_pub", "wx_app"}

# ==================== 日志配置 ====================
LOG_DIR: Final[str] = os.path.join(os.path.dirname(__file__), 'logs')
LOG_FILE: Final[str] = os.path.join(LOG_DIR, 'finance.log')
os.makedirs(LOG_DIR, exist_ok=True)


# ==================== SQL 建表语句 ====================
CREATE_USERS = """
CREATE TABLE IF NOT EXISTS users (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    mobile VARCHAR(30) NOT NULL UNIQUE,
    password_hash CHAR(60) NOT NULL,
    name VARCHAR(100),
    member_level TINYINT NOT NULL DEFAULT 0,
    referral_id BIGINT UNSIGNED,
    referral_code VARCHAR(6) NOT NULL UNIQUE,
    member_points BIGINT NOT NULL DEFAULT 0,
    merchant_points BIGINT NOT NULL DEFAULT 0,
    withdrawable_balance BIGINT NOT NULL DEFAULT 0,
    avatar_path VARCHAR(255),
    status TINYINT NOT NULL DEFAULT 0,
    level_changed_at DATETIME NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_mobile (mobile),
    INDEX idx_member_level (member_level)
);
"""

CREATE_REFS = """
CREATE TABLE IF NOT EXISTS user_referrals (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT UNSIGNED NOT NULL,
    referrer_id BIGINT UNSIGNED,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_user (user_id),
    INDEX idx_referrer (referrer_id)
);
"""

CREATE_AUDIT = """
CREATE TABLE IF NOT EXISTS audit_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT UNSIGNED NOT NULL,
    op_type VARCHAR(30) NOT NULL,
    old_val INT,
    new_val INT,
    reason VARCHAR(255),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_dt (user_id, created_at)
);
"""

CREATE_POINTS_LOG = """
CREATE TABLE IF NOT EXISTS points_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT UNSIGNED NOT NULL,
    points_type ENUM('member', 'merchant') NOT NULL DEFAULT 'member',
    change_amount BIGINT NOT NULL,
    reason VARCHAR(255),
    related_order BIGINT UNSIGNED,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_dt (user_id, created_at)
);
"""

CREATE_ADDRESSES = """
CREATE TABLE IF NOT EXISTS addresses (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    name VARCHAR(100) NOT NULL,
    phone VARCHAR(30) NOT NULL,
    province VARCHAR(50) NOT NULL,
    city VARCHAR(50) NOT NULL,
    district VARCHAR(50) NOT NULL,
    detail VARCHAR(255) NOT NULL,
    is_default TINYINT(1) NOT NULL DEFAULT 0,
    addr_type ENUM('shipping', 'return') NOT NULL DEFAULT 'shipping',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_id (user_id),
    CONSTRAINT fk_addr_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);
"""

CREATE_TEAM_REWARDS = """
CREATE TABLE IF NOT EXISTS team_rewards (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT UNSIGNED NOT NULL,
    from_user_id BIGINT UNSIGNED NOT NULL,
    order_id BIGINT UNSIGNED,
    layer INT NOT NULL,
    reward_amount DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_dt (user_id, created_at),
    INDEX idx_order_id (order_id)
);
"""

# 荣誉董事表
CREATE_DIRECTORS = """
CREATE TABLE IF NOT EXISTS directors (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT UNSIGNED NOT NULL,
    status ENUM('pending','active','frozen') NOT NULL DEFAULT 'pending',
    dividend_amount DECIMAL(14,2) NOT NULL DEFAULT 0.00,   -- 累计已得分红
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    activated_at DATETIME NULL,
    UNIQUE KEY uk_user (user_id),
    INDEX idx_status (status)
);
"""

# 每次周分红明细
CREATE_DIRECTOR_DIVIDENDS = """
CREATE TABLE IF NOT EXISTS director_dividends (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT UNSIGNED NOT NULL,
    period_date DATE NOT NULL,          -- 分红周期（自然周）
    dividend_amount DECIMAL(14,2) NOT NULL,
    new_sales DECIMAL(14,2) NOT NULL,   -- 本周平台新业绩
    weight DECIMAL(8,4) NOT NULL,       -- 个人加权系数
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_period (user_id, period_date)
);
"""

# 为了快速判定"直推 3 个六星 + 团队 10 个六星"，在 users 表加两个派生字段
ALTER_USERS_FOR_DIRECTOR = """
ALTER TABLE users
  ADD COLUMN six_director TINYINT NOT NULL DEFAULT 0 COMMENT '直推六星人数' AFTER member_level,
  ADD COLUMN six_team     INT  NOT NULL DEFAULT 0 COMMENT '团队六星人数' AFTER six_director;
"""
