-- Monthly rain-point coupon policy. Run with scripts/migrate.py before enabling the scheduler.
SET @rain_column_ddl = IF(
    EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'coupons' AND column_name = 'rain_settlement_id'),
    'SELECT 1',
    'ALTER TABLE coupons ADD COLUMN rain_settlement_id BIGINT UNSIGNED DEFAULT NULL COMMENT ''Monthly rain-point settlement ID'''
);
PREPARE rain_column_stmt FROM @rain_column_ddl;
EXECUTE rain_column_stmt;
DEALLOCATE PREPARE rain_column_stmt;

CREATE TABLE IF NOT EXISTS rain_point_monthly_settlements (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT UNSIGNED NOT NULL,
    settlement_period CHAR(7) NOT NULL COMMENT 'Settlement month, YYYY-MM',
    conversion_mode ENUM('excess','monthly') NOT NULL,
    rain_points_before DECIMAL(14,6) NOT NULL,
    converted_amount DECIMAL(14,6) NOT NULL DEFAULT 0,
    coupon_id BIGINT UNSIGNED DEFAULT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_user_period (user_id, settlement_period),
    INDEX idx_period (settlement_period),
    INDEX idx_coupon (coupon_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
