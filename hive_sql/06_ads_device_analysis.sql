CREATE TABLE IF NOT EXISTS ads_device_counts_2025 (
    hour_interval STRING COMMENT '时间段，如 00:00~01:00',
    device_type   STRING COMMENT 'mobile 或 desktop',
    access_count  INT    COMMENT '访问量'
)
COMMENT '设备分析结果表 - 按小时段统计移动端与桌面端访问量'
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE LOCAL DIRECTORY '/export/data/ads_device_export/'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT
    hour_interval,
    device_type,
    access_count
FROM user_behavior_db.ads_device_counts_2025
ORDER BY hour_interval, device_type;
