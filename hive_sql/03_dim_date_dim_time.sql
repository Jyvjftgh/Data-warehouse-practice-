CREATE EXTERNAL TABLE IF NOT EXISTS dim_date (
    date_key STRING COMMENT '日期的唯一标识，如20250101',
    date_value DATE COMMENT '日期，如2025-01-01',
    day_of_week STRING COMMENT '星期（英文）',
    month INT COMMENT '月份',
    year INT COMMENT '年份',
    day_of_year INT COMMENT '年份的第几天',
    day_of_month INT COMMENT '月份的第几天',
    quarter INT COMMENT '季度'
)
COMMENT '日期维度表'
STORED AS ORC
LOCATION '/warehouse/user_behavior_db/dim/dim_date'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY'
);

CREATE EXTERNAL TABLE IF NOT EXISTS dim_time (
    time_key STRING COMMENT '时间的唯一标识，如000001',
    time_value STRING COMMENT '时间，如00:00:01',
    hours24 INT COMMENT '小时(24小时制)',
    minutes INT COMMENT '分钟',
    seconds INT COMMENT '秒',
    am_pm STRING COMMENT '上午(AM)或下午(PM)'
)
COMMENT '时间维度表'
STORED AS ORC
LOCATION '/warehouse/user_behavior_db/dim/dim_time'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY'
);

INSERT INTO TABLE dim_date
SELECT
    date_format(dt, 'yyyyMMdd') AS date_key,
    dt AS date_value,
    date_format(dt, 'EEEE') AS day_of_week,
    month(dt) AS month,
    year(dt) AS year,
    date_format(dt, 'D') AS day_of_year,
    day(dt) AS day_of_month,
    quarter(dt) AS quarter
FROM (
    SELECT date_add('2025-01-01', t1.pos + t2.pos * 10 + t3.pos * 100) AS dt
    FROM
        (SELECT 0 AS pos UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t1,
        (SELECT 0 AS pos UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t2,
        (SELECT 0 AS pos UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t3
    WHERE date_add('2025-01-01', t1.pos + t2.pos * 10 + t3.pos * 100) <= '2025-12-31'
) temp;

INSERT OVERWRITE TABLE dim_time
SELECT
    lpad(cast(hours24 as string), 2, '0') ||
    lpad(cast(minutes as string), 2, '0') ||
    lpad(cast(seconds as string), 2, '0') AS time_key,
    lpad(cast(hours24 as string), 2, '0') || ':' ||
    lpad(cast(minutes as string), 2, '0') || ':' ||
    lpad(cast(seconds as string), 2, '0') AS time_value,
    hours24,
    minutes,
    seconds,
    CASE WHEN hours24 < 12 THEN 'AM' ELSE 'PM' END AS am_pm
FROM (
    SELECT posexplode(split(space(23), ' ')) AS (pos, hours24)
) h
CROSS JOIN (
    SELECT posexplode(split(space(59), ' ')) AS (pos, minutes)
) m
CROSS JOIN (
    SELECT posexplode(split(space(59), ' ')) AS (pos, seconds)
) s;
