CREATE EXTERNAL TABLE IF NOT EXISTS dwd_user_behavior_detail (
    page_id INT COMMENT '页面的唯一标识',
    page_url STRING COMMENT '页面的URL地址',
    product_id INT COMMENT '商品的唯一标识',
    category STRING COMMENT '商品所属的品类',
    user_id INT COMMENT '用户的唯一标识',
    behavior_type STRING COMMENT '用户的行为',
    operating_system STRING COMMENT '操作系统',
    access_method INT COMMENT '标识访问方式，其中APP用0表示；浏览器用1表示',
    browser_type STRING COMMENT '浏览器类型',
    app_version STRING COMMENT 'App的版本号',
    province STRING COMMENT '省份',
    city STRING COMMENT '城市',
    action_date_key STRING COMMENT '日期键，用于关联dim_date表',
    action_time_key STRING COMMENT '时间键，用于关联dim_time表'
)
COMMENT '用户行为明细表(DWD层)'
PARTITIONED BY (
    yearinfo STRING COMMENT '年份分区',
    monthinfo STRING COMMENT '月份分区',
    dayinfo STRING COMMENT '日期分区'
)
STORED AS ORC
LOCATION '/warehouse/user_behavior_db/dwd/dwd_user_behavior_detail'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY'
);

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 10000;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET hive.exec.mode.local.auto=false;

INSERT OVERWRITE TABLE dwd_user_behavior_detail PARTITION (yearinfo, monthinfo, dayinfo)
SELECT
    page_info.page_id,
    page_info.page_url,
    page_info.product_id,
    page_info.category,
    behavior_info.user_id,
    behavior_info.behavior_type,
    device_info.operating_system,
    CASE
        WHEN device_info.access_method = 'app' THEN 0
        WHEN device_info.access_method = 'browser' THEN 1
        ELSE -1
    END AS access_method,
    device_info.browser_type,
    device_info.app_version,
    split(behavior_info.location, ', ')[0] AS province,
    split(behavior_info.location, ', ')[1] AS city,
    regexp_replace(split(behavior_info.action_time, ' ')[0], '-', '') AS action_date_key,
    regexp_replace(split(behavior_info.action_time, ' ')[1], ':', '') AS action_time_key,
    split(split(behavior_info.action_time, ' ')[0], '-')[0] AS yearinfo,
    split(split(behavior_info.action_time, ' ')[0], '-')[1] AS monthinfo,
    split(split(behavior_info.action_time, ' ')[0], '-')[2] AS dayinfo
FROM ods_user_behavior;
