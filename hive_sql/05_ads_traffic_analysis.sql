CREATE TABLE IF NOT EXISTS user_behavior_db.ads_traffic_hourly (
    stat_date STRING COMMENT '统计日期(yyyy-MM-dd)',
    stat_hour STRING COMMENT '统计小时(HH)',
    pv_count BIGINT COMMENT '页面浏览量(PV)',
    uv_count BIGINT COMMENT '独立访客数(UV)'
)
COMMENT '24小时流量趋势统计表'
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE user_behavior_db.ads_traffic_hourly
SELECT
    CONCAT(yearinfo, '-', monthinfo, '-', dayinfo) AS stat_date,
    substr(action_time_key, 1, 2) AS stat_hour,
    COUNT(*) AS pv_count,
    COUNT(DISTINCT user_id) AS uv_count
FROM user_behavior_db.dwd_user_behavior_detail
WHERE yearinfo IS NOT NULL AND action_time_key IS NOT NULL
GROUP BY
    yearinfo,
    monthinfo,
    dayinfo,
    substr(action_time_key, 1, 2);

CREATE TABLE IF NOT EXISTS ads_traffic_daily
(
    stat_date STRING,
    pv_count BIGINT,
    uv_count BIGINT,
    avg_pv_per_user DOUBLE
);

INSERT OVERWRITE TABLE ads_traffic_daily
SELECT
    CONCAT(yearinfo, '-', monthinfo, '-', dayinfo) AS stat_date,
    COUNT(*) AS pv_count,
    COUNT(DISTINCT user_id) uv_count,
    COUNT(*)/COUNT(DISTINCT user_id) avg_pv_per_user
FROM user_behavior_db.dwd_user_behavior_detail
WHERE yearinfo IS NOT NULL
GROUP BY
    yearinfo,
    monthinfo,
    dayinfo
ORDER BY stat_date ASC;

INSERT OVERWRITE LOCAL DIRECTORY '/opt/data/export/ads_traffic_daily/'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    stat_date,
    uv_count,
    pv_count,
    avg_pv_per_user
FROM user_behavior_db.ads_traffic_daily;
