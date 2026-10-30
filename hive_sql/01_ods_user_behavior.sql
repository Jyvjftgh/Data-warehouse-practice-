CREATE EXTERNAL TABLE ods_user_behavior (
    page_info STRUCT<
        page_id: INT,
        page_url: STRING,
        product_id: INT,
        category: STRING
    > COMMENT '页面信息',
    behavior_info STRUCT<
        user_id: INT,
        behavior_type: STRING,
        action_time: STRING,
        location: STRING
    > COMMENT '行为信息',
    device_info STRUCT<
        operating_system: STRING,
        access_method: STRING,
        browser_type: STRING,
        app_version: STRING
    > COMMENT '设备信息'
)
COMMENT '用户行为ODS层表'
PARTITIONED BY (dt STRING)
STORED AS TEXTFILE
LOCATION '/warehouse/user_behavior_db/ods/ods_user_behavior';

CREATE TABLE ods_user_behavior_tmp (line STRING);

LOAD DATA LOCAL INPATH '/export/data/log/2025/user_behaviors.log' INTO TABLE ods_user_behavior_tmp;

SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE ods_user_behavior PARTITION (dt)
SELECT
    named_struct(
        'page_id', CAST(get_json_object(line, '$.page_info.page_id') AS INT),
        'page_url', get_json_object(line, '$.page_info.page_url'),
        'product_id', CAST(get_json_object(line, '$.page_info.product_id') AS INT),
        'category', get_json_object(line, '$.page_info.category')
    ),
    named_struct(
        'user_id', CAST(get_json_object(line, '$.behavior_info.user_id') AS INT),
        'behavior_type', get_json_object(line, '$.behavior_info.behavior_type'),
        'action_time', get_json_object(line, '$.behavior_info.action_time'),
        'location', get_json_object(line, '$.behavior_info.location')
    ),
    named_struct(
        'operating_system', get_json_object(line, '$.device_info.operating_system'),
        'access_method', get_json_object(line, '$.device_info.access_method'),
        'browser_type', get_json_object(line, '$.device_info.browser_type'),
        'app_version', get_json_object(line, '$.device_info.app_version')
    ),
    substr(get_json_object(line, '$.behavior_info.action_time'), 1, 10) as dt
FROM ods_user_behavior_tmp;
