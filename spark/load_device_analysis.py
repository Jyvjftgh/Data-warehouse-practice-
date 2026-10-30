from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("LoadDeviceAnalysis") \
    .config("hive.metastore.uris", "thrift://hdp11:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

dwd_count = spark.sql(
    "SELECT COUNT(*) FROM user_behavior_db.dwd_user_behavior_detail"
).collect()[0][0]
print(f"DWD 层总行数：{dwd_count:,}")

spark.sql("""
    INSERT OVERWRITE TABLE user_behavior_db.ads_device_counts_2025
    SELECT
        CONCAT(
            LPAD(CAST(hour_val AS STRING), 2, '0'), ':00~',
            LPAD(CAST((hour_val + 1) % 24 AS STRING), 2, '0'), ':00'
        )                                                   AS hour_interval,
        CASE
            WHEN access_method = 0 THEN 'mobile'
            WHEN access_method = 1 THEN 'desktop'
            ELSE 'unknown'
        END                                                 AS device_type,
        COUNT(*)                                            AS access_count
    FROM (
        SELECT
            CAST(SUBSTR(action_time_key, 1, 2) AS INT)      AS hour_val,
            access_method
        FROM user_behavior_db.dwd_user_behavior_detail
        WHERE action_time_key IS NOT NULL
          AND access_method IN (0, 1)
    ) t
    GROUP BY hour_val, access_method
    ORDER BY hour_val, access_method
""")

spark.sql("""
    SELECT hour_interval, device_type, access_count
    FROM user_behavior_db.ads_device_counts_2025
    ORDER BY hour_interval, device_type
""").show(48, truncate=False)

spark.stop()
