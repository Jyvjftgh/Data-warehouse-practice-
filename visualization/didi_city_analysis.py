from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, current_timestamp, regexp_replace, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

# 1. 初始化 SparkSession
spark = SparkSession.builder \
    .appName("DidiCityAnalysis") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("spark.jars", "/app/spark/jars/spark-sql-kafka-0-10_2.12-3.3.0.jar") \
    .enableHiveSupport() \
    .getOrCreate()

# 设置Hive数据库
spark.sql("USE user_behavior_db")

# ✅ 检查并创建表（如果不存在）
def init_hive_table():
    """初始化 Hive 表"""
    try:
        # 检查表是否存在
        tables = spark.sql("SHOW TABLES LIKE 'ads_sale_city_2026'").collect()

        if len(tables) == 0:
            print("Table ads_sale_city_2026 does not exist, creating...")
            spark.sql("""
                CREATE TABLE ads_sale_city_2026 (
                    city STRING COMMENT '城市名称',
                    sale_count BIGINT COMMENT '销售量',
                    create_time TIMESTAMP COMMENT '创建时间'
                )
                STORED AS ORC
                TBLPROPERTIES (
                    'orc.compress'='SNAPPY',
                    'transactional'='false'
                )
            """)
            print("Table created successfully!")
        else:
            print("Table ads_sale_city_2026 already exists.")

    except Exception as e:
        print(f"Error checking/creating table: {e}")
        # 如果是权限问题，使用默认方式创建
        spark.sql("""
            CREATE TABLE IF NOT EXISTS ads_sale_city_2026 (
                city STRING,
                sale_count BIGINT,
                create_time TIMESTAMP
            )
            STORED AS ORC
        """)

# 调用初始化函数
init_hive_table()

# 2. 定义数据结构 (Schema)
schema = StructType([
    StructField("page_info", StructType([
        StructField("page_id", LongType(), True),
        StructField("page_url", StringType(), True),
        StructField("product_id", LongType(), True),
        StructField("category", StringType(), True)
    ]), True),
    StructField("behavior_info", StructType([
        StructField("user_id", LongType(), True),
        StructField("behavior_type", StringType(), True),
        StructField("action_time", StringType(), True),
        StructField("location", StringType(), True)
    ]), True),
    StructField("device_info", StructType([
        StructField("operating_system", StringType(), True),
        StructField("access_method", StringType(), True),
        StructField("browser_type", StringType(), True),
        StructField("app_version", StringType(), True)
    ]), True)
])

# 3. 读取 Kafka 数据流
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "hdp11:9092,hdp12:9092,hdp13:9092") \
    .option("subscribe", "user_behavior_topic") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", 1000) \
    .load()

# 4. 解析 JSON 数据，过滤 purchase 行为
df_parsed = df_raw.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select(
    col("data.behavior_info.location").alias("city"),
    col("data.behavior_info.behavior_type").alias("behavior_type")
).filter(col("behavior_type") == "purchase")

# 5. 实时聚合计算
df_result = df_parsed \
    .groupBy("city") \
    .agg(count("*").alias("sale_count")) \
    .withColumn("create_time", current_timestamp()) \
    .select("city", "sale_count", "create_time")

# 6. 优化的写入函数
def write_to_hive(batch_df, batch_id):
    """
    每个微批处理结束时调用，使用 ORC 格式写入 Hive
    """
    print(f"=== Processing batch ID: {batch_id} ===")

    if batch_df.isEmpty():
        print(f"Batch {batch_id} is empty, skipping write.")
        return

    # 显示批次数据
    print(f"Batch {batch_id} has {batch_df.count()} records")
    batch_df.show(5, truncate=False)

    # 清理数据（处理特殊字符）
    cleaned_df = batch_df.withColumn(
        "city",
        regexp_replace(col("city"), "[,\\n\\r\\t]", " ")
    )

    # 明确指定使用 ORC 格式写入
    try:
        print(f"Writing batch {batch_id} to Hive using ORC format...")
        cleaned_df.write \
            .mode("append") \
            .format("orc") \
            .insertInto("user_behavior_db.ads_sale_city_2026")
        print(f"Batch {batch_id} written successfully!")

    except Exception as e:
        print(f"Error writing batch {batch_id}: {str(e)}")

        # 备用方案：使用 saveAsTable
        try:
            print(f"Retrying with saveAsTable for batch {batch_id}...")
            cleaned_df.write \
                .mode("append") \
                .format("orc") \
                .saveAsTable("user_behavior_db.ads_sale_city_2026")
            print(f"Batch {batch_id} saved successfully!")

        except Exception as e2:
            print(f"SaveAsTable also failed: {str(e2)}")

            # 最终方案：保存到备份目录
            backup_path = f"/user/root/backup/ads_sale_city_2026/batch_{batch_id}"
            print(f"Backing up to {backup_path}...")
            cleaned_df.write \
                .mode("append") \
                .format("orc") \
                .save(backup_path)
            print(f"Data backed up to {backup_path}")

# 7. 启动流式查询
query = df_result.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_hive) \
    .option("checkpointLocation", "/user/root/checkpoint/DidiCityAnalysis_v3") \
    .trigger(processingTime="10 seconds") \
    .start()

print("Streaming query started successfully!")
print("Monitoring logs at: /export/data/spark_streaming.log")

# 8. 等待程序终止
query.awaitTermination()
