#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
真正的实时数据可视化 - 直接从Kafka读取
"""
import sys
import os
import json
import time
import threading
import logging
from datetime import datetime
from collections import defaultdict
from queue import Queue

from flask import Flask, jsonify, render_template_string
from flask_cors import CORS
from flask_socketio import SocketIO, emit

# 设置环境变量
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import from_json, col, count, current_timestamp
    from pyspark.sql.types import StructType, StructField, StringType, LongType
except ImportError as e:
    print(f"❌ PySpark import error: {e}")
    sys.exit(1)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# 实时数据存储 - 使用字典存储每个城市的实时计数
realtime_data = {
    'cities': defaultdict(int),  # 城市 -> 实时销量
    'last_update': '',
    'total_sales': 0
}

# 数据锁
data_lock = threading.Lock()

def init_spark_streaming():
    """初始化Spark Streaming"""
    try:
        spark = SparkSession.builder \
            .appName("RealtimeVisualization") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("hive.metastore.uris", "thrift://hdp11:9083") \
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
            .enableHiveSupport() \
            .getOrCreate()

        # 定义Kafka数据schema
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

        # 读取Kafka流
        df_raw = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "hdp11:9092,hdp12:9092,hdp13:9092") \
            .option("subscribe", "user_behavior_topic") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", 100) \
            .load()

        # 解析JSON并过滤purchase行为
        df_parsed = df_raw.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select(
            col("data.behavior_info.location").alias("city"),
            col("data.behavior_info.behavior_type").alias("behavior_type")
        ).filter(col("behavior_type") == "purchase")

        # 实时聚合
        df_result = df_parsed \
            .groupBy("city") \
            .agg(count("*").alias("sale_count")) \
            .withColumn("create_time", current_timestamp())

        return spark, df_result

    except Exception as e:
        logger.error(f"Failed to initialize Spark Streaming: {e}")
        raise

def process_stream_batch(batch_df, batch_id):
    """处理每个微批次"""
    global realtime_data

    if batch_df.isEmpty():
        return

    try:
        # 收集批次数据
        rows = batch_df.collect()

        with data_lock:
            for row in rows:
                city = row['city']
                count = row['sale_count']
                realtime_data['cities'][city] += count

            # 更新总销量
            realtime_data['total_sales'] = sum(realtime_data['cities'].values())
            realtime_data['last_update'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # 获取Top城市
            top_cities = sorted(realtime_data['cities'].items(), key=lambda x: x[1], reverse=True)[:30]

            # 通过WebSocket推送实时数据
            socketio.emit('realtime_update', {
                'cities': [c[0] for c in top_cities],
                'sales': [c[1] for c in top_cities],
                'timestamp': realtime_data['last_update'],
                'total_sales': realtime_data['total_sales']
            })

            logger.info(f"✅ Batch {batch_id}: Updated {len(rows)} cities, Total: {realtime_data['total_sales']}")

    except Exception as e:
        logger.error(f"Error processing batch: {e}")

def start_streaming():
    """启动流处理"""
    try:
        spark, df_result = init_spark_streaming()

        # 启动流查询
        query = df_result.writeStream \
            .outputMode("update") \
            .foreachBatch(process_stream_batch) \
            .option("checkpointLocation", "/user/root/checkpoint/RealtimeVisualization") \
            .trigger(processingTime="3 seconds") \
            .start()

        logger.info("✅ Streaming query started")
        return query

    except Exception as e:
        logger.error(f"Failed to start streaming: {e}")
        raise

# 启动流处理（在后台线程中）
streaming_query = None

def start_streaming_thread():
    global streaming_query
    try:
        streaming_query = start_streaming()
        streaming_query.awaitTermination()
    except Exception as e:
        logger.error(f"Streaming thread error: {e}")

# 启动流处理线程
streaming_thread = threading.Thread(target=start_streaming_thread, daemon=True)
streaming_thread.start()
logger.info("✅ Streaming thread started")

# HTML模板
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>实时数据可视化</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <script src="https://cdn.socket.io/4.5.4/socket.io.min.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Microsoft YaHei', sans-serif;
            background: linear-gradient(135deg, #0a0e27 0%, #1a1f3d 100%);
            color: #fff;
            min-height: 100vh;
            padding: 20px;
        }
        .container { max-width: 1200px; margin: 0 auto; }
        h1 {
            text-align: center;
            font-size: 32px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            padding: 20px 0 10px;
        }
        .subtitle {
            text-align: center;
            opacity: 0.7;
            margin-bottom: 30px;
            font-size: 14px;
        }
        .stats {
            display: grid;
            grid-template-columns: repeat(5, 1fr);
            gap: 15px;
            margin-bottom: 30px;
        }
        .stat-box {
            background: rgba(255,255,255,0.05);
            border: 1px solid rgba(255,255,255,0.1);
            border-radius: 10px;
            padding: 20px;
            text-align: center;
            transition: all 0.3s;
            animation: pulse 2s ease-in-out infinite;
        }
        .stat-box .number {
            font-size: 28px;
            font-weight: bold;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        .stat-box .label {
            font-size: 14px;
            opacity: 0.7;
            margin-top: 5px;
        }
        .stat-box .highlight {
            color: #43e97b;
            font-size: 12px;
            opacity: 0.9;
            margin-top: 5px;
        }
        .cities-section {
            background: rgba(255,255,255,0.05);
            border: 1px solid rgba(255,255,255,0.1);
            border-radius: 10px;
            padding: 20px;
            max-height: 600px;
            overflow-y: auto;
        }
        .cities-section h2 {
            margin-bottom: 15px;
            color: #a8b5d9;
            font-size: 18px;
        }
        .city-item {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 10px 15px;
            border-bottom: 1px solid rgba(255,255,255,0.05);
            transition: all 0.3s;
            animation: slideIn 0.5s ease;
        }
        .city-item .left {
            display: flex;
            align-items: center;
            flex: 1;
        }
        .city-item .rank {
            color: #667eea;
            font-weight: bold;
            margin-right: 15px;
            min-width: 30px;
        }
        .city-item .name {
            flex: 1;
        }
        .city-item .sales {
            color: #43e97b;
            font-weight: bold;
            min-width: 80px;
            text-align: right;
        }
        .city-item .bar {
            width: 120px;
            height: 6px;
            background: rgba(255,255,255,0.1);
            border-radius: 3px;
            margin-left: 15px;
        }
        .city-item .bar-fill {
            height: 100%;
            background: linear-gradient(90deg, #667eea, #764ba2);
            border-radius: 3px;
            transition: width 0.5s ease;
        }
        .update-time {
            text-align: center;
            opacity: 0.5;
            font-size: 12px;
            margin-top: 20px;
        }
        .badge {
            display: inline-block;
            background: #ff6b6b;
            color: #fff;
            padding: 2px 10px;
            border-radius: 12px;
            font-size: 11px;
            font-weight: bold;
            margin-left: 10px;
            animation: blink 1s infinite;
        }
        .no-data {
            text-align: center;
            padding: 40px;
            opacity: 0.6;
        }
        @keyframes slideIn {
            from { opacity: 0; transform: translateX(-20px); }
            to { opacity: 1; transform: translateX(0); }
        }
        @keyframes pulse {
            0%, 100% { transform: scale(1); }
            50% { transform: scale(1.02); }
        }
        @keyframes blink {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.3; }
        }
        @media (max-width: 768px) {
            .stats { grid-template-columns: repeat(2, 1fr); }
            .city-item { flex-wrap: wrap; gap: 5px; }
            .city-item .bar { width: 100%; margin-left: 45px; }
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>⚡ 实时销售数据看板</h1>
        <div class="subtitle" id="status">🔄 连接中...</div>

        <div class="stats">
            <div class="stat-box">
                <div class="number" id="cities">0</div>
                <div class="label">🏙️ 实时城市</div>
                <div class="highlight">⬆ 持续更新</div>
            </div>
            <div class="stat-box">
                <div class="number" id="total">0</div>
                <div class="label">💰 实时总销量</div>
                <div class="highlight">⬆ 持续更新</div>
            </div>
            <div class="stat-box">
                <div class="number" id="topCity">-</div>
                <div class="label">🏆 销冠城市</div>
                <div class="highlight" id="topCitySales">-</div>
            </div>
            <div class="stat-box">
                <div class="number" id="speed">0</div>
                <div class="label">📊 每秒销量</div>
                <div class="highlight">实时速率</div>
            </div>
            <div class="stat-box">
                <div class="number" id="updates">0</div>
                <div class="label">🔄 更新次数</div>
                <div class="highlight">实时推送</div>
            </div>
        </div>

        <div class="cities-section">
            <h2>📊 实时销量排行榜 <span class="badge">LIVE</span></h2>
            <div id="citiesList"><div class="no-data">等待实时数据...</div></div>
        </div>

        <div class="update-time" id="updateTime">等待数据...</div>
    </div>

    <script>
        const socket = io();
        let updateCount = 0;
        let lastSales = 0;
        let lastTime = Date.now();

        // 连接成功
        socket.on('connect', function() {
            document.getElementById('status').textContent = '✅ 实时连接已建立';
            document.getElementById('status').style.color = '#43e97b';
        });

        // 断开连接
        socket.on('disconnect', function() {
            document.getElementById('status').textContent = '❌ 连接断开，等待重连...';
            document.getElementById('status').style.color = '#ff6b6b';
        });

        // 接收实时数据
        socket.on('realtime_update', function(data) {
            updateCount++;
            document.getElementById('updates').textContent = updateCount;

            // 更新总销量
            document.getElementById('total').textContent = data.total_sales.toLocaleString();
            document.getElementById('cities').textContent = data.cities.length;

            // 更新销冠
            if (data.cities.length > 0) {
                document.getElementById('topCity').textContent = data.cities[0];
                document.getElementById('topCitySales').textContent = data.sales[0].toLocaleString() + ' 单';
            }

            // 计算速率
            const now = Date.now();
            const elapsed = (now - lastTime) / 1000;
            if (elapsed > 0) {
                const currentTotal = data.total_sales;
                const speed = Math.round((currentTotal - lastSales) / elapsed);
                document.getElementById('speed').textContent = speed;
            }
            lastSales = data.total_sales;
            lastTime = now;

            // 更新城市列表
            if (data.cities && data.cities.length > 0) {
                let html = '';
                const maxSales = Math.max(...data.sales);

                data.cities.forEach((city, i) => {
                    const percentage = maxSales > 0 ? (data.sales[i] / maxSales * 100) : 0;
                    const rank = i + 1;
                    const medal = rank === 1 ? '🥇' : rank === 2 ? '🥈' : rank === 3 ? '🥉' : `#${rank}`;

                    html += `
                        <div class="city-item">
                            <div class="left">
                                <span class="rank">${medal}</span>
                                <span class="name">${city}</span>
                            </div>
                            <span class="sales">${data.sales[i].toLocaleString()} 单</span>
                            <div class="bar">
                                <div class="bar-fill" style="width: ${percentage}%;"></div>
                            </div>
                        </div>
                    `;
                });

                document.getElementById('citiesList').innerHTML = html;
                document.getElementById('updateTime').textContent = '🔄 最后更新: ' + data.timestamp + ' (实时)';
                document.getElementById('status').textContent = '✅ 实时数据流 (每3秒更新)';
            }
        });
    </script>
</body>
</html>
'''

@app.route('/')
def index():
    """主页"""
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/status')
def get_status():
    """获取状态"""
    with data_lock:
        return jsonify({
            'status': 'running',
            'last_update': realtime_data['last_update'],
            'total_sales': realtime_data['total_sales'],
            'cities_count': len(realtime_data['cities'])
        })

if __name__ == '__main__':
    logger.info("=" * 60)
    logger.info("⚡ Starting Realtime Visualization Server")
    logger.info(f"📊 Access URL: http://hdp11:5000")
    logger.info("💡 Data is streamed directly from Kafka in real‑time")
    logger.info("=" * 60)

    socketio.run(app, host='0.0.0.0', port=5000, debug=False, allow_unsafe_werkzeug=True)
