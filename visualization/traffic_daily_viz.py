import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# ==========================================
# 1. 设置中文字体 (防止中文乱码)
# ==========================================
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

# ==========================================
# 2. 读取数据
# ==========================================
# ⚠️ 请确保文件名和你导出的一致，比如 daily_traffic.csv
file_path = 'traffic_daily.csv'

try:
    # 读取 CSV
    df = pd.read_csv(file_path)

    # 【关键步骤】将 'dt' 列转换为真正的日期格式
    # 这样 Matplotlib 才能识别这是时间轴，而不是普通的字符串
    df['dt'] = pd.to_datetime(df['dt'])

    # 按日期排序，防止画图时线条乱跳
    df = df.sort_values('dt')

    # ==========================================
    # 3. 开始绘图
    # ==========================================
    fig, ax1 = plt.subplots(figsize=(14, 6)) # 设置画布大小：宽14，高6

    # --- 绘制 PV (左轴) ---
    color_pv = '#2E86C1' # 蓝色
    ax1.set_xlabel('日期', fontsize=12)
    ax1.set_ylabel('PV (页面浏览量)', color=color_pv, fontsize=12)
    line1 = ax1.plot(df['dt'], df['pv'], color=color_pv, marker='o', markersize=4, label='PV', linewidth=2)
    ax1.tick_params(axis='y', labelcolor=color_pv)

    # --- 绘制 UV (右轴) ---
    # 使用 twinx() 创建一个共享 X 轴的右侧 Y 轴
    ax2 = ax1.twinx()
    color_uv = '#E67E22' # 橙色
    ax2.set_ylabel('UV (独立访客数)', color=color_uv, fontsize=12)
    line2 = ax2.plot(df['dt'], df['uv'], color=color_uv, marker='s', markersize=4, label='UV', linestyle='--', linewidth=2)
    ax2.tick_params(axis='y', labelcolor=color_uv)

    # ==========================================
    # 4. 优化 X 轴日期显示 (核心优化)
    # ==========================================
    # 自动格式化日期，避免重叠
    # 如果天数少，显示 '日'；如果天数多，显示 '月‑日'
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m‑%d'))

    # 自动旋转日期标签，防止重叠
    fig.autofmt_xdate(rotation=45)

    # ==========================================
    # 5. 添加标题和网格
    # ==========================================
    plt.title('宏观趋势：每日用户活跃度分析 (PV vs UV)', fontsize=16, pad=20)

    # 添加网格线 (只画 Y 轴方向的虚线，看起来更干净)
    ax1.grid(True, which='major', axis='y', linestyle='--', alpha=0.7)

    # 合并两个轴的图例
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc='upper left', fontsize=12)

    plt.tight_layout() # 自动调整布局，防止标签被切掉
    plt.show()

    print("✅ 图表绘制成功！")

except FileNotFoundError:
    print(f"❌ 错误：找不到文件 '{file_path}'。请检查文件名或路径是否正确。")
except Exception as e:
    print(f"❌ 发生错误：{e}")
