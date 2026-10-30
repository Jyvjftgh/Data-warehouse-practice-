import matplotlib.pyplot as plt
import pandas as pd

# 1. 读取数据 (请确保路径指向你刚才生成的 final_traffic.csv)
# 如果你的文件在 Windows 本地，路径可能是 'C:/Users/.../final_traffic.csv'
# 如果在 Linux 服务器当前目录，直接用 './final_traffic.csv'
file_path = r'C:\Users\ASUS\shixun/traffic_hourly.csv'

try:
    df = pd.read_csv(file_path)
except FileNotFoundError:
    print(f"错误：找不到文件 {file_path}，请检查路径！")
    exit()

# 2. 设置中文字体 (防止中文乱码)
# Windows 系统通常用 SimHei，Mac/Linux 可能需要换成 Arial Unicode MS 或 Heiti TC
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False # 解决负号显示问题

# 3. 创建画布和主坐标轴
fig, ax1 = plt.subplots(figsize=(12, 6))

# --- 绘制 PV (左轴) ---
color_pv = '#1f77b4'  # 蓝色
ax1.set_xlabel('时间 (小时)', fontsize=12)
ax1.set_ylabel('PV (页面浏览量)', color=color_pv, fontsize=12)
line1 = ax1.plot(df['hour'], df['pv'], color=color_pv, marker='o', label='PV', linewidth=2)
ax1.tick_params(axis='y', labelcolor=color_pv)
ax1.grid(True, linestyle='--', alpha=0.5) # 添加网格

# --- 绘制 UV (右轴) ---
ax2 = ax1.twinx()  # 实例化共享 x 轴的第二个 y 轴
color_uv = '#ff7f0e'  # 橙色
ax2.set_ylabel('UV (独立访客数)', color=color_uv, fontsize=12)
line2 = ax2.plot(df['hour'], df['uv'], color=color_uv, marker='s', linestyle='--', label='UV', linewidth=2)
ax2.tick_params(axis='y', labelcolor=color_uv)

# 4. 添加标题和图例
plt.title('24小时用户活跃度分析 (PV vs UV)', fontsize=16, pad=20)

# 合并两个轴的图例
lines = line1 + line2
labels = [l.get_label() for l in lines]
ax1.legend(lines, labels, loc='upper left', fontsize=12)

# 5. 调整 X 轴刻度 (确保显示 0‑23)
plt.xticks(range(0, 24))

# 6. 自动调整布局并保存/显示
plt.tight_layout()
plt.savefig('traffic_analysis.png', dpi=300) # 保存为高清图片
print("✅ 图片已保存为 traffic_analysis.png")
plt.show()
