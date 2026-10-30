# -*- coding: utf-8 -*-
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')

# 1. 设置绘图样式
sns.set_style("whitegrid")

# 2. 读取数据
df = pd.read_csv('D:/闲置/ads_sale_counts_2025.csv')

print("=" * 60)
print("Data Preview")
print("=" * 60)
print(df.head())

# 添加英文标签列，用于后续图例显示
df['sale_type_name'] = df['sale_type'].map({0: 'Hot-selling', 1: 'Slow-moving'})

# 3. 创建可视化图表
fig = plt.figure(figsize=(16, 12))

# ---------- 图1：热销商品条形图 ----------
ax1 = plt.subplot(2, 2, 1)
# 筛选出热销商品并按销量升序排列，为水平条形图做准备
top_sales = df[df['sale_type'] == 0].sort_values('sale_count', ascending=True)
# 使用蓝色系调色板
colors_top = sns.color_palette("Blues_r", n_colors=len(top_sales))
# 绘制水平条形图
bars1 = ax1.barh(top_sales['product_id'].astype(str), top_sales['sale_count'],
                 color=colors_top, edgecolor='navy', linewidth=1.5)
# 设置图表标题和坐标轴标签
ax1.set_title('Top 10 Hot-selling Products', fontsize=14, fontweight='bold')
ax1.set_xlabel('Sales Count', fontsize=12)
ax1.set_ylabel('Product ID', fontsize=12)
# 反转y轴，使销量最高的在最上面
ax1.invert_yaxis()
# 在每个条形图旁边添加销量数值和品类信息
for i, (idx, row) in enumerate(top_sales.iterrows()):
    ax1.text(row['sale_count'] + 0.3, i, f"{row['sale_count']}",
             va='center', fontsize=10, fontweight='bold')
    ax1.text(row['sale_count'] + 1.5, i, f"({row['category']})",
             va='center', fontsize=8, color='gray')

# ---------- 图2：滞销商品条形图 ----------
ax2 = plt.subplot(2, 2, 2)
# 筛选出滞销商品并按销量升序排列
bottom_sales = df[df['sale_type'] == 1].sort_values('sale_count', ascending=True)
if not bottom_sales.empty:
    # 使用红色系调色板
    colors_bottom = sns.color_palette("Reds_r", n_colors=len(bottom_sales))
    # 绘制水平条形图
    bars2 = ax2.barh(bottom_sales['product_id'].astype(str), bottom_sales['sale_count'],
                     color=colors_bottom, edgecolor='darkred', linewidth=1.5)
    # 设置图表标题和坐标轴标签
    ax2.set_title(' Top 10 Slow-moving Products', fontsize=14, fontweight='bold')
    ax2.set_xlabel('Sales Count', fontsize=12)
    ax2.set_ylabel('Product ID', fontsize=12)
    # 反转y轴
    ax2.invert_yaxis()
    # 在每个条形图旁边添加销量数值和品类信息
    for i, (idx, row) in enumerate(bottom_sales.iterrows()):
        ax2.text(row['sale_count'] + 0.3, i, f"{row['sale_count']}",
                 va='center', fontsize=10, fontweight='bold')
        ax2.text(row['sale_count'] + 1.5, i, f"({row['category']})",
                 va='center', fontsize=8, color='gray')
else:
    # 如果没有滞销商品数据，显示提示信息
    ax2.text(0.5, 0.5, 'No slow‑moving product data', ha='center', va='center',
             fontsize=14, transform=ax2.transAxes, style='italic')

# 调整子图之间的间距，防止重叠
plt.tight_layout()

# 保存图表到指定路径
output_path = 'D:/bigdata/ads_visualization_report_en.png'
plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
print(f"\n Chart saved to: {output_path}")

# 显示图表
plt.show()
