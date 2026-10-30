# -*- coding: utf-8 -*-
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')

# 读取数据
df = pd.read_csv('D:/闲置/ads_category_sales_summary.csv')

# 创建画布
fig, ax = plt.subplots(figsize=(10, 8))

# 颜色配置
colors = sns.color_palette("viridis", n_colors=len(df))

# 绘制饼图
wedges, texts, autotexts = ax.pie(
    df['total_sale_count'],
    labels=df['category'],
    autopct='%1.1f%%',
    colors=colors,
    shadow=True,
    startangle=90,
    textprops={'fontsize': 12}
)

# 设置百分比文字样式
for autotext in autotexts:
    autotext.set_color('white')
    autotext.set_fontweight('bold')
    autotext.set_fontsize(11)

# 设置标题
ax.set_title('Sales Distribution by Category', fontsize=16, fontweight='bold')

# 自动调整布局
plt.tight_layout()

# 保存图表
output_path = 'D:/bigdata/category_pie_chart_simple.png'
plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
print(f"\n 图表已保存到: {output_path}")

plt.show()
