CREATE TABLE IF NOT EXISTS ads_sale_counts_2025 (
    product_id INT COMMENT '商品的唯一标识',
    sale_type INT COMMENT '标识销量类型。0表示畅销品；1表示滞销品',
    sale_count INT COMMENT '销量',
    category STRING COMMENT '商品所属的品类'
)
COMMENT '商品销量分析结果表'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

CREATE TABLE IF NOT EXISTS ads_category_sales_summary (
    category STRING COMMENT '商品类别',
    total_sale_count BIGINT COMMENT '该类别总销量',
    sale_ratio DECIMAL(10,4) COMMENT '销量占比',
    category_rank INT COMMENT '销量排名'
)
COMMENT '各品类销售数量及占比统计表'
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

SET mapreduce.map.memory.mb = 2048;
SET mapreduce.reduce.memory.mb = 2048;

DROP TABLE IF EXISTS tmp_product_sales;
CREATE TABLE tmp_product_sales AS
SELECT
    product_id,
    category,
    COUNT(*) AS sale_count
FROM dwd_user_behavior_detail
WHERE behavior_type = 'purchase'
GROUP BY product_id, category;

INSERT OVERWRITE TABLE ads_sale_counts_2025
SELECT
    product_id,
    CASE
        WHEN rn_desc <= 10 THEN 0
        WHEN rn_asc <= 10 THEN 1
    END AS sale_type,
    sale_count,
    category
FROM (
    SELECT
        product_id,
        sale_count,
        category,
        ROW_NUMBER() OVER (ORDER BY sale_count DESC) AS rn_desc,
        ROW_NUMBER() OVER (ORDER BY sale_count ASC) AS rn_asc
    FROM tmp_product_sales
) ranked
WHERE rn_desc <= 10 OR rn_asc <= 10;

INSERT OVERWRITE TABLE ads_category_sales_summary
SELECT
    category,
    SUM(sale_count) AS total_sale_count,
    ROUND(SUM(sale_count) / SUM(SUM(sale_count)) OVER(), 4) AS sale_ratio,
    ROW_NUMBER() OVER(ORDER BY SUM(sale_count) DESC) AS category_rank
FROM tmp_product_sales
GROUP BY category;

INSERT OVERWRITE LOCAL DIRECTORY '/app/ads_export'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT
    product_id,
    sale_type,
    sale_count,
    category
FROM ads_sale_counts_2025
ORDER BY sale_type, sale_count DESC;
