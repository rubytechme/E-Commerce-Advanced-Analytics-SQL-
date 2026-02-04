# E-Commerce-Advanced-Analytics--SQL
Advanced End-to-end SQL analytics project demonstrating window functions, CTEs, cohort analysis, predictive modeling, Customer segmentation, RFM analysis, retention cohorts, time-series forecasting, and recursive queries on e-commerce data

## Table of Contents:
1. Introduction
2. Problem Statement
3. Skills & Concepts Demonstrated
4. Main Insights & Code
5. Challenges Encountered
6. Conclusion
7. How to Use

## Introduction:
This project showcases advanced SQL analytics capabilities through a comprehensive e-commerce dataset analysis. The project simulates a real-world business intelligence scenario where data-driven insights are critical for strategic decision-making. Using PostgreSQL, I've built a complete analytics pipeline that covers customer behavior analysis, sales forecasting, cohort analysis, and product performance evaluation.
The project goes beyond basic SQL queries to demonstrate proficiency in window functions, recursive CTEs, complex aggregations, and advanced analytical techniques that are essential for senior data analyst and data engineer roles.

## Problem Statement:
E-commerce businesses face several critical challenges:

- Customer Retention: Understanding which customers are at risk of churning and identifying high-value customers
- Revenue Optimization: Analyzing product performance and identifying profit drivers
- Predictive Analytics: Forecasting customer behavior and purchase patterns
- Cohort Analysis: Tracking customer retention across different acquisition periods
- Sales Trends: Identifying seasonal patterns and growth trajectories

This project addresses these challenges by building a comprehensive SQL-based analytics framework that provides actionable insights for business stakeholders.

## Skills & Concepts Demonstrated:
### Advanced SQL Techniques:


- ✅ **Window Functions:** ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, NTILE
- ✅ **Common Table Expressions (CTEs):** Multiple CTEs, nested CTEs
- ✅ **Recursive Queries:** Category hierarchy traversal
- ✅ **Advanced Aggregations:** CUBE, ROLLUP, GROUPING
- ✅ **Subqueries:** Correlated and non-correlated subqueries
- ✅ **Complex Joins:** Multiple table joins, self-joins
- ✅ **Date/Time Functions:** Date arithmetic, time series analysis
- ✅ **Analytical Functions:** Moving averages, running totals, percentiles
- ✅ **Performance Optimization:** Strategic indexing

### Business Analytics Concepts:

1. Customer Lifetime Value (CLV) calculation
2. Cohort-based retention analysis
3. RFM (Recency, Frequency, Monetary) segmentation
4. Time series analysis with moving averages
5. Predictive purchase modeling
6. Profit margin and revenue analysis


## Main Insights & Code:

### 1️⃣ Customer Lifetime Value (CLV) Analysis
Business Value: Identifies the most valuable customers and tracks their revenue contribution over time.


```language
WITH customer_orders AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        c.customer_segment,
        o.order_id,
        o.order_date,
        SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100)) AS order_revenue
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.order_status = 'Completed'
    GROUP BY c.customer_id, c.customer_name, c.customer_segment, o.order_id, o.order_date
),
customer_clv AS (
    SELECT 
        customer_id,
        customer_name,
        customer_segment,
        order_date,
        order_revenue,
        SUM(order_revenue) OVER (
            PARTITION BY customer_id 
            ORDER BY order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_total_clv,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS order_number,
        COUNT(*) OVER (PARTITION BY customer_id) AS total_orders
    FROM customer_orders
)
SELECT 
    customer_id,
    customer_name,
    customer_segment,
    total_orders,
    ROUND(running_total_clv, 2) AS lifetime_value,
    RANK() OVER (ORDER BY running_total_clv DESC) AS clv_rank
FROM customer_clv
WHERE order_number = total_orders
ORDER BY lifetime_value DESC;
```

### Key Insights:
- Uses window functions to calculate running totals per customer
- Ranks customers by their total lifetime value
- Enables identification of VIP customers for targeted marketing


### 2️⃣ Cohort Retention Analysis:
Business Value: Tracks how well different customer cohorts are retained over time, crucial for measuring marketing effectiveness.

```language
WITH customer_cohorts AS (
    SELECT 
        customer_id,
        DATE_TRUNC('month', MIN(order_date)) AS cohort_month
    FROM orders
    WHERE order_status = 'Completed'
    GROUP BY customer_id
),
customer_activities AS (
    SELECT 
        cc.customer_id,
        cc.cohort_month,
        DATE_TRUNC('month', o.order_date) AS activity_month,
        EXTRACT(YEAR FROM AGE(o.order_date, cc.cohort_month)) * 12 + 
        EXTRACT(MONTH FROM AGE(o.order_date, cc.cohort_month)) AS months_since_cohort
    FROM customer_cohorts cc
    JOIN orders o ON cc.customer_id = o.customer_id
    WHERE o.order_status = 'Completed'
),
cohort_sizes AS (
    SELECT 
        cohort_month,
        COUNT(DISTINCT customer_id) AS cohort_size
    FROM customer_cohorts
    GROUP BY cohort_month
),
retention_data AS (
    SELECT 
        ca.cohort_month,
        ca.months_since_cohort,
        COUNT(DISTINCT ca.customer_id) AS active_customers,
        cs.cohort_size
    FROM customer_activities ca
    JOIN cohort_sizes cs ON ca.cohort_month = cs.cohort_month
    GROUP BY ca.cohort_month, ca.months_since_cohort, cs.cohort_size
)
SELECT 
    cohort_month,
    months_since_cohort,
    cohort_size,
    active_customers,
    ROUND(100.0 * active_customers / cohort_size, 2) AS retention_rate
FROM retention_data
ORDER BY cohort_month, months_since_cohort;
```

### Key Insights:
- Identifies which customer acquisition periods yielded the most loyal customers
- Calculates retention rates month-over-month for each cohort
- Helps optimize customer acquisition strategies


### 3️⃣ RFM Customer Segmentation:
Business Value: Segments customers based on purchasing behavior to enable personalized marketing campaigns.

```language
WITH customer_rfm_base AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        c.customer_segment,
        MAX(o.order_date) AS last_order_date,
        COUNT(DISTINCT o.order_id) AS frequency,
        SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100)) AS monetary_value,
        CURRENT_DATE - MAX(o.order_date) AS days_since_last_order
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.order_status = 'Completed'
    GROUP BY c.customer_id, c.customer_name, c.customer_segment
),
rfm_scores AS (
    SELECT 
        *,
        NTILE(5) OVER (ORDER BY days_since_last_order) AS recency_score,
        NTILE(5) OVER (ORDER BY frequency DESC) AS frequency_score,
        NTILE(5) OVER (ORDER BY monetary_value DESC) AS monetary_score
    FROM customer_rfm_base
),
rfm_segments AS (
    SELECT 
        *,
        recency_score + frequency_score + monetary_score AS rfm_total_score,
        CASE 
            WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
            WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Loyal Customers'
            WHEN recency_score >= 4 AND frequency_score <= 2 THEN 'New Customers'
            WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'At Risk'
            WHEN recency_score <= 2 AND frequency_score <= 2 THEN 'Lost Customers'
            WHEN monetary_score >= 4 THEN 'Big Spenders'
            ELSE 'Potential Loyalists'
        END AS rfm_segment
    FROM rfm_scores
)
SELECT 
    customer_name,
    customer_segment AS original_segment,
    rfm_segment AS rfm_calculated_segment,
    frequency AS total_orders,
    ROUND(monetary_value, 2) AS total_spent,
    days_since_last_order,
    recency_score,
    frequency_score,
    monetary_score,
    rfm_total_score
FROM rfm_segments
ORDER BY rfm_total_score DESC, monetary_value DESC;
```

### Key Insights:
- Uses NTILE to create quintile-based scoring system
- Automatically segments customers into actionable categories
- Identifies "Champions" (best customers) and "At Risk" customers who need intervention


### 4️⃣ Time Series Analysis with Moving Averages:
Business Value: Identifies sales trends and smooths out daily volatility to reveal underlying patterns.

```language
WITH daily_sales AS (
    SELECT 
        o.order_date,
        COUNT(DISTINCT o.order_id) AS orders_count,
        COUNT(DISTINCT o.customer_id) AS unique_customers,
        SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100)) AS daily_revenue,
        AVG(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100)) AS avg_order_value
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.order_status = 'Completed'
    GROUP BY o.order_date
),
sales_with_trends AS (
    SELECT 
        order_date,
        orders_count,
        unique_customers,
        ROUND(daily_revenue, 2) AS daily_revenue,
        ROUND(avg_order_value, 2) AS avg_order_value,
        ROUND(AVG(daily_revenue) OVER (
            ORDER BY order_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2) AS moving_avg_7day,
        ROUND(AVG(daily_revenue) OVER (
            ORDER BY order_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 2) AS moving_avg_30day,
        LAG(daily_revenue, 7) OVER (ORDER BY order_date) AS revenue_1week_ago,
        SUM(daily_revenue) OVER (
            PARTITION BY DATE_TRUNC('month', order_date)
            ORDER BY order_date
        ) AS month_running_total
    FROM daily_sales
)
SELECT 
    order_date,
    TO_CHAR(order_date, 'Day') AS day_of_week,
    orders_count,
    unique_customers,
    daily_revenue,
    moving_avg_7day,
    moving_avg_30day,
    ROUND(month_running_total, 2) AS month_running_total,
    CASE 
        WHEN revenue_1week_ago IS NOT NULL THEN 
            ROUND(100.0 * (daily_revenue - revenue_1week_ago) / revenue_1week_ago, 2)
        ELSE NULL
    END AS week_over_week_growth_pct
FROM sales_with_trends
ORDER BY order_date DESC;
```

### Key Insights:
- Calculates 7-day and 30-day moving averages to smooth volatility
- Tracks week-over-week growth rates
- Maintains running monthly totals for progress tracking


### 5️⃣ Predictive Purchase Modeling:
Business Value: Predicts when customers are likely to make their next purchase, enabling proactive engagement.

```language
WITH customer_order_sequence AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        o.order_id,
        o.order_date,
        LAG(o.order_date) OVER (PARTITION BY c.customer_id ORDER BY o.order_date) AS previous_order_date,
        LEAD(o.order_date) OVER (PARTITION BY c.customer_id ORDER BY o.order_date) AS next_order_date,
        ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY o.order_date) AS order_sequence_number
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    WHERE o.order_status = 'Completed'
),
purchase_intervals AS (
    SELECT 
        customer_id,
        customer_name,
        order_date,
        previous_order_date,
        CASE 
            WHEN previous_order_date IS NOT NULL 
            THEN order_date - previous_order_date 
            ELSE NULL 
        END AS days_since_last_purchase
    FROM customer_order_sequence
),
customer_purchase_stats AS (
    SELECT 
        customer_id,
        customer_name,
        COUNT(*) AS total_orders,
        ROUND(AVG(days_since_last_purchase), 0) AS avg_days_between_orders,
        ROUND(STDDEV(days_since_last_purchase), 0) AS stddev_days_between_orders,
        MAX(order_date) AS last_order_date,
        MIN(order_date) AS first_order_date
    FROM purchase_intervals
    GROUP BY customer_id, customer_name
)
SELECT 
    customer_name,
    total_orders,
    avg_days_between_orders,
    stddev_days_between_orders,
    last_order_date,
    last_order_date + CAST(avg_days_between_orders AS INT) AS predicted_next_order_date,
    CURRENT_DATE - last_order_date AS days_since_last_order,
    CASE 
        WHEN (CURRENT_DATE - last_order_date) > (avg_days_between_orders + stddev_days_between_orders) 
        THEN 'Overdue - High Churn Risk'
        WHEN (CURRENT_DATE - last_order_date) > avg_days_between_orders 
        THEN 'Due for Reorder'
        ELSE 'On Track'
    END AS customer_status
FROM customer_purchase_stats
WHERE total_orders >= 2
ORDER BY days_since_last_order DESC;
```


