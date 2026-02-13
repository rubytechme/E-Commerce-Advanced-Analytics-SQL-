-- ============================================================================
-- E-COMMERCE ADVANCED ANALYTICS PROJECT
-- Author: Ruby
-- Database: PostgreSQL
-- Purpose: Comprehensive sales and customer behavior analysis
-- ============================================================================

-- ============================================================================
-- SECTION 1: DATABASE SETUP & SAMPLE DATA CREATION
-- ============================================================================

-- Create tables for e-commerce analytics
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    registration_date DATE,
    country VARCHAR(50),
    customer_segment VARCHAR(20) -- 'Premium', 'Standard', 'Basic'
);

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(200),
    category VARCHAR(50),
    subcategory VARCHAR(50),
    unit_price DECIMAL(10,2),
    cost_price DECIMAL(10,2)
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    order_date DATE,
    ship_date DATE,
    order_status VARCHAR(20) -- 'Completed', 'Cancelled', 'Returned'
);

CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES orders(order_id),
    product_id INT REFERENCES products(product_id),
    quantity INT,
    unit_price DECIMAL(10,2),
    discount_percent DECIMAL(5,2)
);

-- Insert sample data (simplified for demonstration)
INSERT INTO customers (customer_name, email, registration_date, country, customer_segment) VALUES
('John Smith', 'john@email.com', '2023-01-15', 'USA', 'Premium'),
('Emma Wilson', 'emma@email.com', '2023-02-20', 'UK', 'Standard'),
('Carlos Rodriguez', 'carlos@email.com', '2023-03-10', 'Spain', 'Basic'),
('Yuki Tanaka', 'yuki@email.com', '2023-01-25', 'Japan', 'Premium'),
('Sarah Johnson', 'sarah@email.com', '2023-04-05', 'USA', 'Standard');

INSERT INTO products (product_name, category, subcategory, unit_price, cost_price) VALUES
('Laptop Pro 15', 'Electronics', 'Computers', 1299.99, 800.00),
('Wireless Mouse', 'Electronics', 'Accessories', 29.99, 15.00),
('Office Chair Deluxe', 'Furniture', 'Seating', 349.99, 200.00),
('Standing Desk', 'Furniture', 'Desks', 599.99, 350.00),
('USB-C Hub', 'Electronics', 'Accessories', 49.99, 25.00);

-- ============================================================================
-- SECTION 2: ADVANCED ANALYTICS QUERIES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- QUERY 1: Customer Lifetime Value (CLV) with Running Totals
-- Demonstrates: Window Functions (SUM OVER), CTEs, RANK
-- ----------------------------------------------------------------------------

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
WHERE order_number = total_orders -- Get final CLV per customer
ORDER BY lifetime_value DESC;


-- ----------------------------------------------------------------------------
-- QUERY 2: Cohort Analysis - Monthly Retention Rate
-- Demonstrates: Self Joins, EXTRACT, Complex Window Functions, Cohort Analysis
-- ----------------------------------------------------------------------------

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


-- ----------------------------------------------------------------------------
-- QUERY 3: Product Performance Analysis with Market Basket Insights
-- Demonstrates: Multiple CTEs, Window Functions (LAG, LEAD), Advanced Aggregations
-- ----------------------------------------------------------------------------

WITH product_sales AS (
    SELECT 
        p.product_id,
        p.product_name,
        p.category,
        p.subcategory,
        COUNT(DISTINCT o.order_id) AS times_ordered,
        SUM(oi.quantity) AS total_units_sold,
        SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100)) AS total_revenue,
        SUM(oi.quantity * p.cost_price) AS total_cost,
        AVG(oi.discount_percent) AS avg_discount_rate
    FROM products p
    JOIN order_items oi ON p.product_id = oi.product_id
    JOIN orders o ON oi.order_id = o.order_id
    WHERE o.order_status = 'Completed'
    GROUP BY p.product_id, p.product_name, p.category, p.subcategory
),
product_metrics AS (
    SELECT 
        *,
        total_revenue - total_cost AS gross_profit,
        ROUND(100.0 * (total_revenue - total_cost) / total_revenue, 2) AS profit_margin_pct,
        ROUND(total_revenue / times_ordered, 2) AS revenue_per_order
    FROM product_sales
),
ranked_products AS (
    SELECT 
        *,
        RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
        RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) AS category_rank,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_revenue) OVER (PARTITION BY category) AS category_median_revenue,
        LAG(total_revenue) OVER (PARTITION BY category ORDER BY total_revenue DESC) AS prev_product_revenue,
        LEAD(total_revenue) OVER (PARTITION BY category ORDER BY total_revenue DESC) AS next_product_revenue
    FROM product_metrics
)
SELECT 
    product_name,
    category,
    subcategory,
    total_units_sold,
    ROUND(total_revenue, 2) AS total_revenue,
    ROUND(gross_profit, 2) AS gross_profit,
    profit_margin_pct,
    revenue_rank,
    category_rank,
    ROUND(category_median_revenue, 2) AS category_median_revenue,
    CASE 
        WHEN total_revenue > category_median_revenue THEN 'Above Median'
        ELSE 'Below Median'
    END AS performance_vs_category
FROM ranked_products
ORDER BY total_revenue DESC;


-- ----------------------------------------------------------------------------
-- QUERY 4: Advanced Customer Segmentation with RFM Analysis
-- Demonstrates: NTILE, Complex CASE, Multiple Window Functions, Scoring Logic
-- ----------------------------------------------------------------------------

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


-- ----------------------------------------------------------------------------
-- QUERY 5: Time Series Analysis - Sales Trends with Moving Averages
-- Demonstrates: Window Functions with RANGE, Date Functions, Moving Calculations
-- ----------------------------------------------------------------------------

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
        -- 7-day moving average
        ROUND(AVG(daily_revenue) OVER (
            ORDER BY order_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2) AS moving_avg_7day,
        -- 30-day moving average
        ROUND(AVG(daily_revenue) OVER (
            ORDER BY order_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 2) AS moving_avg_30day,
        -- Month-over-month comparison
        LAG(daily_revenue, 7) OVER (ORDER BY order_date) AS revenue_1week_ago,
        -- Running total for the month
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


-- ----------------------------------------------------------------------------
-- QUERY 6: Category Performance with Recursive Category Hierarchy
-- Demonstrates: Recursive CTEs, Complex Aggregations, Self-Referencing
-- ----------------------------------------------------------------------------

-- First, let's create a category hierarchy table
CREATE TABLE category_hierarchy (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100),
    parent_category_id INT REFERENCES category_hierarchy(category_id)
);

-- Recursive CTE to traverse category hierarchy
WITH RECURSIVE category_tree AS (
    -- Base case: top-level categories
    SELECT 
        category_id,
        category_name,
        parent_category_id,
        category_name AS full_path,
        1 AS level
    FROM category_hierarchy
    WHERE parent_category_id IS NULL
    
    UNION ALL
    
    -- Recursive case: child categories
    SELECT 
        ch.category_id,
        ch.category_name,
        ch.parent_category_id,
        ct.full_path || ' > ' || ch.category_name AS full_path,
        ct.level + 1 AS level
    FROM category_hierarchy ch
    JOIN category_tree ct ON ch.parent_category_id = ct.category_id
)
SELECT 
    category_id,
    REPEAT('  ', level - 1) || category_name AS indented_category_name,
    full_path,
    level
FROM category_tree
ORDER BY full_path;


-- ----------------------------------------------------------------------------
-- QUERY 7: Customer Purchase Patterns - Next Purchase Prediction
-- Demonstrates: LAG/LEAD, Date Arithmetic, Statistical Analysis
-- ----------------------------------------------------------------------------

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
WHERE total_orders >= 2 -- Only for customers with repeat purchases
ORDER BY days_since_last_order DESC;


-- ----------------------------------------------------------------------------
-- QUERY 8: Complex Multi-Dimensional Sales Report
-- Demonstrates: CUBE/ROLLUP, GROUPING, Multiple Dimensions
-- ----------------------------------------------------------------------------

SELECT 
    COALESCE(p.category, 'ALL CATEGORIES') AS category,
    COALESCE(c.country, 'ALL COUNTRIES') AS country,
    COALESCE(TO_CHAR(o.order_date, 'YYYY-MM'), 'ALL MONTHS') AS order_month,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT c.customer_id) AS unique_customers,
    SUM(oi.quantity) AS total_units_sold,
    ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100)), 2) AS total_revenue,
    ROUND(AVG(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100)), 2) AS avg_order_value,
    GROUPING(p.category) AS category_grouping,
    GROUPING(c.country) AS country_grouping,
    GROUPING(TO_CHAR(o.order_date, 'YYYY-MM')) AS month_grouping
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE o.order_status = 'Completed'
GROUP BY CUBE(p.category, c.country, TO_CHAR(o.order_date, 'YYYY-MM'))
ORDER BY 
    GROUPING(p.category), 
    GROUPING(c.country), 
    GROUPING(TO_CHAR(o.order_date, 'YYYY-MM')),
    category,
    country,
    order_month;


-- ============================================================================
-- SECTION 3: PERFORMANCE OPTIMIZATION
-- ============================================================================

-- Create indexes for better query performance
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_order_date ON orders(order_date);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_product_id ON order_items(product_id);
CREATE INDEX idx_customers_segment ON customers(customer_segment);
CREATE INDEX idx_products_category ON products(category);

-- Create composite index for common query patterns
CREATE INDEX idx_orders_customer_date_status ON orders(customer_id, order_date, order_status);

-- ============================================================================
-- END OF PROJECT
-- ============================================================================
