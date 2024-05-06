-- TOP 10 CUSTOMERS IN THE LAST 12 MONTHS, ARRANGED BY REVENUE -- 

SELECT 
    oi.customer_id,
    SUM(oi.order_total) AS total_revenue
FROM order_info AS oi
WHERE 
    oi.order_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '11 months'
    AND oi.order_date < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' 
GROUP BY oi.customer_id
ORDER BY total_revenue DESC
LIMIT 10;


-- MONTHLY REVENUE OF THE TOP 10 CUSTOMERS --

WITH MonthlyRevenue AS (
    SELECT 
        customer_id,
        DATE_TRUNC('month', order_date) AS order_month,
        SUM(order_total) AS monthly_revenue
    FROM order_info
    WHERE 
        customer_id IN (
            SELECT customer_id
            FROM order_info
            GROUP BY customer_id
            ORDER BY SUM(order_total) DESC
            LIMIT 10
        )
        AND order_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '11 months'
        AND order_date < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'
    GROUP BY 
        customer_id, DATE_TRUNC('month', order_date)
)
SELECT 
    customer_id,
    order_month,
    monthly_revenue
FROM MonthlyRevenue
ORDER BY customer_id, order_month DESC;


-- TOP 10 ITEMS BOUGHT BY THE TOP CUSTOMERS, ARRANGED BY REVENUE --

WITH TopCustomers AS (
    SELECT customer_id
    FROM order_info
    WHERE 
      order_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '11 months'
      AND order_date < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'
    GROUP BY customer_id
    ORDER BY SUM(order_total) DESC
    LIMIT 10
),
CustomerTopItems AS (
    SELECT 
        oi.customer_id,
        ii.sku,
        SUM(ii.subtotal) AS revenue
    FROM order_info oi
    JOIN item_info ii ON oi.order_id = ii.order_id
    JOIN TopCustomers tc ON oi.customer_id = tc.customer_id
    GROUP BY oi.customer_id, ii.sku
),
RankedCustomerItems AS (
    SELECT 
        cti.customer_id,
        cti.sku,
        cti.revenue,
        ROW_NUMBER() OVER (PARTITION BY cti.customer_id ORDER BY cti.revenue DESC) AS item_rank
    FROM CustomerTopItems cti
)
SELECT 
    rci.customer_id,
    rci.sku,
    rci.revenue
FROM RankedCustomerItems rci
WHERE rci.item_rank <= 10


-- MOST COMMON PAIRING FOR EACH ITEM --

WITH SkuPairs AS (
    SELECT 
        ii1.sku AS sku,
        ii2.sku AS sku2,
        COUNT(*) AS pair_count
    FROM item_info ii1
    JOIN item_info ii2 ON ii1.order_id = ii2.order_id
    WHERE ii1.sku != ii2.sku
    GROUP BY ii1.sku, ii2.sku
),
RankedPairs AS (
    SELECT 
        sku,
        sku2,
        pair_count,
        ROW_NUMBER() OVER (PARTITION BY sku ORDER BY pair_count DESC) AS rank
    FROM SkuPairs
)
SELECT 
    sku,
    sku2,
    pair_count
FROM RankedPairs
WHERE rank <= 1


-- AVERAGE ORDER VALUE AND ORDER COUNT, ARRANGED BY DAY OF MONTH -- 

SELECT
  EXTRACT(day FROM order_date) AS extracted_date,
  COUNT(order_id) AS order_count,
  ROUND(AVG(order_total), 2) AS avg_order_value
FROM order_info
GROUP BY extracted_date
ORDER BY extracted_date

-- TRUNCATED DAILY, MONTHLY, AND YEARLY REVENUE AND PROFIT

WITH daily_revenue_profit AS (
    SELECT
        DATE_TRUNC('day', oi.order_date) AS order_day,
        DATE_TRUNC('month', oi.order_date) AS order_month,
        DATE_TRUNC('year', oi.order_date) AS order_year,
        SUM(ii.subtotal) AS total_revenue,
        SUM(ii.quantity * si.price * (1-ii.discount_percent/100) - (ii.quantity * si.cost)) AS total_profit
    FROM order_info oi
    JOIN item_info ii ON oi.order_id = ii.order_id
    JOIN sku_info si ON ii.sku = si.sku
    GROUP BY order_day, order_month, order_year
)
SELECT
    order_day, order_month, order_year,
    COALESCE(SUM(total_revenue), 0) AS total_revenue,
    COALESCE(SUM(total_profit), 0) AS total_profit
FROM daily_revenue_profit
GROUP BY order_day, order_month, order_year
ORDER BY order_day DESC;


-- PERCENT OF ON TIME DELIVERIES

SELECT 
    ROUND(((SUM(CASE 
					WHEN finished_date <= eta + INTERVAL '24 hours' THEN 1 
					ELSE 0 END
			   )::numeric / COUNT(*)) * 100), 2) AS on_time_percentage
FROM 
    order_info;

 