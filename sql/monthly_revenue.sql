-- Monthly revenue trend for last 6 months (PostgreSQL version)
SELECT 
    TO_CHAR(order_date, 'YYYY-MM') as month,
    ROUND(SUM(amount), 2) as monthly_revenue
FROM order_details 
WHERE status = 'completed' 
    AND order_date >= CURRENT_DATE - INTERVAL
GROUP BY TO_CHAR(order_date, 'YYYY-MM')
ORDER BY month DESC;