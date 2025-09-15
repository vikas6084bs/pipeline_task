-- Top 5 customers by total spend
SELECT 
    customer_name, 
    country, 
    ROUND(SUM(amount), 2) as total_spend
FROM order_details 
WHERE status = 'completed'
GROUP BY customer_name, country 
ORDER BY total_spend DESC 
