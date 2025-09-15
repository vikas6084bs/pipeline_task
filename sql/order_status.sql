-- Count of orders by status
SELECT 
    status,
    COUNT(*) as order_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM order_details), 2) as percentage
FROM order_details 
GROUP BY status 
ORDER BY order_count DESC;