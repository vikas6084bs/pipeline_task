-- Most popular product categories by number of orders
SELECT 
    category, 
    COUNT(*) as order_count
FROM order_details 
WHERE status = 'completed'
GROUP BY category 
ORDER BY order_count DESC

