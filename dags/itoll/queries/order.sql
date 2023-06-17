select * from Orders O
WHERE CAST(O.created_at as DATE) = '{}'
AND O.payment_status = 'Paid'
AND O.order_status = 123