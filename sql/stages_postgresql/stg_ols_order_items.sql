with products_computes as (
	select
		i.order_id,
		i.product_id,
		i.seller_id,
		o.customer_id,
		p.payment_type,
		sum(i.price) / max(p.payment_sequential) as total_item_price,
		sum(i.freight_value) / max(p.payment_sequential) as total_freight_value,
		max(p.payment_sequential) as max_payment_sequential,
		max(p.payment_installments) as max_payment_installments,
		sum(p.payment_value) as total_payment_value
	from order_items i
	left join orders o on o.order_id = i.order_id
	left join order_payments p on p.order_id = i.order_id 
	where 
		(o.order_status = 'delivered' or 
		o.order_status = 'shipped'   or
		o.order_status = 'approved') AND 
		p.order_id is not null
	group by i.order_id, i.product_id, 
	         i.seller_id, o.customer_id, 
	         p.payment_type
)
select
	c.order_id,
	c.product_id,
	c.seller_id,
	c.customer_id,
	c.payment_type,
	o.order_status,
	o.order_purchase_timestamp,
	o.order_approved_at,
	o.order_delivered_carrier_date,
	o.order_delivered_customer_date,
	o.order_estimated_delivery_date,
	c.total_item_price,
	c.total_freight_value,
	c.max_payment_sequential,
	c.max_payment_installments,
	c.total_payment_value
from products_computes c
left join orders o on o.order_id = c.order_id and o.customer_id = c.customer_id