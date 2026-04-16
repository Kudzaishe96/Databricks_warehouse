CREATE VIEW gold_facts_sales AS
select
		s.sls_ord_num AS order_number,
		s.sls_prd_key AS product_key,
		c.customer_id AS customer_key,
		s.sls_order_dt AS sales_oder_date,
		s.sls_ship_dt AS sales_ship_date,
		s.sls_due_dt AS sales_due_date,
		s.sls_sales AS sales,
		s.sls_quantity AS sales_quantity,
		s.sls_price AS sale_price
from kue_data_warehousing_source.silver.crm_sales s
left join gold_dim_customers c
on s.sls_cust_id =c.customer_id
left join gold_dim_products p
on s.sls_prd_key =p.product_key_number