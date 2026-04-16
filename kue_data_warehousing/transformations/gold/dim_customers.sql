CREATE VIEW gold_dim_customers As
select d.cst_id AS customer_id,
	   d.cst_key AS customer_number,
	   d.cst_firstname AS first_name,
	   d.cst_lastname AS last_name,
	   d.cst_marital_status AS marital_status,
	   v.country AS country,
	   Case when d.cst_gndr != 'n/a' then d.cst_gndr
	   Else Coalesce(c.gender,'n/a') 
	   End AS gender,
	   c.birthday AS birth_date,
	   d.cst_create_date AS created_date
from kue_data_warehousing_source.silver.crm_customer d
left join kue_data_warehousing_source.silver.erp_customers c
on d.cst_key =c.cid
left join kue_data_warehousing_source.silver.erp_location v
on d.cst_key =v.cid;