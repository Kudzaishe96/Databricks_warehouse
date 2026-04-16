CREATE VIEW gold_dim_products AS
select  v.prd_id AS product_id,
		v.prd_key AS product_number,
		v.prd_nm AS product_name,
		v.prd_sk AS product_key_number,
		v.prd_cost AS product_cost,
		v.prd_line AS product_line,
		v.prd_start_dt AS start_date,
		v.prd_end_dt AS end_date,
		v.prod_cat_sk AS category_id,
		b.category AS category,
		b.sub_category AS sub_category,
		b.Maintenance AS maintainance
from kue_data_warehousing_source.silver.crm_products v
left join kue_data_warehousing_source.silver.erp_products b
on b.id = v.prod_cat_sk
where v.prd_end_dt is null