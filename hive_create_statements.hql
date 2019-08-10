create table product(
product_id String, product_name String, product_type String,product_version String,product_price String)
stored as orc tblproperties ("orc.compress"="SNAPPY");

create table customer(
customer_id String, customer_first_name String, customer_last_name String,phone_number String)
stored as orc tblproperties ("orc.compress"="SNAPPY");

create table sales(
transaction_id String, customer_id String, product_id String,timestamp String,total_amount String,total_quantity String )
stored as orc tblproperties ("orc.compress"="SNAPPY");

create table sales(
refund_id String, original_transactional_id String, customer_id String,product_id String,timestamp String,refund_amountString, refund_quantity String)
stored as orc tblproperties ("orc.compress"="SNAPPY");