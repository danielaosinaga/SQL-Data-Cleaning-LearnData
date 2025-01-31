CREATE DATABASE  IF NOT EXISTS `learndataDB`;
USE `learndataDB`;

#Creation dimension table dim_product
CREATE TABLE learndataDB.dim_product (
id_product int,
type_product varchar(50),
name_product varchar(100),
published_Ind int,
catalog_visibility_ind int,
stock_ind int,
stock_status varchar(50),
sold_ind int,
price decimal(10,2),
category varchar(50),
PRIMARY KEY (id_product)
)
;

#Insert data into the table dim_product
INSERT INTO learndataDB.dim_product
select 
id ,
type,
name ,
published ,
case 
	when catalog_visibility ='visible' then 1
    when catalog_visibility !='visible' then 0
end catalog_visibility_ind,
in_stock ,
stock,
sold_individually,
regular_price,
categories
from learndata_raw.raw_products_wocommerce;


#Creation dimension table dim_customers
CREATE TABLE learndataDB.dim_customers (
id_customers int primary key,
name_customer varchar(15),
lastname_customer varchar(30),
email varchar(50),
phone varchar(20),
country varchar(40),
state varchar(40),
date_created date
)
;

#generate a select statement with the data from the source table just as we would like to insert it into the newly created table.
#use the json_value functions to extract the value of the fields and str_to_date to cast the source string to the date.
SELECT 
id,
json_value(billing,"$.first_name") as name,
last_name as apellido,
json_value(billing,"$.email") as email,
json_value(billing,"$.phone") as phone,
json_value(billing,"$.country") as country,
json_value(billing,"$.Region") as state,
str_to_date(date_created,'%d/%m/%Y %H:%i:%s') as date_created
FROM learndata_raw.raw_customers_wocommerce;

#Insert the data into the customers table directly from the select generated in the previous step.
insert into learndataDB.dim_customers
SELECT 
id,
json_value(billing,"$.first_name") as name_customer,
last_name as lastname_customer,
json_value(billing,"$.email") as email,
json_value(billing,"$.phone") as phone,
json_value(billing,"$.country") as country,
json_value(billing,"$.Region") as state,
str_to_date(date_created,'%d/%m/%Y %H:%i:%s') as date_created
FROM learndata_raw.raw_customers_wocommerce;


#Creation dimension table fact_orders
CREATE TABLE learndataDB.fact_orders
(
id_order int PRIMARY KEY,
sku_product int,
order_status varchar(50),
order_Date datetime,
payment_method varchar(50),
subtotal_amount decimal(10,2),
discount_amount decimal(10,2),
cod_coupon varchar(50),
total_amount decimal(10,2),
id_product int,
qty_order int
#id_cliente int
);

#Insert id_customer field. Remember that the created field is placed in the last position of the table.
ALTER TABLE learndataDB.fact_orders
ADD COLUMN id_customer INT;

#Alter name of the id_customer field to see how we would do it in a supposed need.
ALTER TABLE learndataDB.fact_orders
RENAME COLUMN id_customer TO id_customers;

#Alter subtotal_amount field because originally the field that contains this information measures 12.2 and it was created at 10.2
ALTER TABLE learndataDB.fact_orders
MODIFY COLUMN subtotal_amount decimal(12,2);

#Alter the sku_product field because it was created as an integer but the source field is a varchar.
ALTER TABLE learndataDB.fact_orders
MODIFY COLUMN sku_product varchar(100);


/*SELECT statement with the data from the source table, formatted as we would like to insert it into the newly created table.
Using STR_TO_DATE function to convert the source string to a date, and the CASE function to create a calculated field. 
Additionally, adding LEFT JOIN function to include the product_id field that is not in the orders table but is in the table we created on 
the first day (more information about the JOIN is provided below the SELECT statement).
Lastly, we standardize the payment method field to group the different values from the source into just two categories: 'stripe' or 'card'. */
select 
order_number,
sku,
order_status,
str_to_date(order_date,'%Y-%m-%d %H:%i') as order_date,
 case
	when upper(payment_method_tittle) like '%STRIPE%' then 'Stripe'
    else 'Card Payment'
end as payment_method,
cart_subtotal_amount,
cart_discount_amount,
item_coupon,
order_total_amount,
prod.id_product,
`id client`
from learndata_raw.raw_orders_wocommerce ped
 left join learndataDB.dim_product prod
 on replace(name_product,'dashborads','dashboards') = prod.name_product;
 
 
#with the previuos select, execute INSERT statement
insert into learndataDB.fact_orders
select 
order_number,
sku,
order_status,
str_to_date(order_date,'%Y-%m-%d %H:%i') as order_date,
 case
	when upper(payment_method_tittle) like '%STRIPE%' then 'Stripe'
    else 'Card Payment'
end as payment_method,
cart_subtotal_amount,
cart_discount_amount,
item_coupon,
order_total_amount,
prod.id_product,
quantity,
`id client`
from learndata_raw.raw_orders_wocommerce ped
 left join learndataDB.dim_product prod
 on replace(item_name,'dashborads','dashboards') = prod.name_product;
 
#Creation table fees Stripe
#drop table fact_fees_stripe;
CREATE TABLE fact_fees_stripe (
  id_fees VARCHAR(50),
  payment_date datetime,
  id_order int ,
  net_payment decimal(10,2)  ,
  currency VARCHAR(5),
  commission_payment decimal(10,2) ,
  order_amount decimal(10,2) ,
  payment_method VARCHAR(50),
  PRIMARY KEY (id_fees)
);

insert into fact_fees_stripe 
SELECT
	id as id_fees,
	timestamp(STR_TO_DATE(created,"%Y-%m-%dT%H:%i:%sZ")),
	RIGHT(description,5) as id_order,
	amount as order_amount,
	currency as currency,
	CAST(REPLACE(fee,',','.')AS DECIMAL(10,2)) as commission_payment,
	CAST(REPLACE(net,',','.') AS DECIMAL(10,2))  as net_payment,
	type as payment_method
FROM learndata_raw.raw_fees_stripe;

# Verify that there are no null fields, and if they exist, replace them with a placeholder value (according to the data type)
SELECT *
  FROM learndataDB.fact_fees_stripe 
WHERE net_payment is null;


-- In this case, we see that the net_payment is the sum of order_amount + commission_payment. First, we verify that it is correct and then update the table.
select * 
from fact_fees_stripe  
where net_payment != round(commission_payment + order_amount,0);

-- We disable an internal data protection service that MySQL has in order to update data:
SET SQL_SAFE_UPDATES = 0;

update learndataDB.fact_fees_stripe  set net_payment=round(commission_payment + order_amount,0) where id_order ='28990';

-- update net_payment
Select * 
from learndataDB.fact_fees_stripe 
 where id_order ='28990';
 
SELECT *
  FROM learndataDB.fact_fees_stripe 
WHERE net_payment is null;

# Execute a query that returns dates in different formats.

-- Dates in different formats, and above all, display part of their information (day, month, year, etc...).

select 
payment_date, -- initial format 
month(payment_date), -- month
year(payment_date), -- year
day(payment_date), -- day
substr(payment_date,1,7), -- year-month
DATE_FORMAT(payment_date,"%Y-%m-%d"), -- Add the format you want to see the dates in
DATE_FORMAT(payment_date,"%d/%m/%Y") -- reverse date (day, month, year, w/slash)
from learndataDB.fact_fees_stripe ;


# Check that the data types of the PK fields are integer (int) in the tables.

/* In our case, not all tables have the PK fields as int, since it is not an essential condition.

We can go table by table looking at the PK field and seeing the field type, but we can also access the internal mysql metadata tables
where we can consult this information. There are two tables for them:
- information_schema.KEY_COLUMN_USAG --> where we can filter the columns that are primary key
- information_schema.COLUMNS --> where we have the details of all the fields of all the tables

By crossing both tables, we can get what we are interested in.
*/

SELECT 
    kcu.TABLE_NAME,
    kcu.COLUMN_NAME,
    c.COLUMN_TYPE
FROM 
    information_schema.KEY_COLUMN_USAGE AS kcu
JOIN 
    information_schema.COLUMNS AS c
ON 
    kcu.TABLE_SCHEMA = c.TABLE_SCHEMA
    AND kcu.TABLE_NAME = c.TABLE_NAME
    AND kcu.COLUMN_NAME = c.COLUMN_NAME
WHERE 
    kcu.CONSTRAINT_SCHEMA = 'learndataDB' 
    AND kcu.CONSTRAINT_NAME = 'PRIMARY';



