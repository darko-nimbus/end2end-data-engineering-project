CREATE DATABASE E2E_AMAZON_SALES;
CREATE SCHEMA SOURCE;

-- create a virtual warehouse
use role sysadmin;
create warehouse snowpark_etl_wh 
    with 
    warehouse_size = 'XSMALL' 
    warehouse_type = 'standard' 
    auto_suspend = 1 
    auto_resume = true 
    min_cluster_count = 1
    max_cluster_count = 1 
    scaling_policy = 'standard';

-- create a snowpark user (it can only be created using accountadmin role)
use role accountadmin;
create user snowpark_user 
  password = 'Test@12$4' 
  comment = 'this is a s snowpark user' 
  default_role = sysadmin
  default_secondary_roles = ('ALL')
  must_change_password = false;

-- grants
grant role sysadmin to user snowpark_user;
grant USAGE on warehouse snowpark_etl_wh to role sysadmin;


-- create database
create database if not exists E2E_AMAZON_SALES;

use database E2E_AMAZON_SALES;

create schema if not exists source; -- will have source stage etc
create schema if not exists curated; -- data curation and de-duplication
create schema if not exists consumption; -- fact & dimension
create schema if not exists audit; -- to capture all audit records
create schema if not exists common; -- for file formats sequence object etc

-- creating internal stage within source schema.
use schema source;
create or replace stage my_internal_stg;

-- Upload of data happened on VS Code via python script.

-- Checking that data was uploaded into the stage.
DESC STAGE MY_INTERNAL_STG;

list @MY_INTERNAL_STG/source=FR/;
list @MY_INTERNAL_STG/source=US/;
list @MY_INTERNAL_STG/source=IN/;

-- File Format Objects Within Common Schema

use schema common;

-- create file formats csv (India), json (France), Parquet (USA)
create or replace file format my_csv_format
  type = csv
  field_delimiter = ','
  skip_header = 1
  null_if = ('null', 'null')
  empty_field_as_null = true
  field_optionally_enclosed_by = '\042'
  compression = auto;

-- json file format with strip outer array true
create or replace file format my_json_format
  type = json
  strip_outer_array = true
  compression = auto;

-- parquet file format
create or replace file format my_parquet_format
  type = parquet
  compression = snappy;

  show file formats;

  desc file format my_json_format;

--query staged files without table

  use schema source;

-- Internal Stage - Query The CSV Data File Format
select 
    t.$1::text as order_id, 
    t.$2::text as customer_name, 
    t.$3::text as mobile_key,
    t.$4::number as order_quantity, 
    t.$5::number as unit_price, 
    t.$6::number as order_valaue,  
    t.$7::text as promotion_code , 
    t.$8::number(10,2)  as final_order_amount,
    t.$9::number(10,2) as tax_amount,
    t.$10::date as order_dt,
    t.$11::text as payment_status,
    t.$12::text as shipping_status,
    t.$13::text as payment_method,
    t.$14::text as payment_provider,
    t.$15::text as mobile,
    t.$16::text as shipping_address
 from 
   @e2e_amazon_sales.source.my_internal_stg/source=IN/format=csv/
   (file_format => 'e2e_amazon_sales.common.my_csv_format') t; 

-- Internal Stage - Query The Parquet Data File Format

LIST @e2e_amazon_sales.source.my_internal_stg/source=US/format=parquet/;

-- Remove .DS_Store files if listed
REMOVE @e2e_amazon_sales.source.my_internal_stg/source=US/format=parquet/.DS_Store;
   
select 
  t.$1:"Order ID"::text as ordercanv_id,
  t.$1:"Customer Name"::text as customer_name,
  t.$1:"Mobile Model"::text as mobile_key,
  t.$1:"Quantity"::number as quantity,
  t.$1:"Price per Unit"::number(10,2) as unit_price,
  t.$1:"Total Price"::number(10,2) as total_price,
  t.$1:"Promotion Code"::text as promotion_code,
  t.$1:"Order Amount"::number as order_amount,
  t.$1:"Tax"::number(10,2) as tax,
  t.$1:"Order Date"::date as order_dt,
  t.$1:"Payment Status"::text as payment_status,
  t.$1:"Shipping Status"::text as shipping_status,
  t.$1:"Payment Method"::text as payment_method,
  t.$1:"Payment Provider"::text as payment_provider,
  t.$1:"Phone"::text as phone,
  t.$1:"Delivery Address"::text as shipping_address
from 
     @e2e_amazon_sales.source.my_internal_stg/source=US/format=parquet/
     (file_format => 'e2e_amazon_sales.common.my_parquet_format') t;

-- Internal Stage - Query The JSON Data File Format

-- List files in the stage
LIST @e2e_amazon_sales.source.my_internal_stg/source=FR/format=json/;

-- Remove .DS_Store files if listed
REMOVE @e2e_amazon_sales.source.my_internal_stg/source=FR/format=json/.DS_Store;
    
select                                                       
    $1:"Order ID"::text as order_id,                   
    $1:"Customer Name"::text as customer_name,          
    $1:"Mobile Model"::text as mobile_key,              
    to_number($1:"Quantity") as quantity,               
    to_number($1:"Price per Unit") as unit_price,       
    to_decimal($1:"Total Price") as total_price,        
    $1:"Promotion Code"::text as promotion_code,        
    $1:"Order Amount"::number(10,2) as order_amount,    
    to_decimal($1:"Tax") as tax,                        
    $1:"Order Date"::date as order_dt,                  
    $1:"Payment Status"::text as payment_status,        
    $1:"Shipping Status"::text as shipping_status,      
    $1:"Payment Method"::text as payment_method,        
    $1:"Payment Provider"::text as payment_provider,    
    $1:"Phone"::text as phone,                          
    $1:"Delivery Address"::text as shipping_address
from                                                
@e2e_amazon_sales.source.my_internal_stg/source=FR/format=json/
(file_format => e2e_amazon_sales.common.my_json_format);

-- put 'file://C:/Users/darko/Downloads/exchange-rate-data.csv' @e2e_amazon_sales.source.my_internal_stg/exchange/ parallel=10 auto_compress=false;--

list @e2e_amazon_sales.source.my_internal_stg/exch
use schema common;

create or replace transient table exchange_rate(
    date date, 
    usd2usd decimal(10,7),
    usd2eu decimal(10,7),
    usd2can decimal(10,7),
    usd2uk decimal(10,7),
    usd2inr decimal(10,7),
    usd2jp decimal(10,7)
);

copy into e2e_amazon_sales.common.exchange_rate
from 
(
select 
    t.$1::date as exchae2e_amazon_salesnge_dt,
    to_decimal(t.$2) as usd2usd,
    to_decimal(t.$3,12,10) as usd2eu,
    to_decimal(t.$4,12,10) as usd2can,
    to_decimal(t.$4,12,10) as usd2uk,
    to_decimal(t.$4,12,10) as usd2inr,
    
    to_decimal(t.$4,12,10) as usd2jp
from 
     @e2e_amazon_sales.source.my_internal_stg/exchange/exchange-rate.csv
     (file_format => 'e2e_amazon_sales.common.my_csv_format') t
);

-- order table
use schema source;

create or replace sequence in_sales_order_seq 
  start = 1 
  increment = 1 
comment='This is sequence for India sales order table';

create or replace sequence us_sales_order_seq 
  start = 1 
  increment = 1 
  comment='This is sequence for USA sales order table';

create or replace sequence fr_sales_order_seq 
  start = 1 
  increment = 1 
  comment='This is sequence for France sales order table';


  show sequences;
  
-- India Sales Table in Source Schema (CSV File)
create or replace transient table in_sales_order (
 sales_order_key number(38,0),
 order_id varchar(),
 customer_name varchar(),
 mobile_key varchar(),
 order_quantity number(38,0),
 unit_price number(38,0),
 order_value number(38,0),
 promotion_code varchar(),
 final_order_amount number(10,2),
 tax_amount number(10,2),
 order_dt date,
 payment_status varchar(),
 shipping_status varchar(),
 payment_method varchar(),
 payment_provider varchar(),
 mobile varchar(),
 shipping_address varchar(),
 _metadata_file_name varchar(),
 _metadata_row_numer number(38,0),
 _metadata_last_modified timestamp_ntz(9)
);

-- US Sales Table in Source Schema (Parquet File)
create or replace transient table us_sales_order (
 sales_order_key number(38,0),
 order_id varchar(),
 customer_name varchar(),
 mobile_key varchar(),
 order_quantity number(38,0),
 unit_price number(38,0),
 order_value number(38,0),
 promotion_code varchar(),
 final_order_amount number(10,2),
 tax_amount number(10,2),
 order_dt date,
 payment_status varchar(),
 shipping_status varchar(),
 payment_method varchar(),
 payment_provider varchar(),
 phone varchar(),
 shipping_address varchar(),
 _metadata_file_name varchar(),
 _metadata_row_numer number(38,0),
 _metadata_last_modified timestamp_ntz(9)
);

-- France Sales Table in Source Schema (JSON File)
create or replace transient table fr_sales_order (
 sales_order_key number(38,0),
 order_id varchar(),
 customer_name varchar(),
 mobile_key varchar(),
 order_quantity number(38,0),
 unit_price number(38,0),
 order_value number(38,0),
 promotion_code varchar(),
 final_order_amount number(10,2),
 tax_amount number(10,2),
 order_dt date,
 payment_status varchar(),
 shipping_status varchar(),
 payment_method varchar(),
 payment_provider varchar(),
 phone varchar(),
 shipping_address varchar(),
 _metadata_file_name varchar(),
 _metadata_row_numer number(38,0),
 _metadata_last_modified timestamp_ntz(9)
);

-- Copy into the tables

COPY INTO e2e_amazon_sales.source.in_sales_order
FROM (
    SELECT
        in_sales_order_seq.nextval,
        t.$1::text AS order_id,
        t.$2::text AS customer_name,
        t.$3::text AS mobile_key,
        t.$4::number AS order_quantity,
        t.$5::number AS unit_price,
        t.$6::number AS order_value,
        t.$7::text AS promotion_code,
        t.$8::number(10,2) AS final_order_amount,
        t.$9::number(10,2) AS tax_amount,
        t.$10::date AS order_dt,
        t.$11::text AS payment_status,
        t.$12::text AS shipping_status,
        t.$13::text AS payment_method,
        t.$14::text AS payment_provider,
        t.$15::text AS mobile,
        t.$16::text AS shipping_address,
        metadata$filename AS stg_file_name,
        metadata$file_row_number AS stg_row_number,
        metadata$file_last_modified AS stg_last_modified
    FROM
        @e2e_amazon_sales.source.my_internal_stg/source=IN/format=csv/
        (file_format => 'e2e_amazon_sales.common.my_csv_format') t
);

COPY INTO e2e_amazon_sales.source.us_sales_order
FROM (
    SELECT
        us_sales_order_seq.nextval,
        $1:"Order ID"::text AS order_id,
        $1:"Customer Name"::text AS customer_name,
        $1:"Mobile Model"::text AS mobile_key,
        to_number($1:"Quantity") AS quantity,
        to_number($1:"Price per Unit") AS unit_price,
        to_decimal($1:"Total Price") AS total_price,
        $1:"Promotion Code"::text AS promotion_code,
        $1:"Order Amount"::number(10,2) AS order_amount,
        to_decimal($1:"Tax") AS tax,
        $1:"Order Date"::date AS order_dt,
        $1:"Payment Status"::text AS payment_status,
        $1:"Shipping Status"::text AS shipping_status,
        $1:"Payment Method"::text AS payment_method,
        $1:"Payment Provider"::text AS payment_provider,
        $1:"Phone"::text AS phone,
        $1:"Delivery Address"::text AS shipping_address,
        metadata$filename AS stg_file_name,
        metadata$file_row_number AS stg_row_number,
        metadata$file_last_modified AS stg_last_modified
    FROM
        @e2e_amazon_sales.source.my_internal_stg/source=US/format=parquet/
        (file_format => 'e2e_amazon_sales.common.my_parquet_format')
);


COPY INTO e2e_amazon_sales.source.fr_sales_order
FROM (
    SELECT
        fr_sales_order_seq.nextval,
        $1:"Order ID"::text AS order_id,
        $1:"Customer Name"::text AS customer_name,
        $1:"Mobile Model"::text AS mobile_key,
        to_number($1:"Quantity") AS quantity,
        to_number($1:"Price per Unit") AS unit_price,
        to_decimal($1:"Total Price") AS total_price,
        $1:"Promotion Code"::text AS promotion_code,
        $1:"Order Amount"::number(10,2) AS order_amount,
        to_decimal($1:"Tax") AS tax,
        $1:"Order Date"::date AS order_dt,
        $1:"Payment Status"::text AS payment_status,
        $1:"Shipping Status"::text AS shipping_status,
        $1:"Payment Method"::text AS payment_method,
        $1:"Payment Provider"::text AS payment_provider,
        $1:"Phone"::text AS phone,
        $1:"Delivery Address"::text AS shipping_address,
        metadata$filename AS stg_file_name,
        metadata$file_row_number AS stg_row_number,
        metadata$file_last_modified AS stg_last_modified
    FROM
        @e2e_amazon_sales.source.my_internal_stg/source=FR/format=json/
        (file_format => e2e_amazon_sales.common.my_json_format)
);
