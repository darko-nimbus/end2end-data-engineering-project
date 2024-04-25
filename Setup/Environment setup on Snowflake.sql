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
create user ******** 
  password = '********' 
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

