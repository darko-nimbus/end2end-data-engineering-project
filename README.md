# End 2 End Data Engineering Project

This report contains projects based on the YouTube tutorials by [Data Engineering Project](https://www.youtube.com/watch?v=1jC98XQwBZw&list=PLba2xJ7yxHB6W0XT7gxeY1HbJ39FMWoUF&index=1).

## Requisites:
- Snowflake Free Trial
- Python 3.8.x
- Snowpark Python Lib
- VS Code
- SnoSQL CLI

## Organization: 
- 1 database
- 3 schemas, named bronze (source), silver (curation), and gold (consumption)
- 

## datasets: 
Data used in this project is Amazon sales data related to mobile handset across three different regions. Data comes in three different formats and is loaded into an internal stage in the Bronze Schema. 

- USA Sales, in Parquet format
- India Sales, in CSV format
- France Sales, in JSON format

###  step 1 - Create user & Virtual Warehouse

Followed the instructions on [this blog post on Medium](https://data-engineering-simplified.medium.com/end-to-end-de-project-using-snowpark-amazon-sales-analytics-744c1d5a8d50)

###  step 2 - Create Databases & Schema

## Step 4.2 Loading Data To Internal Stage Using Snowpark File API
I had to use a different script than the one that was provided.

