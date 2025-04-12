create database etl;
use etl;
create schema bronze;
create schema silver;
create schema gold;

create or replace table load_tracker 
(
table_name varchar(30),
last_load_dtm timestamp_ltz(9)
)
;
