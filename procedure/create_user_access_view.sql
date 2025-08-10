create or replace procedure create_user_access_view(table_name string)
returns string
language python
runtime_version = '3.8'
packages = ('snowflake-snowpark-python')
handler = 'create_user_access_view'
as 
$$
def create_user_access_view(session, table_name):
    table_structure = session.table(table_name)
    select_clause = ",".join([col.name for col in table_structure.schema.fields])
    filter_nest = [["_bfr_2011","<"],["_aftr_2010",">"]]
    for each_conditional_filter in filter_nest:
        session.sql(f"create or replace view {table_name}" + each_conditional_filter[0] + f" as select {select_clause} from {table_name} where trade_date " + each_conditional_filter[1] + " '2010-12-31'").collect()
    return f"views created for {table_name}"
$$
;
