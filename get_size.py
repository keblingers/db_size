import pandas as pd
import connect
from datetime import datetime

def mysql_size():
    now = datetime.today()
    conn = connect.sqlalchemy_conn()
    sql = '''
        SELECT table_schema "db_name",
        ROUND(SUM(data_length + index_length) / 1024 / 1024, 1) "db_size_in_mb" 
        FROM information_schema.tables 
        where table_schema in ('airflow','ihsg')
        GROUP BY table_schema; 
    '''
    try:
        data = pd.read_sql(sql,conn)
        data['created_date']= now
        data['updated_date']= now
        data['database_size_type'] = 'mysql'
        print(data)
    except Exception as error:
        print(error)
    conn.close()

def put_size():
    conn = connect.sqlalchemy_conn()
    mysql = mysql_size()
    db_type_list = [mysql]

    for x in db_type_list:
        print(x)

put_size()