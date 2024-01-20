import pandas as pd
import connect
from datetime import datetime
from dotenv import load_dotenv
import os
from pathlib import Path

def get_size(db_type):
    #print(db_type)
    now = datetime.today()
    conn = connect.sqlalchemy_conn([db_type])
    if db_type == 'mysql':
        sql = '''
        SELECT table_schema "database_size_name",
        ROUND(SUM(data_length + index_length) / 1024 / 1024, 1) "database_size_mb" 
        FROM information_schema.tables 
        GROUP BY table_schema; 
    '''
    elif db_type == 'postgres':
        sql = '''
        SELECT pg_database.datname as "database_size_name", pg_database_size(pg_database.datname)/1024/1024 AS database_size_mb 
        FROM pg_database ORDER by database_size_mb DESC;
    '''
    else:
        print('db_type not define')
    
    try:
        data = pd.read_sql(sql,conn)
        data['created_date']= now
        data['updated_date']= now
        data['database_size_type'] = db_type
        #print(data)
    except Exception as error:
        print(error)
    else:
        return data
    conn.close()


def put_size(type_input,mon_db):
    table_name = 'database_size'
    conn = connect.sqlalchemy_conn(mon_db)

    for x in type_input:
        print(x)
        data = get_size(x)
        print(data)
        put_data = data.to_sql(table_name, conn, if_exists='append',index=False)


if __name__ == '__main__':
    env_path = Path('C:\\Users\\FATAH\\Documents\\REPO\\PROJECT\\env_file\\get_size.env')
    load_dotenv(dotenv_path=env_path)
    db_type = os.environ['DB_TYPE'].split(",")
    mon_db = os.environ['MON_DB']
    for x in db_type:
        put_size([x],[mon_db])