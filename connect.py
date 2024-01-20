from sqlalchemy import create_engine
import mysql.connector
import os
from dotenv import load_dotenv
import pandas as pd
from pathlib import Path

def get_variables(by_type):
    env_path = Path('C:\\Users\\FATAH\\Documents\\REPO\\PROJECT\\env_file\\get_size.env')
    load_dotenv(dotenv_path=env_path)
    username = os.environ['USER'].split(",")
    passwd = os.environ['PASSWD'].split(",")
    database = os.environ['DATABASE'].split(",")
    host = os.environ['HOST'].split(",")
    db_type = os.environ['DB_TYPE'].split(",")
    db_driver = os.environ['DB_DRIVER'].split(",")

    df = pd.DataFrame(list(zip(username,passwd,database,host,db_type,db_driver)),columns=['username','passwd','database','host','db_type','db_driver'])
    data = df.query("db_type == @by_type")

    return data['username'].iloc[0],data['passwd'].iloc[0],data['database'].iloc[0],data['host'].iloc[0],data['db_driver'].iloc[0]


def sqlalchemy_conn(db_type):
    for x in db_type:
        username, passwd, db, hostname,db_driver = get_variables(x)
        #print(x)
        try:
            engine = create_engine(f'{db_driver}://{username}:{passwd}@{hostname}/{db}')
            conn = engine.connect()
        except Exception as error:
            print('cant connect to db',error)
        else:
            return conn

def mysql_conn():
    username, passwd, db, hostname = get_variables()
    try:
        engine = mysql.connector.connect(host=hostname,user=username,password=passwd,database=db)
    except Exception as error:
        print('cant connect to db',error)
    
    return engine

def postgres_conn():
    print('a')


sqlalchemy_conn(['mysql'])