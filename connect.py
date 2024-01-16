from sqlalchemy import create_engine
import mysql.connector
import os
from dotenv import load_dotenv

def get_variables():
    load_dotenv('.env')
    username = os.environ['USER']
    passwd = os.environ['PASSWD']
    database = os.environ['DATABASE']
    host = os.environ['HOST']
    return username, passwd, database, host


def sqlalchemy_conn():
    username, passwd, db, hostname = get_variables()
    try:
        engine = create_engine(f'mysql://{username}:{passwd}@{hostname}/{db}')
        conn = engine.connect()
    except Exception as error:
        print('cant connect to db',error)
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