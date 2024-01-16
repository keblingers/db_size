import connect


def create_table():
    conn = connect.native_conn()
    sql = '''
        CREATE TABLE `database_size` (
        `database_size_id` bigint NOT NULL AUTO_INCREMENT primary key,
        `created_date` datetime(6) DEFAULT NULL,
        `updated_date` datetime(6) DEFAULT NULL,
        `database_size_name` varchar(25) DEFAULT NULL,
        `database_size_type` varchar(25) DEFAULT NULL,
        `database_size_mb` decimal(19,4) DEFAULT NULL
        );
        
    '''
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
    except Exception as error:
        print('failed create database size table',error)
    conn.close()

def add_index():
    conn = connect.native_conn()
    sql='''
        alter table database_size add index database_type (database_size_type);
    '''
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
    except Exception as error:
        print('cant create index',error)


if __name__ == '__main__':
    create_table()
    add_index()