import pymysql


def get_conn(_host, _port, _user, _password, _database, _timeout=None):
    return pymysql.connect(
        host=_host,
        port=_port,
        user=_user,
        password=_password,
        database=_database,
        read_timeout=_timeout,
        charset='utf8mb4'
        # cursorclass=pymysql.cursors.DictCursor
    )


def get_list(conn, sql_text):
    res = None
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_text)
            res = cursor.fetchall()
    finally:
        conn.close()
    return res


def get_one(conn, sql_text):
    res = None
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_text)
            res = cursor.fetchone()
    finally:
        conn.close()
    return res


# insert or update one
def execute_one(conn, sql_text):
    res = None
    with conn.cursor() as cursor:
            cursor.execute(sql_text)
            res = cursor.lastrowid
            conn.commit()
    return res


def execute_many(conn, ls_sql):
    res = False
    try:
        with conn.cursor() as cursor:
            for s in ls_sql:
                sql = s.replace('\n' , ' ').strip(' ')
                if len(sql)>0:
                    print(sql)
                    cursor.execute(sql)
        conn.commit()
        res = True
    finally:
        conn.close()
    return res