import sqlite3

path = 'file_path'


def success_log(log):
    with open('success_log.text', 'a+') as f:
        f.write(log + "\r\r\n")
        f.close()


def error_log(log):
    with open('error_log.txt', 'a+') as f:
        f.write(log + "\r\r\n")
        f.close()


def execute_statement(sql_script, curr):
    try:
        curr.execute(sql_script)
        success_log(sql_script)
    except (IOError, OSError):
        print(IOError)
        error_log(IOError)


def generate_update_statement(table_name, cur):
    try:
        cur.execute("SELECT * FROM {t}".format(t=table_name))
        column_names = [description[0] for description in cur.description]
        for column in column_names:
            sql = f'update {table_name} set {column} = null where {column} = ""; '
            try:
                execute_statement(sql, cur)
            except (IOError, OSError):
                print(IOError)
                error_log(IOError)
    except (IOError, OSError):
        print(IOError)
        error_log(IOError)


with sqlite3.connect(path) as conn:
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        for tablerow in cursor.fetchall():
            tableName = tablerow[0]
            generate_update_statement(tableName, cursor)
    except (IOError, OSError):
        print(IOError)
        error_log(IOError)
