import sqlite3
from typing import Callable
from pandas import read_sql_query


def read_scripts(cursor, scripts: list, many=True) -> None:
    all_scripts = []
    for sql_script_path in scripts:
        with open(sql_script_path, 'r') as sql_file:
            sql_script_content = sql_file.read()
        all_scripts.append(sql_script_content)
    run_scripts(all_scripts, cursor, many)


def run_scripts(tables: list, cursor, many=True) -> None:
    for table in tables:
        if many:
            cursor.executescript(table)
        else:
            cursor.execute(table)


def read_to_pd(db_loc: str, query):
    connection = sqlite3.connect(db_loc)
    df = read_sql_query(query, connection)
    connection.close()
    return df


def wrapper_con(db: str, func: Callable, *args) -> None:
    conn = sqlite3.connect(db)
    func(conn, *args)
    conn.close()


def wrapper_cursor(db: str, func: Callable, *args, **kwargs) -> None:
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    func(cursor, *args, **kwargs)
    conn.commit()
    conn.close()


def getpath(folder: str, seq: tuple) -> list:
    return [folder+'/'+x for x in seq]


def add_new_data_to_table(conn, df, tablename):
    df.to_sql(tablename, conn, if_exists='append', index=False)
