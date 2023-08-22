import sqlite3
from typing import Callable


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
