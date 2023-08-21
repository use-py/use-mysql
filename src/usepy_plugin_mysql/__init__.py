import logging

import time
from contextlib import contextmanager

from _mysql_connector import MySQLInterfaceError

from mysql.connector.cursor import MySQLCursor
from mysql.connector.pooling import MySQLConnectionPool, PooledMySQLConnection

MAX_CONNECTION_ATTEMPTS = float('inf')  # 最大连接重试次数
MAX_CONNECTION_DELAY = 2 ** 5  # 最大延迟时间

logger = logging.Logger(__name__)

DEFAULT_CURSOR_PARAMS = {
    "dictionary": True,  # 返回字典类型
}

_ = lambda key: f"`{key}`"


class MysqlStore:

    def __init__(self, *, host=None, user=None, password=None, port=None, cursor_params=None, **kwargs):
        """
        :param host: Mysql host
        :param port: Mysql port
        :param password: Mysql password
        :param kwargs: Mysql parameters
        """
        self.parameters = {
            'host': host or 'localhost',
            'user': user or 'root',
            'password': password or 'root',
            'port': port or 3306,
        }
        if kwargs:
            self.parameters.update(kwargs)
        self.cursor_params = cursor_params or DEFAULT_CURSOR_PARAMS
        self._connection = None
        self._cursor: MySQLCursor = None

    def _create_connection(self):
        attempts = 1
        delay = 1
        while attempts <= MAX_CONNECTION_ATTEMPTS:
            try:
                connector = MySQLConnectionPool(
                    **self.parameters
                ).get_connection()
                if not connector.is_connected():
                    raise Exception("MysqlStore connection error, not connected")
                if attempts > 1:
                    logger.warning(f"MysqlStore connection succeeded after {attempts} attempts", )
                return connector
            except Exception as exc:
                logger.warning(f"MysqlStore connection error<{exc}>; retrying in {delay} seconds")
                attempts += 1
                time.sleep(delay)
                if delay < MAX_CONNECTION_DELAY:
                    delay *= 2
                    delay = min(delay, MAX_CONNECTION_DELAY)

    @property
    def connection(self) -> PooledMySQLConnection:
        if self._connection is None:
            self._connection = self._create_connection()
        return self._connection

    @connection.deleter
    def connection(self):
        del self.cursor
        if self._connection:
            try:
                self._connection.close()
            except Exception as exc:
                logger.exception(f"MysqlStore connection close error<{exc}>")
            self._connection = None

    @property
    def cursor(self) -> MySQLCursor:
        if self._cursor is None:
            self._cursor = self.connection.cursor(**self.cursor_params)
        return self._cursor

    @cursor.deleter
    def cursor(self):
        if self._cursor:
            try:
                self._cursor.close()
            except Exception as exc:
                logger.exception(f"MysqlStore cursor close error<{exc}>")
            self._cursor = None

    def __delete__(self, instance):
        del self.connection

    def __enter__(self):
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        del self.cursor

    def execute(self, sql, params=None):
        while True:
            try:
                self.cursor.execute(sql, params)
                self.connection.commit()
                return self.cursor.lastrowid
            except MySQLInterfaceError:
                logger.exception(f"MysqlStore execute error<{sql}>")
                del self.connection
            except Exception as e:
                raise e


class Mysql(MysqlStore):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self._table = None
        self._where = []

    def table(self, table_name):
        self._table = table_name
        return self

    def where(self, **kwargs):
        for key, value in kwargs.items():
            if isinstance(value, dict):
                for operator, condition in value.items():
                    self._where.append(f"{_(key)} {operator} {condition}")
            elif isinstance(value, list):
                condition = ",".join(str(v) for v in value)
                self._where.append(f"{_(key)} IN ({condition})")
            else:
                self._where.append(f"{_(key)} = '{value}'")

        return self

    def insert(self, **kwargs):
        keys = ",".join(_(key) for key in kwargs.keys())
        values = ",".join(f"'{value}'" for value in kwargs.values())
        sql = f"INSERT INTO {_(self._table)} ({keys}) VALUES ({values})"
        return self.execute(sql)

    def update(self, **kwargs):
        if not self._table:
            raise Exception("Table not specified")

        if not self._where:
            raise Exception("No conditions specified")

        set_values = ", ".join(f"{_(key)} = '{value}'" for key, value in kwargs.items())
        where_clause = " AND ".join(self._where)
        sql = f"UPDATE {_(self._table)} SET {set_values} WHERE {where_clause}"
        return self.execute(sql)


if __name__ == '__main__':
    ms = Mysql(db="db")
    # print(ms.table("user").insert(name="test"))

    print(ms.table("user").where().update(name="1231"))
