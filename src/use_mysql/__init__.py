import logging
import time
from datetime import date, datetime

from _mysql_connector import MySQLInterfaceError
from mysql.connector.cursor import MySQLCursor
from mysql.connector.pooling import MySQLConnectionPool, PooledMySQLConnection

MAX_CONNECTION_ATTEMPTS = float("inf")  # 最大连接重试次数
MAX_CONNECTION_DELAY = 2**5  # 最大延迟时间

logger = logging.Logger(__name__)

DEFAULT_CURSOR_PARAMS = {
    "dictionary": True,  # 返回字典类型
}

_ = lambda key: f"`{key}`"


class MySQLStore:
    def __init__(
        self,
        *,
        host=None,
        user=None,
        password=None,
        port=None,
        cursor_params=None,
        **kwargs,
    ):
        """
        :param host: Mysql host
        :param port: Mysql port
        :param password: Mysql password
        :param kwargs: Mysql parameters
        """
        self.parameters = {
            "host": host or "localhost",
            "user": user or "root",
            "password": password or "root",
            "port": port or 3306,
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
                connector = MySQLConnectionPool(**self.parameters).get_connection()
                if not connector.is_connected():
                    raise Exception("MysqlStore connection error, not connected")
                if attempts > 1:
                    logger.warning(
                        f"MysqlStore connection succeeded after {attempts} attempts",
                    )
                return connector
            except Exception as exc:
                logger.warning(
                    f"MysqlStore connection error<{exc}>; retrying in {delay} seconds"
                )
                attempts += 1
                time.sleep(delay)
                if delay < MAX_CONNECTION_DELAY:
                    delay *= 2
                    delay = min(delay, MAX_CONNECTION_DELAY)

    @property
    def connection(self) -> PooledMySQLConnection:
        if self._connection is None or not self._connection.is_connected():
            self._connection = self._create_connection()
        return self._connection

    @connection.deleter
    def connection(self):
        self._close_cursor()
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
        self._close_cursor()

    def _close_cursor(self):
        if self._cursor:
            try:
                self._cursor.close()
            except Exception as exc:
                logger.exception(f"MySQLStore cursor close error <{exc}>")
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
                with self.cursor as cursor:
                    cursor.execute(sql, params)
                    self.connection.commit()
                    return cursor.lastrowid
            except MySQLInterfaceError:
                logger.exception(f"MysqlStore execute error<{sql}>")
                del self.connection
            except Exception as e:
                raise e


class ModelMetaClass(type):
    def __new__(cls, name, bases, attrs):
        super_new = super().__new__
        if name == "Model":
            return super_new(cls, name, bases, attrs)
        module = attrs.pop("__module__")
        new_attrs = {"__module__": module}
        for key, value in attrs.items():
            new_attrs[key] = value
        new_class = super_new(cls, name, bases, new_attrs)
        attr_meta = attrs.pop("Meta", None)
        meta = attr_meta or getattr(new_class, "Meta", None)

        new_class.db_table = (
            meta.db_table if hasattr(meta, "db_table") else name.lower()
        )
        new_class.connection = meta.connection if hasattr(meta, "connection") else None

        return new_class


class Model(metaclass=ModelMetaClass):
    def __init__(self):
        self._where_conditions = []
        self._insert_data = {}
        self._update_data = {}
        self._delete_flag = False

    @staticmethod
    def _format_value(value):
        if isinstance(value, str):
            return f"'{value}'"
        elif isinstance(value, date):
            return f"'{value.strftime('%Y-%m-%d')}'"
        elif isinstance(value, datetime):
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
        return value

    def where(self, **kwargs):
        for key, value in kwargs.items():
            if isinstance(value, dict):
                for operator, condition in value.items():
                    self._where_conditions.append(
                        f"{_(key)} {operator} {self._format_value(condition)}"
                    )
            elif isinstance(value, list):
                condition = ",".join(str(v) for v in value)
                self._where_conditions.append(f"{_(key)} IN ({condition})")
            else:
                self._where_conditions.append(f"{_(key)} = {self._format_value(value)}")

        return self

    def create(self, **kwargs):
        self._insert_data = kwargs
        return self

    def update(self, **kwargs):
        if not self._where_conditions:
            raise Exception("No conditions specified")

        self._update_data = kwargs
        return self

    def delete(self):
        if not self._where_conditions:
            raise Exception("No conditions specified")

        self._delete_flag = True
        return self

    @property
    def sql(self):
        if self._insert_data:
            keys = ",".join(_(key) for key in self._insert_data.keys())
            values = ",".join(
                f"{self._format_value(value)}" for value in self._insert_data.values()
            )
            sql = f"INSERT INTO {_(self.db_table)} ({keys}) VALUES ({values})"
            return sql

        if self._where_conditions:
            where_clause = " AND ".join(self._where_conditions)
            if self._update_data:
                set_values = ", ".join(
                    f"{_(key)} = '{value}'" for key, value in self._update_data.items()
                )
                return (
                    f"UPDATE {_(self.db_table)} SET {set_values} WHERE {where_clause}"
                )
            elif self._delete_flag:
                return f"DELETE FROM {_(self.db_table)} WHERE {where_clause}"
            else:
                return f"SELECT * FROM {_(self.db_table)} WHERE {where_clause}"
        else:
            return f"SELECT * FROM {_(self.db_table)}"

    def execute(self):
        logger.warning(self.sql)
        return self.connection.execute(self.sql)

    def all(self):
        with self.connection as cursor:
            cursor.execute(self.sql)
            result = cursor.fetchall()
            return result

    def one(self):
        with self.connection as cursor:
            cursor.execute(self.sql)
            result = cursor.fetchone()
            return result

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.sql}>"
