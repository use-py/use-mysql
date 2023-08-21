import datetime
import time

import pytest

from usepy_plugin_mysql import Mysql


@pytest.fixture()
def mysql():
    return Mysql(db="db")


def test_table(mysql):
    assert mysql.table("test")._table == "test"


@pytest.mark.parametrize("kwargs, where", [
    ({"id": 1}, ["`id` = '1'"]),
    ({"id": {"=": 1}}, ["`id` = 1"]),
    ({"id": {"!=": 1}}, ["`id` != 1"]),
    ({"id": [1, 2, 3]}, ["`id` IN (1,2,3)"]),
])
def test_where(mysql, kwargs, where):
    assert mysql.where(**kwargs)._where == where


def test_insert(mysql):
    print(mysql.table("user").insert(name="test"))
