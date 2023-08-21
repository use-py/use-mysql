import datetime
import time

import pytest

from usepy_plugin_mysql import Model, MySQLStore


@pytest.fixture()
def mysql():
    return MySQLStore(db="db")


@pytest.fixture()
def model(mysql):
    class User(Model):
        class Meta:
            connection = mysql

    return User


def test_table(model):
    assert model.db_table == "user"


@pytest.fixture(scope="function")
def cleanup_where(model):
    model._where = []
    yield


@pytest.mark.parametrize("kwargs, where", [
    ({"id": 1}, ["`id` = '1'"]),
    ({"id": {"=": 1}}, ["`id` = 1"]),
    ({"id": {"!=": 1}}, ["`id` != 1"]),
    ({"id": [1, 2, 3]}, ["`id` IN (1,2,3)"]),
])
def test_where(cleanup_where, model, kwargs, where):
    assert model.where(**kwargs)._where == where


def test_create(model):
    assert model.create(name="qqqqq") > 0
