import os
from datetime import datetime
from typing import Optional
from uuid import uuid4

import pytest

from use_mysql import Field, Model, MysqlStore


class TimestampMixin(Model):
    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)


class User(TimestampMixin, Model, table=True):
    __tablename__ = "test_users"  # pyright: ignore[reportAssignmentType]
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    mobile: str


@pytest.fixture(scope="session")
def mysql_uri() -> str:
    return os.getenv(
        "MYSQL_URI",
        "mysql+pymysql://root:root@localhost:3306/onestep",
    )


@pytest.fixture()
def store(mysql_uri: str):
    ms = MysqlStore(uri=mysql_uri, echo=False, generate_schemas=True)
    ms.init()
    try:
        yield ms
    finally:
        ms.shutdown()


def test_create_and_get_user(store: MysqlStore):
    mobile = f"{uuid4()}"
    u = store.create(User, name="Alice", mobile=mobile)
    got = store.get(User, id=u.id)
    assert got is not None
    assert got.id == u.id
    assert got.name == "Alice"
    assert got.mobile == mobile


def test_filter_users(store: MysqlStore):
    tag = f"{uuid4()}"
    store.create(User, name="Bob", mobile=tag)
    store.create(User, name="Carol", mobile=tag)
    rows = store.filter(User, mobile=tag)
    assert len(rows) >= 2
    names = {r.name for r in rows}
    assert {"Bob", "Carol"}.issubset(names)


def test_update_user(store: MysqlStore):
    u = store.create(User, name="Dave", mobile=f"{uuid4()}")
    updated = store.update(u, name="David")
    assert updated.name == "David"
    again = store.get(User, id=updated.id)
    assert again is not None
    assert again.name == "David"


def test_delete_user(store: MysqlStore):
    u = store.create(User, name="Eve", mobile=f"{uuid4()}")
    store.delete(u)
    assert store.get(User, id=u.id) is None


def test_create_many_users(store: MysqlStore):
    tag = f"{uuid4()}"
    rows = [
        {"name": "Frank", "mobile": tag},
        {"name": "Grace", "mobile": tag},
    ]
    created = store.create_many(User, rows)
    assert len(created) == 2
    ids = [r.id for r in created]
    assert all(i is not None for i in ids)
    fetched = store.filter(User, mobile=tag)
    names = {r.name for r in fetched}
    assert {"Frank", "Grace"}.issubset(names)
