from typing import Any, List, Optional, Type, TypeVar

from sqlmodel import SQLModel, Session, select
from sqlmodel import create_engine

T = TypeVar("T", bound=SQLModel)

class MysqlStore:
    def __init__(
        self,
        uri: str,
        *,
        echo: Optional[bool] = False,
        generate_schemas: bool = False,
        **kwargs: Any,
    ) -> None:
        self._initialized = False
        self._engine = None
        self._db_url = uri
        self._echo = echo or False
        self._generate_schemas = generate_schemas
        self._kwargs = kwargs


    def init(self) -> None:
        self._engine = create_engine(self._db_url, echo=self._echo, **self._kwargs)
        if self._generate_schemas:
            SQLModel.metadata.create_all(self._engine)
        self._initialized = True

    def shutdown(self) -> None:
        if self._engine:
            self._engine.dispose()
        self._initialized = False

    def session(self) -> Session:
        if not self._engine:
            self.init()
        return Session(self._engine)

    def create(self, model: Type[T], **kwargs: Any) -> T:
        with self.session() as s:
            obj = model(**kwargs)
            s.add(obj)
            s.commit()
            s.refresh(obj)
            return obj

    def get(self, model: Type[T], **kwargs: Any) -> Optional[T]:
        with self.session() as s:
            if "id" in kwargs and len(kwargs) == 1:
                return s.get(model, kwargs["id"])
            stmt = select(model)
            for k, v in kwargs.items():
                stmt = stmt.where(getattr(model, k) == v)
            return s.exec(stmt).first()

    def filter(self, model: Type[T], **kwargs: Any) -> List[T]:
        with self.session() as s:
            stmt = select(model)
            for k, v in kwargs.items():
                stmt = stmt.where(getattr(model, k) == v)
            return list(s.exec(stmt).all())

    def update(self, instance: T, **kwargs: Any) -> T:
        with self.session() as s:
            for k, v in kwargs.items():
                setattr(instance, k, v)
            s.add(instance)
            s.commit()
            s.refresh(instance)
            return instance

    def delete(self, instance: T) -> None:
        with self.session() as s:
            s.delete(instance)
            s.commit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.shutdown()
        except Exception:
            pass
        return False


useMysql = MysqlStore
Model = SQLModel
__all__ = ["MysqlStore", "useMysql", "Model"]
