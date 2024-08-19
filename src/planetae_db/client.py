from abc import ABC, abstractmethod
import asyncio
from database import Database
from typing import Any


class Client(ABC):
    cursor: Any = None
    databases: set[Database] | None = None

    def __init__(self, host: str, port: int, username: str | None = None, password: str | None = None):
        self.cursor = None
        self._databases = None
        self.host = host
        self.port = port
        self.username = username
        self.password = password

    @property
    def databases(self) -> set[Database]:
        if self._databases is None:
            self._databases = []
        return self._databases

    @databases.setter
    def databases(self, value: set[Database]):
        self._databases = value

    async def connect(self) -> None:
        await self._connect()
        self.databases = await self.get_databases()

    @abstractmethod
    async def _connect(self) -> None:
        pass

    @abstractmethod
    async def create_database(self, name: str) -> bool:
        pass

    @abstractmethod
    async def get_databases(self) -> list[Database]:
        pass

    @abstractmethod
    async def get_database(self, name: str) -> Database:
        pass

    @abstractmethod
    async def delete_database(self, name: str) -> bool:
        pass

    def __iter__(self):
        return iter(self.databases)

    def __next__(self):
        pass
