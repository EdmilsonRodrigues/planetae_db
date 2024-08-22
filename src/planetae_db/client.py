from abc import ABC, abstractmethod
import asyncio
from database import Database
import mariadb
import mysql
from typing import Any, AsyncGenerator, Coroutine
from planetae_logger import Logger


class Client(ABC):
    cursor: Any
    connection: Any
    host: str | None = None
    port: int | None = None
    _databases: AsyncGenerator[Database | None, None] | None = None
    username: str | None = None
    password: str | None = None
    connection_string: str | None = None
    logger_file: str | None = None
    _logger: Logger | None = None

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        username: str | None = None,
        password: str | None = None,
        connection_string: str | None = None,
        logger_file: str | None = None,
    ):
        self._databases = None
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.connection_string = connection_string
        self.logger_file = logger_file
        if logger_file:
            self._logger = Logger("Client", log_file=logger_file)

    async def __aiter__(self):
        self._databases = self.get_databases()
        async for database in self._databases:
            yield database

    @abstractmethod
    async def create_database(self, name: str) -> bool:
        pass

    @abstractmethod
    async def get_databases(self) -> AsyncGenerator[Database | None, None]:
        yield

    @abstractmethod
    async def get_databases_names(self) -> tuple:
        pass

    @abstractmethod
    async def get_database(self, name: str) -> Database | None:
        pass

    @abstractmethod
    async def delete_database(self, name: str) -> bool:
        pass

    def close(self):
        self.connection.close()


class SQLClient(Client):
    cursor: Any
    connection: Any

    async def _execute(self, query: str, values: tuple | None = None, log: Any = None) -> bool:
        if log and self._logger:
            self._logger.info(log)
        try:
            if values:
                self.cursor.execute(query, values)
            return self.cursor.execute(query)
        except Exception as e:
            if self._logger:
                self._logger.debug(str(e))
            raise

    async def create_database(self, name: str) -> bool:
        return await self._execute(
            f"CREATE DATABASE {name} DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
        )

    def _get_credentials(self) -> dict[str, str | int | None]:
        return {
            "username": self.username,
            "password": self.password,
            "host": self.host,
            "port": self.port,
        }

    async def get_database(self, name: str) -> Database | None:
        dbs = await self.get_databases_names()
        if name not in dbs:
            return None
        return Database(**self._get_credentials(), name=name)

    async def get_databases(self) -> AsyncGenerator[Database | None, None]:
        databases = await self.get_databases_names()
        for database in databases:
            yield await self.get_database(database)

    async def get_databases_names(self) -> tuple:
        query = "SHOW DATABASES;"
        get = await self._execute(
            query=query, log="Fetched all the tables of database."
        )
        if not get:
            return tuple()
        return self.cursor.fetchone()

    async def delete_database(self, name: str) -> bool:
        query = f"DROP DATABASE {name};"
        return self.cursor.execute(query)


class MariaDBClient(SQLClient):
    cursor: mariadb.Cursor | None = None
    connection : mariadb.Connection | None = None

    def __init__(self, username: str, password: str, host: str, port: int, logger_file: str | None= None):
        super().__init__(username=username, password=password, host=host, port=port, logger_file=logger_file)
        self.connection = mariadb.connect(
            user=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
        )
        self.cursor = self.connection.cursor()


class MySQLClient(SQLClient):
    pass


class SQLite3Client(SQLClient):
    pass


class MSSQLClient(SQLClient):
    pass


class PostGresSQLClient(SQLClient):
    pass

class NoSQLClient(Client):
    pass


class MongoDBClient(NoSQLClient):
    cursor: Any
    connection: Any

    async def _execute(self, query: str, values: tuple | None = None, log: Any = None) -> bool:
        if log and self._logger:
            self._logger.info(log)
        try:
            if values:
                self.cursor.execute(query, values)
            return self.cursor.execute(query)
        except Exception as e:
            if self._logger:
                self._logger.debug(str(e))
            raise

    async def create_database(self, name: str) -> bool:
        return await self._execute(
            f"CREATE DATABASE {name} DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
        )

    def _get_credentials(self) -> dict[str, str | int | None]:
        return {
            "username": self.username,
            "password": self.password,
            "host": self.host,
            "port": self.port,
        }

    async def get_database(self, name: str) -> Database | None:
        dbs = await self.get_databases_names()
        if name not in dbs:
            return None
        return Database(**self._get_credentials(), name=name)

    async def get_databases(self) -> AsyncGenerator[Database | None, None]:
        databases = await self.get_databases_names()
        for database in databases:
            yield await self.get_database(database)

    async def get_databases_names(self) -> tuple:
        query = "SHOW DATABASES;"
        get = await self._execute(
            query=query, log="Fetched all the tables of database."
        )
        if not get:
            return tuple()
        return self.cursor.fetchone()

    async def delete_database(self, name: str) -> bool:
        query = f"DROP DATABASE {name};"
        return self.cursor.execute(query)
