from abc import ABC, abstractmethod
import asyncio
import mysql.connector
import aiomysql
from src.planetae_db.database import Database
import mariadb
from typing import Any, AsyncGenerator
from planetae_logger import Logger


class Client(ABC):
    cursor: Any
    connection: Any
    host: str | None = None
    port: int | None = None
    _databases: set[str] | None = None
    username: str | None = None
    password: str | None = None
    connection_string: str | None = None
    logger_file: str | None = None
    _logger: Logger | None = None
    _automatically_create_database: bool = False

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        username: str | None = None,
        password: str | None = None,
        connection_string: str | None = None,
        logger_file: str | None = None,
        automatically_create_database: bool = False,
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
        if automatically_create_database:
            self._automatically_create_database = True

    @property
    def automatically_create_database(self) -> bool:
        return self._automatically_create_database

    @automatically_create_database.setter
    def automatically_create_database(self, value: bool):
        self._automatically_create_database = value

    def __aiter__(self):
        return self

    async def __anext__(self) -> Database | None:
        if self._databases is None:
            self._databases = await self.get_databases_names()
        try:
            database = self._databases.pop()
            return await self.get_database(database)
        except KeyError:
            raise StopAsyncIteration

    @abstractmethod
    def __getitem__(self, item: str) -> Database:
        pass

    @abstractmethod
    async def create_database(self, name: str, exist_ok: bool = True) -> bool:
        pass

    @abstractmethod
    async def get_databases(self) -> AsyncGenerator[Database | None, None]:
        yield

    @abstractmethod
    async def get_databases_names(self) -> set:
        pass

    @abstractmethod
    async def get_database(self, name: str) -> Database | None:
        pass

    @abstractmethod
    async def delete_database(self, name: str) -> bool:
        pass

    @classmethod
    def _get_database_class(cls) -> type:
        from importlib import import_module

        def get_database_class_name(cls):
            return cls.__name__.replace("Client", "Database")

        return getattr(import_module("src.planetae_db.database"), get_database_class_name(cls=cls))

    def close(self):
        self.connection.close()


class SQLClient(Client):
    cursor: Any
    connection: Any

    async def _execute(self, query: str, values: tuple | None = None, log: Any = None) -> bool:
        return self._execute_sync(query, values, log)

    async def _fetchone(self, query: str, values: tuple | None = None, log: Any = None) -> tuple:
        pass

    async def _fetchall(self, query: str, values: tuple | None = None, log: Any = None) -> list[tuple]:
        pass

    def _execute_sync(self, query: str, values: tuple | None = None, log: Any = None) -> bool:
        if log and self._logger:
            self._logger.info(log)
        try:
            if values:
                self.cursor.execute(query, values)
            self.cursor.execute(query)
            return True
        except Exception as e:
            if self._logger:
                self._logger.debug(str(e))
            raise

    def __getitem__(self, item: str) -> Database:
        try:
            database = self._get_database_class()
            return database(**self._get_credentials(), name=item)
        except Exception:
            if self.automatically_create_database:
                self.cursor.execute(f"CREATE DATABASE {item} DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
                return self[item]
            raise

    async def create_database(self, name: str, exist_ok: bool = True) -> bool:
        try:
            return await self._execute(
                f"CREATE DATABASE {name} DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
            )
        except mariadb.ProgrammingError as e:
            if exist_ok:
                if self._logger:
                    self._logger.info(f"Database {name} already exists.")
                return False
            raise e

    def _get_credentials(self) -> dict[str, str | int | None]:
        return {
            "username": self.username,
            "password": self.password,
            "host": self.host,
            "port": self.port,
        }

    async def get_database(self, name: str):
        try:
            database = self._get_database_class()
            return database(**self._get_credentials(), name=name)
        except Exception:
            if self.automatically_create_database:
                await self.create_database(name)
                return await self.get_database(name)
            raise

    async def get_databases(self) -> AsyncGenerator[Database | None, None]:
        databases = await self.get_databases_names()
        for database in databases:
            yield await self.get_database(database)

    async def get_databases_names(self) -> set:
        query = "SHOW DATABASES;"
        get = await self._execute(query=query, log="Fetched all the tables of database.")
        if not get:
            return set()
        return set(tup[0] for tup in self.cursor.fetchall())

    async def delete_database(self, name: str) -> bool:
        query = f"DROP DATABASE {name};"
        return await self._execute(query, log=f"Dropped database {name}.")


class MariaDBClient(SQLClient):
    cursor: aiomysql.Cursor
    connection: aiomysql.Connection

    def __init__(self, username: str, password: str, host: str, port: int, logger_file: str | None = None):
        super().__init__(username=username, password=password, host=host, port=port, logger_file=logger_file)
        self.connection = None  # type: ignore
        self.cursor = None  # type: ignore
        self._sync_connection = mysql.connector.connect(
            user=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
        )
        self._sync_cursor = self._sync_connection.cursor()

    async def _execute(self, query: str, values: tuple | None = None, log: Any = None) -> bool:
        if self.connection is None:
            assert self.username is not None and self.password is not None and self.host is not None and self.port is not None
            self.connection = await aiomysql.connect(
                user=self.username,
                password=self.password,
                host=self.host,
                port=self.port,
            )
        if log and self._logger:
            self._logger.info(log)
        try:
            async with self.connection.cursor() as cursor:
                if values:
                    await cursor.execute(query, values)
                else:
                    await cursor.execute(query)
            return True
        except Exception as e:
            if self._logger:
                self._logger.debug(str(e))
            raise
        
    def _execute_sync(self, query: str, values: tuple | None = None, log: Any = None) -> bool:
        if log and self._logger:
            self._logger.info(log)
        try:
            if values:
                self._sync_cursor.execute(query, values)
            self._sync_cursor.execute(query)
            return True
        except Exception as e:
            if self._logger:
                self._logger.debug(str(e))
            raise


class MySQLClient(MariaDBClient):
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

    async def create_database(self, name: str, exist_ok: bool = True) -> bool:
        return await self._execute(f"CREATE DATABASE {name} DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")

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
        get = await self._execute(query=query, log="Fetched all the tables of database.")
        if not get:
            return tuple()
        return self.cursor.fetchone()

    async def delete_database(self, name: str) -> bool:
        query = f"DROP DATABASE {name};"
        return self.cursor.execute(query)
