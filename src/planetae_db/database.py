from typing import Any, AsyncGenerator, Callable, Generator, Iterable
from planetae_logger import Logger
import mariadb

from src.planetae_db.table import Table


class Database:
    cursor: Any
    connection: Any
    name: str
    host: str | None = None
    port: int | None = None
    _tables: AsyncGenerator[Table | None, None] | None = None
    username: str | None = None
    password: str | None = None
    connection_string: str | None = None
    logger_file: str | None = None
    _logger: Logger | None = None

    def __init__(
        self,
        name: str,
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
        self.name = name

    @staticmethod
    def _get_items_from_signature(
        signature: dict,
    ) -> Generator[tuple[str, str], None, None]:
        for key, value in signature.items():
            yield key, value

    @staticmethod
    def not_implemented(default: Any = False):
        src.logger.log_exception(NotImplementedError)
        return default

    async def initialize(self):
        return self.not_implemented(None)

    async def get_all_tables(self) -> tuple[str]:
        return self.not_implemented(None)

    async def create_table(self, table_name: str, signature: dict) -> bool:
        return self.not_implemented()

    async def get_table_description(self, table_name: str) -> dict[str, str]:
        return self.not_implemented()

    async def add_column_to_table(
        self,
        table_name: str,
        signature: dict,
        after: str | None = None,
        default: str | None = None,
        first: bool = False,
    ) -> bool:
        return self.not_implemented()

    async def add_primary_key(self, table_name: str, key: str) -> bool:
        return self.not_implemented()

    async def remove_column_from_table(self, table_name: str, key: str) -> bool:
        return self.not_implemented()

    async def change_signature_from_column(self, table_name: str, signature: dict) -> bool:
        return self.not_implemented()

    async def rename_column(self, table_name: str, old_name: str, signature: dict) -> bool:
        return self.not_implemented()

    async def rename_table(self, old_table_name: str, new_table_name: str) -> bool:
        return self.not_implemented()

    async def delete_table(self, table_name: str) -> Any:
        return self.not_implemented()

    async def truncate_table(self, table_name: str) -> bool:
        return self.not_implemented()

    async def insert_document(
        self, table_name: str, document: dict[str, Any], return_query: bool = False
    ) -> bool | tuple[str, tuple]:
        return self.not_implemented()

    async def update_document(self, table_name: str, query: dict[str, Any], changes: dict[str, Any]) -> bool:
        return self.not_implemented()

    async def delete_document(self, table_name: str, query: dict[str, Any]) -> bool:
        return self.not_implemented()

    async def create_index(self, table_name: str, key: str) -> bool:
        return self.not_implemented()

    async def get_document(self, table_name: str, query: dict[str, Any]) -> dict[str, Any] | None:
        return self.not_implemented(None)

    async def get_documents(self, table_name: str, query: dict[str, Any]) -> list[dict[str, Any]]:  # type: ignore
        return self.not_implemented([])

    async def get_all_documents(self, table_name: str) -> list[dict[str, Any]]:
        return self.not_implemented([])

    async def backup_database(self, path: str, structure_only: bool = False, data_only: bool = False) -> bool:
        return self.not_implemented()

    async def restore_backup(self, path: str) -> bool:
        return self.not_implemented()

    async def delete_database(self) -> bool:
        return self.not_implemented()


class SQLDatabase(Database):
    db: mariadb.Connection
    cursor: mariadb.Cursor

    async def initialize(self):
        """
        Initializes the database returning the an instance of the subclass related to the database set in the configs
        """
        return await super().initialize()

    @staticmethod
    def _get_string_of_items_separated_by_comma(generator: Callable) -> Callable:
        """
        Generates a string of the keys, followed by the values, separated by commas and a new line.

        :param generator: A function that returns an iterator of key value couples
        :type generator: Callable

        :return: A function that generates a string of the keys, followed by the values, \
            separated by commas and a new line.
        :rtype: Callable
        """

        def wrapper(*args, **kwargs) -> Generator[str, None, None]:
            for key, value in generator(*args, **kwargs):
                yield f"{key} {value},\n"

        return wrapper

    @classmethod
    def _get_lines_of_items(cls, signature: dict) -> Generator[str, None, None]:
        return cls._get_string_of_items_separated_by_comma(generator=cls.get_items_from_signature)(signature=signature)

    async def _execute(self, query: str, string: str, values: tuple | None = None) -> bool:
        try:
            print(query)
            if values is None:
                self.cursor.execute(query)
            else:
                self.cursor.execute(query, values)
            src.logger.log_string(string)
            self.db.commit()
            src.logger.log_string("Committed changes.")
            return True
        except Exception as e:
            src.logger.log_exception(e)
            return False

    async def get_all_tables(self) -> tuple[str]:
        query = "SHOW TABLES;"
        get = await self._execute(query=query, string="Fetched all the tables of database.")
        if not get:
            src.logger.log_exception(ValueError)
        return self.cursor.fetchone()

    async def create_table(self, table_name: str, signature: dict[str, str], force: bool = False) -> bool:
        """
        Creates a table in a SQL Database.

        :param #! learn how to do this
        """
        query = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        index = None
        for line in self._get_lines_of_items(signature):
            if "PRIMARY KEY" in line:
                index = line.split()[0]
                line = line.replace("PRIMARY KEY,", ",")
            query += line
        if index is None:
            index = "id"
            query = query.split("(\n")
            id_index = "(\nid int NOT NULL AUTO_INCREMENT,\n"
            query = "".join((query[0], id_index, query[1]))
        query += f"PRIMARY KEY({index})"
        query += ") default charset=utf8mb4;"
        if force is True:
            await self.delete_table(table_name=table_name)
        return await self._execute(query=query, string=f"Created table {table_name}")

    async def get_table_description(self, table_name: str) -> dict[str, str]:
        query = f"DESCRIBE {table_name};"
        ex = await self._execute(query, string=f"Fetched description of table {table_name}.")
        if not ex:
            src.logger.log_exception(NotImplementedError)
        result = self.cursor.fetchall()
        return {field[0]: field[1] for field in result}

    async def add_column_to_table(
        self,
        table_name: str,
        signature: dict,
        after: str | None = None,
        default: str | None = None,
        first: bool = False,
    ) -> bool:
        sig = next(self._get_lines_of_items(signature))[:-2]
        af = ""
        fi = ""
        df = ""
        if after is not None:
            af += f" AFTER {after}"
        elif first:
            fi += " FIRST"
        if default:
            df += f" DEFAULT {default}"
        query = "ALTER TABLE " + table_name + " ADD COLUMN " + sig + af + fi + df + ";"
        return await self._execute(query=query, string=f"Column {sig} added to table {table_name}" + af + fi)

    async def add_primary_key(self, table_name: str, key: str) -> bool:
        query = "ALTER TABLE " + table_name + f" ADD PRIMARY KEY ({key});"
        return await self._execute(query=query, string=f"Added primary key {key} to table {table_name}.")

    async def remove_column_from_table(self, table_name: str, key: str) -> bool:
        query = "ALTER TABLE " + table_name + " DROP COLUMN " + key + ";"
        return await self._execute(query=query, string=f"column {key} dropped from the table.")

    async def change_signature_from_column(self, table_name: str, signature: dict) -> bool:
        line = next(self._get_lines_of_items(signature=signature)).replace(",", ";")
        query = "ALTER TABLE " + table_name + " MODIFY " + line
        return await self._execute(query=query, string=f"Column changed its signature:\nNew signature: {line}")

    async def rename_column(self, table_name: str, old_name: str, signature: dict) -> bool:
        line = next(self._get_lines_of_items(signature=signature)).replace(",", ";")
        query = "ALTER TABLE " + table_name + " CHANGE " + old_name + " " + line
        return await self._execute(
            query=query,
            string=f"Name of column changed from {old_name} to {line.split()[0]}",
        )

    async def rename_table(self, old_table_name: str, new_table_name: str) -> bool:
        query = "ALTER TABLE " + old_table_name + " RENAME TO " + new_table_name + ";"
        return await self._execute(query=query, string=f"Rename table {old_table_name} to {new_table_name}")

    async def delete_table(self, table_name: str) -> bool:
        query = f"DROP TABLE IF EXISTS {table_name};"
        return await self._execute(query=query, string=f"Dropped table {table_name} if table existed.")

    async def truncate_table(self, table_name: str) -> bool:
        query = f"TRUNCATE {table_name};"
        return await self._execute(query=query, string=f"Truncated table {table_name}.")

    async def insert_document(
        self, table_name: str, document: dict[str, Any], return_query: bool = False
    ) -> bool | tuple[str, tuple]:
        keys = self._get_keys_from_dict(document=document)
        values = self._get_values_tuple_from_dict(document=document)
        placeholders = self._get_string_with_placeholders_from_iterable(values)
        query = f"INSERT INTO {table_name} {keys} VALUES {placeholders};"
        if return_query:
            return query, values
        return await self._execute(
            query=query,
            values=values,
            string=f"Inserted {values} in table {table_name}.",
        )

    @staticmethod
    def _get_string_with_placeholders_from_iterable(iterable: Iterable) -> str:
        string = "("
        for _ in iterable:
            string += r"%s, "
        return string[:-2] + ")"

    @staticmethod
    def _gen_placeholder_query_or_set_string(query: dict[str, Any]) -> str:
        sets = ""
        for key in query.keys():
            sets += f"{key} = %s, "
        sets = sets[:-2]
        return sets

    @staticmethod
    def _add_limit(query: str, limit: int | None = None) -> str:
        if limit is not None:
            query = query[:-1] + " LIMIT " + str(limit) + ";"
        return query

    async def update_document(
        self,
        table_name: str,
        query: dict[str, Any],
        changes: dict[str, Any],
        limit: int | None = None,
    ) -> bool:
        queries = self._gen_placeholder_query_or_set_string(query=query)
        sets = self._gen_placeholder_query_or_set_string(query=changes)
        print(queries)
        print(sets)
        replacing_tuple = self._get_values_tuple_from_dict(changes) + self._get_values_tuple_from_dict(query)
        q = f"UPDATE {table_name} SET {sets} WHERE {queries};"
        q = self._add_limit(q, limit)
        return await self._execute(
            query=q,
            string=f"Updated table {table_name} with: {q}.",
            values=replacing_tuple,
        )

    async def delete_document(self, table_name: str, query: dict[str, Any], limit: int | None = None) -> Any:
        queries = self._gen_placeholder_query_or_set_string(query=query)
        queries_tuple = self._get_values_tuple_from_dict(query)
        q = f"DELETE FROM {table_name} WHERE {queries};"
        q = self._add_limit(q, limit=limit)
        return await self._execute(query=q, string=f"Deleted documents where {queries}", values=queries_tuple)

    async def create_index(self, table_name: str, key: str) -> Any:
        src.logger.log_exception(NotImplementedError)

    @staticmethod
    def _convert_tuple_to_dict(line: tuple, keys: Iterable) -> dict:
        return {key: value for key, value in zip(keys, line)}

    @staticmethod
    def _get_keys_from_dict(document: dict) -> str:
        string = "("
        for key in document.keys():
            string += str(key) + ", "
        return string[:-2] + ")"

    @staticmethod
    def _get_values_tuple_from_dict(document: dict) -> tuple:
        return tuple(document.values())

    async def get_document(self, table_name: str, query: dict[str, Any]) -> dict[str, Any] | None:
        queries = self._gen_placeholder_query_or_set_string(query=query)
        queries_values = self._get_values_tuple_from_dict(document=query)
        q = f"SELECT * FROM {table_name} WHERE {queries};"
        keys = await self._get_keys(table_name=table_name)
        ex = await self._execute(
            query=q,
            string=f"Fetched one document that matches {queries}, {queries_values}",
            values=queries_values,
        )
        if not ex:
            src.logger.log_exception(ValueError)
        results = self.cursor.fetchone()
        if results is None:
            return results
        return self._convert_tuple_to_dict(line=results, keys=keys)

    async def get_documents(self, table_name: str, query: dict[str, Any]) -> list[dict[str, Any]]:
        queries = self._gen_placeholder_query_or_set_string(query=query)
        queries_values = self._get_values_tuple_from_dict(document=query)
        q = f"SELECT * FROM {table_name} WHERE {queries};"
        ex = await self._execute(
            query=q,
            string=f"Fetched all documents from table {table_name} that meches {queries}, {queries_values}",
            values=queries_values,
        )
        if not ex:
            src.logger.log_exception(ValueError)
        results = self.cursor.fetchall()
        if results is None:
            return []
        return [
            self._convert_tuple_to_dict(line=result, keys=await self._get_keys(table_name=table_name))
            for result in results
        ]

    async def _get_keys(self, table_name: str):
        return (await self.get_table_description(table_name=table_name)).keys()

    async def get_all_documents(self, table_name: str) -> list[dict[str, Any]]:
        query = f"SELECT * FROM {table_name};"
        ex = await self._execute(query=query, string=f"Fetched all documents from table {table_name}")
        if not ex:
            src.logger.log_exception(ValueError)
        results = self.cursor.fetchall()
        if results is None:
            return []
        return [
            self._convert_tuple_to_dict(line=result, keys=await self._get_keys(table_name=table_name))
            for result in results
        ]

    async def _get_table_creation_command(self, table_name: str) -> str:
        query = f"SHOW CREATE TABLE {table_name};"
        ex = await self._execute(query=query, string=f"Got the commands to create table {table_name}")
        if not ex:
            src.logger.log_exception(ValueError)
        return self.cursor.fetchone()[1] + ";"

    async def _get_database_creation_command(self) -> str:
        query = "SHOW CREATE DATABASE planetae;"
        ex = await self._execute(query=query, string="Got the commands to create database")
        if not ex:
            src.logger.log_exception(ValueError)
        return self.cursor.fetchone()[1] + ";"

    async def backup_database(self, path: str, structure_only: bool = False, data_only: bool = False) -> bool:
        src.logger.log_string("Starting backup.")
        async with asyncio.TaskGroup() as tg:
            try:
                all_tables = await self.get_all_tables()
                src.logger.log_string("Fetching tables")
                tables_tasks = [
                    (
                        tg.create_task(self.get_all_documents(table_name=table_name)),
                        table_name,
                    )
                    for table_name in all_tables
                ]
                create_database_task = tg.create_task(self._get_database_creation_command())
                create_tables_tasks = [tg.create_task(self._get_table_creation_command(table)) for table in all_tables]
                src.logger.log_string("Fetching documments")

            except Exception as e:
                src.logger.log_exception(e)
                raise

            backup = "\n"

        if not data_only:
            backup += create_database_task.result() + "\n\n"

            for table_task in create_tables_tasks:
                table = table_task.result()
                backup += table + "\n\n"

            src.logger.log_string("Tables Fetched")

        if not structure_only:
            for table in tables_tasks:
                for document in table[0].result():
                    query = await self.insert_document(table_name=table[1], document=document, return_query=True)
                    if isinstance(query, bool):
                        src.logger.log_exception("Return query is not properly implemented")
                        raise
                    query = query[0] + "\n" + str(query[1])
                    backup += query

        os.makedirs(os.path.sep.join(path.split(os.path.sep)[:-1]), exist_ok=True)
        with open(path, "w") as backup_file:
            backup_file.write(backup)
        return True

    async def restore_backup(self, path: str) -> bool:
        await self.delete_database()
        with open(path, "r") as backup:
            queries = backup.read().split("\n\n")
        for index, query in enumerate(queries):
            if index == 0:
                await self._execute(query=query, string="Restoring database.")
                await self._execute("USE planetae;", string="Selecting database.")
            else:
                if query:
                    await self._execute(query=query, string="Restoring tables")
        return True


class MariaDBDatabase(SQLDatabase):
    def __init__(
        self,
        name: str,
        host: str,
        port: int,
        username: str,
        password: str,
        logger_file: str | None = None,
    ):
        super().__init__(
            name=name,
            host=host,
            port=port,
            username=username,
            password=password,
            logger_file=logger_file,
        )
        self.connection = mariadb.connect(
            user=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.name,
        )
        self.cursor = self.connection.cursor()


class NoSQLDatabase(Database):
    pass
