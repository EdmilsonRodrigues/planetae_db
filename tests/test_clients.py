import os

import mariadb
import pymysql
from src.planetae_db.client import (
    MariaDBClient,
    MongoDBClient,
    MSSQLClient,
    MySQLClient,
    PostGresSQLClient,
    SQLite3Client,
)
import pytest

from src.planetae_db.database import Database


@pytest.fixture()
def mariadb_client():
    return MariaDBClient(
        username="root", password="", host="localhost", port=3306, logger_file=os.path.join("tests", "test.log")
    )


@pytest.fixture()
def xampp_mariadb_base_databases():
    return {"information_schema", "mysql", "performance_schema", "phpmyadmin"}


@pytest.fixture()
def mysql_client():
    return MySQLClient(
        username="root", password="", host="localhost", port=3306, logger_file=os.path.join("tests", "test.log")
    )


@pytest.fixture()
def database_name():
    return "test"


@pytest.mark.mariadb
@pytest.mark.asyncio()
async def test_create_MariaDB_client(mariadb_client):
    assert mariadb_client is not None


@pytest.mark.mariadb
@pytest.mark.asyncio()
async def test_get_mariadb_database_names(mariadb_client, xampp_mariadb_base_databases):
    databases = await mariadb_client.get_databases_names()
    assert databases is not None
    assert databases == xampp_mariadb_base_databases
    assert await mariadb_client.close()


@pytest.mark.mariadb
@pytest.mark.asyncio()
async def test_fail_to_get_mariadb_database(mariadb_client, database_name):
    mariadb_client.automatically_create_database = False
    with pytest.raises(mariadb.ProgrammingError):
        await mariadb_client.get_database(database_name)
    assert await mariadb_client.close()


@pytest.mark.mariadb
@pytest.mark.asyncio()
async def test_create_and_get_mariadb_database(mariadb_client, database_name):
    mariadb_client.automatically_create_database = True
    database = await mariadb_client.get_database(database_name)
    assert database is not None
    assert isinstance(database, Database)
    assert database.name == database_name
    assert await mariadb_client.close()


@pytest.mark.mariadb
@pytest.mark.asyncio()
async def test_fail_to_create_mariadb_database(mariadb_client, database_name):
    with pytest.raises(pymysql.ProgrammingError):
        await mariadb_client.create_database(database_name, exist_ok=False)
    assert await mariadb_client.close()


@pytest.mark.mariadb
@pytest.mark.asyncio()
async def test_do_not_create_mariadb_database(mariadb_client, database_name):
    assert await mariadb_client.create_database(database_name, exist_ok=True) is False
    assert await mariadb_client.close()


@pytest.mark.mariadb
@pytest.mark.asyncio()
async def test_delete_mariadb_database_0(mariadb_client, database_name):
    assert await mariadb_client.delete_database(database_name)
    assert await mariadb_client.close()


@pytest.mark.mariadb
@pytest.mark.asyncio()
async def test_create_mariadb_database(mariadb_client, database_name):
    assert await mariadb_client.create_database(database_name)
    assert await mariadb_client.close()


@pytest.mark.mariadb
@pytest.mark.asyncio()
async def test_get_mariadb_database_names_2(mariadb_client, xampp_mariadb_base_databases):
    databases = await mariadb_client.get_databases_names()
    assert databases is not None
    xampp_mariadb_base_databases.add("test")
    assert databases == xampp_mariadb_base_databases
    assert await mariadb_client.close()


@pytest.mark.mariadb
@pytest.mark.asyncio()
async def test_get_mariadb_database(mariadb_client, database_name):
    database = await mariadb_client.get_database(database_name)
    assert database is not None
    assert isinstance(database, Database)
    assert database.name == database_name
    assert await mariadb_client.close()


@pytest.mark.mariadb
def test_synchronously_get_mariadb_database(mariadb_client, database_name):
    database = mariadb_client[database_name]
    assert database is not None
    assert isinstance(database, Database)
    assert database.name == database_name


@pytest.mark.mariadb
@pytest.mark.asyncio()
async def test_delete_mariadb_database(mariadb_client, database_name):
    assert await mariadb_client.delete_database(database_name)
    assert await mariadb_client.close()


@pytest.mark.mariadb
@pytest.mark.asyncio()
async def test_synchronously_create_mariadb_database(mariadb_client, database_name):
    mariadb_client.automatically_create_database = True
    database = mariadb_client[database_name]
    assert database is not None
    assert isinstance(database, Database)
    assert database.name == database_name
    assert await mariadb_client.delete_database(database_name)
    assert await mariadb_client.close()


@pytest.mark.mariadb
@pytest.mark.asyncio()
async def test_iterate_through_mariadb_databases(mariadb_client):
    async for database in mariadb_client:
        assert database is not None
        assert isinstance(database, Database)
    assert await mariadb_client.close()


@pytest.mark.mariadb
@pytest.mark.asyncio()
async def test_close_mariadb_client(mariadb_client):
    await mariadb_client.close()
    assert await mariadb_client.close()


@pytest.mark.mariadb
@pytest.mark.asyncio()
async def test_integrated_mariadb_client_test(mariadb_client, database_name, xampp_mariadb_base_databases):
    databases = await mariadb_client.get_databases_names()
    assert databases is not None
    assert databases == xampp_mariadb_base_databases
    mariadb_client.automatically_create_database = True
    database = await mariadb_client.get_database(database_name)
    assert database is not None
    assert isinstance(database, Database)
    assert database.name == database_name
    assert await mariadb_client.delete_database(database_name)
    assert await mariadb_client.close()


@pytest.mark.mysql
@pytest.mark.asyncio()
async def test_create_mysql_client(mysql_client):
    assert mysql_client is not None
    assert await mysql_client.close()


@pytest.mark.mysql
@pytest.mark.asyncio()
async def test_get_mysql_database_names(mysql_client, xampp_mariadb_base_databases):
    databases = await mysql_client.get_databases_names()
    assert databases is not None
    assert databases == xampp_mariadb_base_databases
    assert await mysql_client.close()


@pytest.mark.mysql
@pytest.mark.asyncio()
async def test_fail_to_get_mysql_database(mysql_client, database_name):
    mysql_client.automatically_create_database = False
    with pytest.raises(mariadb.ProgrammingError):
        await mysql_client.get_database(database_name)
    assert await mysql_client.close()


@pytest.mark.mysql
@pytest.mark.asyncio()
async def test_create_and_get_mysql_database(mysql_client, database_name):
    mysql_client.automatically_create_database = True
    database = await mysql_client.get_database(database_name)
    assert database is not None
    assert isinstance(database, Database)
    assert database.name == database_name
    assert await mysql_client.close()


@pytest.mark.mysql
@pytest.mark.asyncio()
async def test_fail_to_create_mysql_database(mysql_client, database_name):
    with pytest.raises(pymysql.ProgrammingError):
        await mysql_client.create_database(database_name, exist_ok=False)
    assert await mysql_client.close()


@pytest.mark.mysql
@pytest.mark.asyncio()
async def test_do_not_create_mysql_database(mysql_client, database_name):
    assert await mysql_client.create_database(database_name, exist_ok=True) is False
    assert await mysql_client.close()


@pytest.mark.mysql
@pytest.mark.asyncio()
async def test_delete_mysql_database_0(mysql_client, database_name):
    assert await mysql_client.delete_database(database_name)
    assert await mysql_client.close()


@pytest.mark.mysql
@pytest.mark.asyncio()
async def test_create_mysql_database(mysql_client, database_name):
    assert await mysql_client.create_database(database_name)
    assert await mysql_client.close()


@pytest.mark.mysql
@pytest.mark.asyncio()
async def test_get_mysql_database_names_2(mysql_client, xampp_mariadb_base_databases):
    databases = await mysql_client.get_databases_names()
    assert databases is not None
    xampp_mariadb_base_databases.add("test")
    assert databases == xampp_mariadb_base_databases
    assert await mysql_client.close()


@pytest.mark.mysql
@pytest.mark.asyncio()
async def test_get_mysql_database(mysql_client, database_name):
    database = await mysql_client.get_database(database_name)
    assert database is not None
    assert isinstance(database, Database)
    assert database.name == database_name
    assert await mysql_client.close()


@pytest.mark.mysql
def test_synchronously_get_mysql_database(mysql_client, database_name):
    database = mysql_client[database_name]
    assert database is not None
    assert isinstance(database, Database)
    assert database.name == database_name


@pytest.mark.mysql
@pytest.mark.asyncio()
async def test_delete_mysql_database(mysql_client, database_name):
    assert await mysql_client.delete_database(database_name)
    assert await mysql_client.close()


@pytest.mark.mysql
@pytest.mark.asyncio()
async def test_synchronously_create_mysql_database(mysql_client, database_name):
    mysql_client.automatically_create_database = True
    database = mysql_client[database_name]
    assert database is not None
    assert isinstance(database, Database)
    assert database.name == database_name
    assert await mysql_client.delete_database(database_name)
    assert await mysql_client.close()


@pytest.mark.mysql
@pytest.mark.asyncio()
async def test_iterate_through_mysql_databases(mysql_client):
    async for database in mysql_client:
        assert database is not None
        assert isinstance(database, Database)
    assert await mysql_client.close()


@pytest.mark.mariadb
@pytest.mark.asyncio()
async def test_close_mysql_client(mysql_client):
    await mysql_client.close()
    assert await mysql_client.close()


@pytest.mark.mysql
@pytest.mark.asyncio()
async def test_integrated_mysql_client_test(mysql_client, database_name, xampp_mariadb_base_databases):
    databases = await mysql_client.get_databases_names()
    assert databases is not None
    assert databases == xampp_mariadb_base_databases
    mysql_client.automatically_create_database = True
    database = await mysql_client.get_database(database_name)
    assert database is not None
    assert isinstance(database, Database)
    assert database.name == database_name
    assert await mysql_client.delete_database(database_name)
    assert await mysql_client.close()
