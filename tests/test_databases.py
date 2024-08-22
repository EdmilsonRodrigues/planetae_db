import os
import pytest
from src.session import MariaDB
from tests.fixtures import *


def remove_warning():
    return initial_signature()


@pytest.mark.parametrize(
    "main_db",
    [
        MariaDB
        # MySQL,
        # SQLite3,
        # MongoDB,
        # PostGresSQL,
        # MSSQL
    ],
)
@pytest.mark.asyncio()
async def test_DBInitialization(main_db):
    global db
    initialization = await main_db().initialize()
    if initialization is None:
        raise
    db = initialization
    assert isinstance(db, main_db)


@pytest.mark.clean_logs
def test_clean_logs(backup_file_path, backup_folder_path):
    with open("crash_log.txt", "w") as file:
        file.write("")
    with open("log.txt", "w") as file:
        file.write("")
    os.makedirs(backup_folder_path, exist_ok=True)
    if os.path.exists(backup_file_path):
        os.remove(backup_file_path)


@pytest.mark.asyncio()
async def test_get_no_tables():
    assert await db.get_all_tables() is None


@pytest.mark.asyncio()
async def test_create_table(initial_signature):
    global name
    name = "test"
    assert await db.create_table(
        table_name=name, signature=initial_signature, force=True
    )


@pytest.mark.asyncio()
async def test_get_tables():
    assert await db.get_all_tables() == ("test",)


@pytest.mark.asyncio()
async def test_get_test_table_description(initial_signature):
    global initial_description
    initial_description = {
        key: value.split(")")[0] + ")" for key, value in initial_signature.items()
    }
    initial_description.update({"id": "int(11)"})
    assert await db.get_table_description(name) == initial_description


@pytest.mark.asyncio()
async def test_add_column_to_table(new_collumns_signatures):
    assert await db.add_column_to_table(
        table_name=name, signature=new_collumns_signatures[0]
    )


@pytest.mark.asyncio()
async def test_change_column_signature(new_collumns_signatures):
    assert await db.change_signature_from_column(
        table_name=name, signature=new_collumns_signatures[1]
    )


@pytest.mark.asyncio()
async def test_change_column_completely(new_collumns_signatures):
    assert await db.rename_column(
        table_name=name, old_name="profession", signature=new_collumns_signatures[2]
    )


@pytest.mark.asyncio()
async def test_delete_new_column():
    assert await db.remove_column_from_table(table_name=name, key="favorite_food")


@pytest.mark.asyncio()
async def test_create_first_documment(documents_to_be_inserted):
    assert await db.insert_document(
        table_name=name, document=documents_to_be_inserted[0]
    )


@pytest.mark.asyncio()
async def test_change_name_from_first_document(documents_to_be_inserted):
    assert await db.update_document(
        table_name=name,
        query={"id": 1},
        changes={"name": documents_to_be_inserted[1]["name"]},
    )


@pytest.mark.asyncio()
async def test_get_first_document_updated(documents_to_be_inserted):
    document = documents_to_be_inserted[0]
    document.update({"id": 1, "name": documents_to_be_inserted[1]["name"]})
    assert await db.get_document(table_name=name, query={"id": 1}) == document


@pytest.mark.asyncio()
async def test_delete_first_document():
    assert await db.delete_document(table_name=name, query={"id": 1})


@pytest.mark.asyncio()
async def test_fail_to_get_first_document_updated():
    assert await db.get_document(table_name=name, query={"id": 1}) is None


@pytest.mark.asyncio()
async def test_delete_primary_key():
    assert await db.remove_column_from_table(table_name=name, key="id")


@pytest.mark.asyncio()
async def test_get_test_table_description_without_id(initial_signature):
    initial_description = {
        key: value.split(")")[0] + ")" for key, value in initial_signature.items()
    }
    assert await db.get_table_description(name) == initial_description


@pytest.mark.asyncio()
async def test_create_primary_key():
    assert await db.add_primary_key(table_name=name, key="phone")


@pytest.mark.asyncio()
async def test_create_backup_from_database(backup_folder_path, backup_file_path):
    assert await db.backup_database(path=backup_file_path)
    assert len(os.listdir(backup_folder_path)) > 0
    assert os.path.exists(backup_file_path)


@pytest.mark.asyncio()
async def test_truncate_table():
    assert await db.truncate_table(table_name=name)


@pytest.mark.asyncio()
async def test_delete_mariadb_table():
    assert await db.delete_table(table_name=name)


@pytest.mark.asyncio()
async def test_restore_backup(initial_signature, backup_file_path):
    initial_description = {
        key: value.split(")")[0] + ")" for key, value in initial_signature.items()
    }
    assert await db.restore_backup(path=backup_file_path)
    assert await db.get_table_description(name) == initial_description
    assert await db.delete_table(table_name=name)


@pytest.mark.asyncio()
async def test_delete_database():
    assert await db.delete_database()
