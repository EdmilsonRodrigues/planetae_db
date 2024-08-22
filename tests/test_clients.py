from src.planetae_db.client import MariaDBClient, MongoDBClient, MSSQLClient, MySQLClient, PostGresSQLClient, SQLite3Client
import pytest


@pytest.mark.parametrize(
    "Client",
    [
        MariaDBClient(username="root", password="", host="localhost", port=3306),
        MongoDBClient(),
        MySQLClient(),
        PostGresSQLClient(),
        MSSQLClient(),
        SQLite3Client(),
    ],
)
@pytest.mark.asyncio()
async def create_client(Client):
    client = Client
    assert client is not None