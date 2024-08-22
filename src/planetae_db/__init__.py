from importlib import import_module


MariaDBClient = import_module("src.planetae_db.client").MariaDBClient
MySQLClient = import_module("src.planetae_db.client").MySQLClient
SQLite3Client = import_module("src.planetae_db.client").SQLite3Client
MSSQLClient = import_module("src.planetae_db.client").MSSQLClient
PostGresSQLClient = import_module("src.planetae_db.client").PostGresSQLClient
MongoDBClient = import_module("src.planetae_db.client").MongoDBClient

database = import_module("src.planetae_db.database")
