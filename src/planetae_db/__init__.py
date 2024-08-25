from importlib import import_module


MariaDBClient = import_module("src.planetae_db.client").MariaDBClient
MySQLClient = import_module("src.planetae_db.client").MySQLClient
SQLite3Client = import_module("src.planetae_db.client").SQLite3Client
MSSQLClient = import_module("src.planetae_db.client").MSSQLClient
PostGresSQLClient = import_module("src.planetae_db.client").PostGresSQLClient
MongoDBClient = import_module("src.planetae_db.client").MongoDBClient

# MariaDBDatabase = import_module("src.planetae_db.database").MariaDBDatabase
# MySQLDatabase = import_module("src.planetae_db.database").MySQLDatabase
# SQLite3Database = import_module("src.planetae_db.database").SQLite3Database
# MSSQLDatabase = import_module("src.planetae_db.database").MSSQLDatabase
# PostGresSQLDatabase = import_module("src.planetae_db.database").PostGresSQLDatabase
# MongoDBDatabase = import_module("src.planetae_db.database").MongoDBDatabase
#
# Table = import_module("src.planetae_db.table").Table
# Document = import_module("src.planetae_db.document").Document
#
# BaseClass = import_module("src.planetae_db.base").BaseClass
#
