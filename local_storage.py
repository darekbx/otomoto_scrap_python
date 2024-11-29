import sqlite3
from config import dbFileLocation

class LocalStorage:
    
    __connection = None
    __transactionCursor = None
    __dbFile = dbFileLocation

    def __init__(self): 
        self.__connectDb()
        self.__createTable()

    def __connectDb(self):
        self.__connection = sqlite3.connect(self.__dbFile)
    
    def close(self):
        self.commitTransaction()
        self.__connection.close()

    def __createTable(self):
        cursor = self.__connection.cursor()
        cursor.execute('''
CREATE TABLE IF NOT EXISTS cars (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    externalId INTEGER NOT NULL UNIQUE,
    createdAt TEXT,
    url TEXT,
    price DOUBLE,
    currency TEXT,
    fuelType TEXT,
    gearbox TEXT,
    enginePower INTEGER,
    year INTEGER,
    countryOrigin TEXT,
    mileage INTEGER
)
''')
        cursor.close()

    def startTransaction(self):
        self.__transactionCursor = self.__connection.cursor()

    def commitTransaction(self):
        self.__connection.commit()
        if self.__transactionCursor:
            self.__transactionCursor.close()

    def addRecord(self, externalId, createdAt, url, price, currency, fuelType, gearbox, enginePower, year, countryOrigin, mileage):
        self.__transactionCursor.execute('''
INSERT OR IGNORE INTO cars (externalId, createdAt, url, price, currency, fuelType, gearbox, enginePower, year, countryOrigin, mileage)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
''', (externalId, createdAt, url, price, currency, fuelType, gearbox, enginePower, year, countryOrigin, mileage))

    def select(self):
        cursor = self.__connection.cursor()
        cursor.execute("SELECT * FROM cars")
        rows = cursor.fetchall()
        cursor.close()
        return rows
