'''
Requirements

Requests: python -m pip install requests
Firebase: python -m pip install --upgrade firebase-admin
'''

import requests
import json
from datetime import datetime

from local_storage import LocalStorage
from remote_storage import RemoteStorage
from config import requestLocation

class OtomotoScrap:

    __request = requestLocation
    __url = 'https://www.otomoto.pl/graphql'
    __headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:132.0) Gecko/20100101 Firefox/132.0",
        "Accept": "application/graphql-response+json, application/graphql+json, application/json, text/event-stream, multipart/mixed",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "content-type": "application/json",
    }

    __storage = RemoteStorage()
    __latestCreatedAt = None

    def print(self):
        print(self.__storage.select())

    def scrap(self):
        print("Start")

        with open(self.__request, "r") as file:
            requestData = json.load(file)

        self.__latestCreatedAt = self.__storage.fetchLastCreatedAt()
        page = 1

        while True:
            # update page in request data
            requestData["variables"]["page"] = page

            response = requests.post(self.__url, json = requestData, headers = self.__headers)
            jsonData = response.json()["data"]["advertSearch"]
            
            totalCount = jsonData["totalCount"]
            pageSize = jsonData["pageInfo"]["pageSize"]
            currentOffset = jsonData["pageInfo"]["currentOffset"]

            print(f"Read page {page}")

            self.__storage.startTransaction()
            self.__readContents(jsonData)
            self.__storage.commitTransaction()

            page = page + 1
            if currentOffset + pageSize > totalCount:
                break

        print("Done!")
            
        self.__storage.close()
    
    def __readContents(self, json):
        nodes = json["edges"]
        for nodeWrapper in nodes:
            node = nodeWrapper["node"]
            id = node["id"]
            createdAt = node["createdAt"]
            url = node["url"]
            price = node["price"]["amount"]["value"]
            currency = node["price"]["amount"]["currencyCode"]
            
            parameters = node["parameters"]
            fuelType = self.__readParameter(parameters, "fuel_type")
            gearBox = self.__readParameter(parameters, "gearbox")
            enginePower = self.__readParameter(parameters, "engine_power")
            year = self.__readParameter(parameters, "year")
            countryOrigin = self.__readParameter(parameters, "country_origin")
            mileage = self.__readParameter(parameters, "mileage")

            createdAtDateTime = datetime.strptime(createdAt, "%Y-%m-%d %H:%M:%S")
            if createdAtDateTime.timestamp() > self.__latestCreatedAt.timestamp():
                self.__storage.addRecord(id, createdAt, url, price, currency, fuelType, gearBox, enginePower, year, countryOrigin, mileage)

    def __readParameter(self, parameters, key):
        return next((param["value"] for param in parameters if param["key"] == key), None)

OtomotoScrap().scrap()
