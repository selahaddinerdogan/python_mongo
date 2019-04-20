from pymongo import MongoClient
from datetime import datetime
import os

""" 
1) haber_verileri adında veritabanı olusturulmali

2) veri adında collection olusturulmali

3) veri dosyalarının adresi 'Connection class' ının  'root_path' degiskeninden ayarlanmalıdır

"""


class DataProsesing:
    root_path = "/home/erdogan/Desktop/veri/prosesing/"
    @staticmethod
    def connect():
        client = MongoClient("localhost", 27017)
        db = client.get_database("haber_verileri")
        collection = db.veri
        return collection

    @staticmethod
    def read_file_insert_mongo():
        try:
            categories = ["ekonomi", "saglik", "teknoloji", "kultursanat", "politika", "spor"]
            collection = DataProsesing.connect()
            for category in categories:
                file_name = category + ".txt"
                file = open(DataProsesing.root_path+file_name, "r")
                file = file.read()
                text_row = file.split("\n")
                for text in text_row:
                    if len(text) > 0:
                        collection.insert({"category": category, "text": text})
        except Exception as ex:
            print("Error: ", ex)

    @staticmethod
    def select_mongo_write_file(data_limit):
        try:
            categories = ["ekonomi", "saglik", "teknoloji", "kultursanat", "politika", "spor"]
            collection = DataProsesing.connect()
            resources_folder = "filter_data"
            DataProsesing.create_folder(resources_folder)
            for category in categories:
                records = collection.find({"category": category}).limit(data_limit)
                for record in records:
                    file_name = category+".txt"
                    with open(DataProsesing.root_path+"/"+resources_folder+"/"+file_name, "a") as the_file:
                        the_file.write(record['text']+"\n")
                    print(record['category'], ": ", record['text'])

        except Exception as ex:
            print("Error: ", ex)

    @staticmethod
    def create_folder(folder_name):
        path = os.path.join(DataProsesing.root_path, folder_name)
        try:
            os.makedirs(path)
        except OSError:
            print("Creation of the directory %s failed" % path)
        else:
            print("Successfully created the directory %s" % path)


if __name__== "__main__":
    #DataProsesing.read_file_insert_mongo()
    DataProsesing.select_mongo_write_file(3)
