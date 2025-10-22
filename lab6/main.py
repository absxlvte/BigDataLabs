import pymongo
import pandas as pd

class MongoLab:
    def __init__(self, db_name,collection_name):
        self.client = pymongo.MongoClient("mongodb://student:Wd9hVzfB@82.148.28.116:27080")
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
    def import_from_xls(self, file_path, limit=1000):
        try:
            df = pd.read_excel(file_path, nrows=limit)
            df = df.where(pd.notnull(df), None)
            data = df.to_dict('records')
            if data:
                result = self.collection.insert_many(data[:limit])
                print(f"Успешно импортировано {len(result.inserted_ids)} записей")
                return result
            else:
                print("Нет данных для импорта")
                return None
        except FileNotFoundError:
            print(f"Файл {file_path} не найден")
            return None
        except Exception as e:
            print(f"Ошибка при импорте: {e}")
            return None
    def show_first_n(self,n=100):
        print(f"Первые {n} записей")
        result = self.collection.find().limit(n)
        for item in result:
            print(item)
    def show_paginated(self,total=100,page_size=20):
        print(f"Первые {total} записей, страницами по {page_size} записей")
        for page_num in range(0, total, page_size):
            result = self.collection.find().skip(page_num).limit(page_size)
            print(f"Страница {page_num // page_size + 1}:")
            for item in result:
                print(item)
    def find_with_criteria(self, field,values,limit=100):
        pass
    def find_with_in(self,field,values,projection=None):
        if projection is None:
            projection = {}
        result = self.collection.find({field: {"$in": values}},projection)
        for item in result:
            print(item)
    def insert_record(self):
        pass
    def update_record(self):
        pass
    def delete_records(self):
        pass
    def close(self):
        self.client.close()
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

def main():
    """with MongoLab("student","st5_medical_data_leaks") as lab:
        #result = lab.import_from_xls("Утечки мед данных.xls",limit=2)
        lab.collection.insert_one({'qwe':123})"""
    client = pymongo.MongoClient("mongodb://student:Wd9hVzfB@82.148.28.116:27080")
    database = client["student"]
    collection = database["patients"]
    print(database.list_collection_names())

    """result = collection.insert_one({"name": "Ivan", "age": 21})
    print(result)"""

if __name__ == "__main__":
    main()