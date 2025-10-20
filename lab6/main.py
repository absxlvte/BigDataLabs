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
        pass
    def show_paginated(self,total=100,page_size=20):
        pass
    def find_with_criteria(self, field,values,limit=100):
        pass
    def find_with_in(self,field,values,projection=None):
        pass
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
    """with MongoLab("st5_db","medical_data_leaks") as lab:
        #result = lab.import_from_xls("Утечки мед данных.xls",limit=2)
        lab.collection.insert_one({'qwe':123})"""
    """client = pymongo.MongoClient("mongodb://student:Wd9hVzfB@82.148.28.116:27080")
    database = client["bigdata"]
    collection = database["patients"]
    result = collection.insert_one({"name": "Ivan", "age": 21})
    print(result)"""

if __name__ == "__main__":
    main()