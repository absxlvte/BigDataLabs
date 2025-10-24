import pymongo
import pandas as pd

class MongoLab:
    def __init__(self, uri, db_name,collection_name):
        self.client = pymongo.MongoClient(uri)
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
        result = list(self.collection.find({},{"_id":0}).limit(n))
        for item in result:
            print(item)
        return result
    def show_paginated(self,total=100,page_size=20):
        print(f"Первые {total} записей, страницами по {page_size} записей")
        for page_num in range(0, total, page_size):
            result = list(self.collection.find({},{"_id":0}).skip(page_num).limit(page_size))
            print(f"Страница {page_num // page_size + 1}:")
            for item in result:
                print(item)
        return result
    def find_with_criteria(self,field,condition,limit=100, sort_field = None, ascending = True):
        query = {field:condition}
        sort_by = sort_field if sort_field else field
        sort_order = 1 if ascending else -1
        try:
            result = self.collection.find(query,{"_id":0}).sort(sort_by,sort_order).limit(limit)
            for item in result:
                print(item)
            return list(result)
        except Exception as e:
            print(f'Ошибка при выполнении find_with_criteria: {e}')
            return []
    def find_with_in(self,field,values,projection=None, limit=100):
        if projection is None:
            projection = {"_id": 0}
        try:
            result = self.collection.find({field: {"$in": values}},projection).limit(limit)
            for item in result:
                print(item)
            return list(result)
        except Exception as e:
            print(f'Ошибка при выполнении find_with_in: {e}')
            return []
    def insert_record(self, record):
        try:
            result = self.collection.insert_one(record)
            print(f"Добавлена запись: {result}")
            for doc in self.collection.find().sort("_id", -1).limit(1):
                print(doc)
            return result.inserted_id
        except Exception as e:
            print(f"Ошибка при добавлении записи: {e}")
            return None
    def update_record(self, criteria, new_values, many=False):
        try:
            if many:
                result = self.collection.update_many(criteria, new_values)
            else:
                result = self.collection.update_one(criteria, new_values)
            print(f"Обновлено записей: {result.modified_count}")
            return result.modified_count
        except Exception as e:
            print(f"Ошибка при обновлении записей: {e}")
            return None

    def delete_records(self, criteria, many=False):
        try:
            if many:
                result = self.collection.delete_many(criteria)
            else:
                result = self.collection.delete_one(criteria)
            print(f"Удалено записей: {result.deleted_count}")
            return result.deleted_count
        except Exception as e:
            print(f"Ошибка при удалении записей: {e}")
            return None
    def delete_collection(self):
        self.collection.drop()
        print(f"Коллекция {self.collection.name} удалена ")
    def close(self):
        self.client.close()
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()


def main():
    with MongoLab("mongodb://student:Wd9hVzfB@82.148.28.116:27080","students","st5_medical_data_leaks") as lab:
        record = {
            "Name of Covered Entity": "St. Mary’s Medical Center",
            "State": "California",
            "Covered Entity Type": "Healthcare Provider",
            "Individuals Affected": 12845,
            "Breach Submission Date": "2024-11-15",
            "Type of Breach": "Hacking/IT Incident",
            "Location of Breached Information": "Network Server",
            "Business Associate Present": "True Health IT Solutions",
            "Web Description": (
                "An unauthorized party gained access to certain network servers "
                "containing patient information, including names, dates of birth, "
                "and limited medical data. The hospital implemented enhanced "
                "security measures and notified affected individuals."
            )
        }
        #lab.delete_collection()
        #lab.import_from_xls("Z:\Work\_Мага\Учеба\БигДата\BigDataLabs\lab6\Утечки мед данных.xls",limit=1000)
        #lab.show_first_n()
        #lab.show_paginated()
        #lab.find_with_criteria("Individuals Affected",{"$gte":100,"$lte":1000}, ascending=False)
        #lab.find_with_in("Covered Entity Type",["Health Plan", "Business Associate"],{"_id":0, "Covered Entity Type":1, "Name of Covered Entity":1, "Individuals Affected	":1})
        #lab.insert_record(record)
        #lab.find_with_criteria("State",{"$eq":"California"})
        #lab.update_record({"Type of Breach": "Hacking/IT Incident"}, {"$set": {"Type of Breach": "Cybersecurity Breach"}}, many=True)
        #lab.find_with_criteria("Type of Breach",{"$eq":"Cybersecurity Breach"},limit=5)
        #lab.delete_records({"Web Description": {"$eq": float('nan')} },many=True)

if __name__ == "__main__":
    main()