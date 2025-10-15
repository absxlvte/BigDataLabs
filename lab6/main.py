import pymongo

class MongoLab:
    def __int__(self, db_name,collection_name):
        self.client = pymongo.MongoClient("mongodb://student:Wd9hVzfB@82.148.28.116:27080")
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
    def import_from_csv(self, file_path, limit=1000):
        pass
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
        pass

def main():
    pass

if __name__ == "__main__":
    main()