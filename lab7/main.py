import pymongo
class MongoLab:
    def __init__(self, uri, db_name):
        self.client = pymongo.MongoClient(uri)
        self.db = self.client[db_name]
    def close(self):
        self.client.close()
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

def main():
    with MongoLab("mongodb://student:Wd9hVzfB@82.148.28.116:27080","users") as lab:
        lab.db["equipments_used_by_doctors"].drop()
        lab.db.command(
            {
                "create":"equipments_used_by_doctors",
                "viewOn":"stage_doctors",
                "pipeline":[
                    {
                        "$lookup":{
                            "from":"stage_patients_procedures",
                            "localField":"_id",
                            "foreignField":"doctor_id",
                            "as":"patients_procedures"
                        }
                    },
                    {"$unwind": "$patients_procedures"},
                    {
                        "$lookup":{
                            "from": "stage_procedures",
                            "localField": "patients_procedures.procedure_id",
                            "foreignField": "_id",
                            "as": "procedures"
                        }
                    },
                    {"$unwind": "$procedures"},
                    {
                        "$lookup":{
                            "from": "stage_procedures_equipments",
                            "localField": "procedures._id",
                            "foreignField": "procedure_id",
                            "as": "procedures_equipment"
                        }
                    },
                    {"$unwind": "$procedures_equipment"},
                    {
                        "$lookup":{
                            "from": "stage_equipments",
                            "localField": "procedures_equipment.specialty_id",
                            "foreignField": "_id",
                            "as": "equipments"
                        }
                    },
                    {"$unwind": "$equipments"},
                    {
                        "$group": {
                            "_id": {"first_name": "$first_name", "last_name": "$last_name"},
                            "unique_equipments": {"$addToSet": "$equipments.name"}
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "first_name": "$_id.first_name",
                            "last_name": "$_id.last_name",
                            "num_unique_equipments": {"$size": "$unique_equipments"}
                        }
                    },
                    {
                        "$sort":{"num_unique_equipments":-1}
                    }
                ]
            }
        )
        if lab.db["equipments_used_by_doctors"].find():
            print("ok")
        else:
            print("bad")

if __name__ == "__main__":
    main()