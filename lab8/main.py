import psycopg2
import pymongo
import json
import random
import time
from tqdm import tqdm
from faker import Faker
import bcrypt
import hashlib
from abc import ABC, abstractmethod
import matplotlib.pyplot as plt

fake = Faker("en_US")
fake_num = Faker("ru_RU")
class DataBase(ABC):
    @abstractmethod
    def create_table(self):
        pass
    @abstractmethod
    def insert_users(self,count):
        pass
    @abstractmethod
    def create_indexes(self):
        pass
    @abstractmethod
    def delete_indexes(self):
        pass
    @abstractmethod
    def find_user(self,login):
        pass
    @abstractmethod
    def drop_table(self):
        pass
    def generate_user(self):
        login = ""
        while len(login) < 5:
            login = fake.word()
        return {
            "login" : login,
            #"password" : bcrypt.hashpw(fake.password().encode('utf-8'), bcrypt.gensalt()).decode('utf-8'),
            "password" : hashlib.sha256(fake.password().encode('utf-8')).hexdigest(),
            "email" : fake.email(),
            "phone_number" : fake_num.phone_number(),
            "is2FA" : random.choice([0,1])
        }

class PostgreTable(DataBase):
    def __init__(self, connection):
        self.con = psycopg2.connect(connection)
        self.cur = self.con.cursor()
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cur.close()
        self.con.close()
    def __enter__(self):
        return self
    def create_table(self):
        self.cur.execute("""
                    CREATE TABLE IF NOT EXISTS st5_users (
                        id SERIAL PRIMARY KEY,
                        login TEXT,
                        password TEXT,
                        email TEXT,
                        phone_number TEXT,
                        is2FA BOOLEAN
                    );
                """)
        self.con.commit()
        print("PostgreSQL: таблица st5_users создана (или уже существует)")

    def insert_users(self, count):
        with tqdm(total=count, desc="Подготовка данных") as pbar:
            users = []
            for _ in range(count):
                user = self.generate_user()
                users.append((user["login"], user["password"], user["email"], user["phone_number"], bool(user["is2FA"])))
                pbar.update(1)
        with tqdm(total=1, desc="Вставка в БД") as pbar:
            self.cur.executemany("""
                INSERT INTO st5_users (login, password, email, phone_number, is2FA)
                VALUES (%s, %s, %s, %s, %s);
            """, users)
            self.con.commit()
            pbar.update(1)
        print(f"PostgreSQL: вставлено {count} записей")

    def find_user(self, login):
        t0 = time.time()
        self.cur.execute("SELECT * FROM st5_users WHERE login = %s;", (login,))
        result = self.cur.fetchone()
        elapsed = time.time() - t0
        print(f"PostgreSQL: время поиска: {elapsed*1000:.3f} мс")
        print(*result)
        explain_modes = [
            ("EXPLAIN", "EXPLAIN SELECT * FROM st5_users WHERE login = %s;"),
            ("EXPLAIN ANALYZE", "EXPLAIN ANALYZE SELECT * FROM st5_users WHERE login = %s;"),
            ("EXPLAIN (ANALYZE, BUFFERS)", "EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM st5_users WHERE login = %s;")
        ]
        for mode_name, query in explain_modes:
            print(f"\n{mode_name}:")
            self.cur.execute(query, (login,))
            plan = self.cur.fetchall()
            for line in plan:
                print(" ", line[0])

        return result

    def create_indexes(self):
        self.cur.execute("CREATE INDEX IF NOT EXISTS idx_st5_users_login ON st5_users(login);")
        self.con.commit()
        print("PostgreSQL: индекс idx_st5_users_login создан")
    def delete_indexes(self):
        self.cur.execute("DROP INDEX IF EXISTS idx_st5_users_login;")
        self.con.commit()
        print("PostgreSQL: индекс idx_st5_users_login удален")
    def drop_table(self):
        self.cur.execute("DROP TABLE IF EXISTS st5_users")
    def close(self):
        self.cur.close()
        self.con.close()

class MongoCollection(DataBase):
    def __init__(self,uri, db_name,collection_name):
        self.client = pymongo.MongoClient(uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()
    def __enter__(self):
        return self
    def close(self):
        self.client.close()
    def create_table(self):
        print("MongoDB: коллекция st5_users готова")
    def insert_users(self,count):
        with tqdm(total=count, desc="Подготовка данных") as pbar:
            users = []
            for _ in range(count):
                user = self.generate_user()
                users.append(user)
                pbar.update(1)
        with tqdm(total=1, desc="Вставка в БД") as pbar:
            self.collection.insert_many(users)
            pbar.update(1)
        print(f"MongoDB: вставлено {count} записей")
    def find_user(self,login):
        t0 = time.time()
        result = self.collection.find_one({"login": login})
        elapsed = time.time() - t0
        print(f"MongoDB: Время поиска: {elapsed*1000:.3f} мс")
        explain = self.collection.find({"login": login}).explain()
        print("\n explain (executionStats):")
        print(json.dumps(explain["executionStats"], indent=4))
        if result:
            print("\n Найден документ:", {k: result[k] for k in ['login', 'email', 'is2FA']})
        else:
            print("\n Документ не найден")
    def create_indexes(self):
        self.collection.create_index("login")
        print("MongoDB: индекс по login создан")
    def delete_indexes(self):
        self.collection.drop_index("login_1")
        print("MongoDB: индекс по login удален")
    def drop_table(self):
        self.collection.drop()

def benchmark_postgres_search():
    with PostgreTable("host=82.148.28.116 user=student password=Wd9hVzfB dbname=student") as pg:
        pg.cur.execute("SELECT login FROM st5_users OFFSET floor(random() * (SELECT COUNT(*) FROM st5_users)) LIMIT 1;")
        login = pg.cur.fetchone()[0]
        print(f"\nТестируем поиск по логину: '{login}'\n")
        pg.delete_indexes()
        times_no_index = []
        for _ in range(5):
            t0 = time.time()
            pg.cur.execute("SELECT * FROM st5_users WHERE login = %s;", (login,))
            pg.cur.fetchone()
            times_no_index.append(time.time() - t0)
        avg_no_index = sum(times_no_index) / len(times_no_index)
        print("\nEXPLAIN (без индекса):")
        pg.cur.execute("EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM st5_users WHERE login = %s;", (login,))
        for line in pg.cur.fetchall():
            print(" ", line[0])
        pg.create_indexes()
        times_with_index = []
        for _ in range(5):
            t0 = time.time()
            pg.cur.execute("SELECT * FROM st5_users WHERE login = %s;", (login,))
            pg.cur.fetchone()
            times_with_index.append(time.time() - t0)
        avg_with_index = sum(times_with_index) / len(times_with_index)
        print("\nEXPLAIN (с индексом):")
        pg.cur.execute("EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM st5_users WHERE login = %s;", (login,))
        for line in pg.cur.fetchall():
            print(" ", line[0])
        labels = ['Без индекса', 'С индексом']
        avg_times = [avg_no_index * 1000, avg_with_index * 1000]  # мс
        plt.bar(labels, avg_times, color=['red', 'green'])
        plt.title("Сравнение времени поиска в PostgreSQL")
        plt.ylabel("Время (мс)")
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        print(f"\nСреднее время поиска без индекса: {avg_no_index*1000:.3f} мс")
        print(f"Среднее время поиска с индексом: {avg_with_index*1000:.3f} мс")
        print(f"Ускорение: {avg_no_index / avg_with_index:.2f}x")
        plt.show()

def benchmark_mongo_search():
    with MongoCollection("mongodb://student:Wd9hVzfB@82.148.28.116:27080", "students", "st5_users") as mn:
        sample = mn.collection.aggregate([{"$sample": { "size": 1 }}])
        login = next(sample)["login"]
        print(f"\nТестируем поиск по логину: '{login}'\n")
        try:
            mn.delete_indexes()
        except Exception:
            pass
        times_no_index = []
        for _ in range(5):
            t0 = time.time()
            mn.collection.find_one({"login": login})
            times_no_index.append(time.time() - t0)
        avg_no_index = sum(times_no_index) / len(times_no_index)
        explain_no_index = mn.collection.find({"login": login}).explain()
        print("\n EXPLAIN (без индекса):")
        print(json.dumps(explain_no_index["executionStats"], indent=4, ensure_ascii=False))
        mn.create_indexes()
        times_with_index = []
        for _ in range(5):
            t0 = time.time()
            mn.collection.find_one({"login": login})
            times_with_index.append(time.time() - t0)
        avg_with_index = sum(times_with_index) / len(times_with_index)
        explain_with_index = mn.collection.find({"login": login}).explain()
        print("\n EXPLAIN (с индексом):")
        print(json.dumps(explain_with_index["executionStats"], indent=4, ensure_ascii=False))
        labels = ['Без индекса', 'С индексом']
        avg_times = [avg_no_index * 1000, avg_with_index * 1000]  # мс
        plt.bar(labels, avg_times, color=['red', 'green'])
        plt.title("Сравнение времени поиска в MongoDB")
        plt.ylabel("Время (мс)")
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        print(f"\nСреднее время поиска без индекса: {avg_no_index*1000:.3f} мс")
        print(f"Среднее время поиска с индексом: {avg_with_index*1000:.3f} мс")
        print(f"Ускорение: {avg_no_index / avg_with_index:.2f}x")
        plt.show()

if __name__ == "__main__":
    with PostgreTable("host=82.148.28.116 user=student password=Wd9hVzfB dbname=student") as pg:
        #pg.create_table()
        #pg.insert_users(1000)
        #pg.drop_table()
        #pg.find_user("accept")
        #pg.create_indexes()
        pass

    with MongoCollection("mongodb://student:Wd9hVzfB@82.148.28.116:27080","students","st5_users") as mn:
        #mn.drop_table()
        #mn.insert_users(1000000)
        #mn.create_indexes()
        #mn.find_user("professor")
        #mn.delete_indexes()
        pass
    #benchmark_postgres_search()
    #benchmark_mongo_search()