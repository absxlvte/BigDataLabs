import psycopg2
import pandas as pd
from io import StringIO
def createDataLeakTable():
    with psycopg2.connect("host=82.148.28.116 user=student password=Wd9hVzfB dbname=student") as conn:
        with conn.cursor() as cur:
            cur.execute(
                """CREATE TABLE IF NOT EXISTS medical_data_breaches_table_st5 (
                    name_of_covered_entity TEXT,
                    state TEXT,
                    covered_entity_type TEXT,
                    individuals_affected INTEGER,
                    breach_submission_date DATE,
                    type_of_breach TEXT,
                    location_of_breached_information TEXT,
                    business_associate_present TEXT,
                    web_description TEXT)"""
            )
            df = pd.read_excel('Утечки мед данных.xls', sheet_name='reportResultTable')
            output = StringIO()
            df.to_csv(output,sep='\t',header=False,index=False)
            output.seek(0)
            cur.copy_from(output,'medical_data_breaches_table_st5', null="")

def dropDataLeakTable():
    with psycopg2.connect("host=82.148.28.116 user=student password=Wd9hVzfB dbname=student") as conn:
        with conn.cursor() as cur:
            cur.execute(
                """DROP TABLE medical_data_breaches_table_st5"""
            )

def addTuple(name_of_covered_entity : str=None,
             state : str=None,
             covered_entity_type : str=None,
             individuals_affected : int=None,
             breach_submission_date : str=None, #'YYYY-MM-DD'
             type_of_breach : str=None,
             location_of_breached_information : str=None,
             business_associate_present : str=None,
             web_description : str=None):
    with psycopg2.connect("host=82.148.28.116 user=student password=Wd9hVzfB dbname=student") as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO 
                    medical_data_breaches_table_st5 
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    (name_of_covered_entity,
                    state,
                    covered_entity_type,
                    individuals_affected,
                    breach_submission_date,
                    type_of_breach,
                    location_of_breached_information,
                    business_associate_present,
                    web_description)
            )



def findTuplesAdvanced(conditions: dict = {}, limit: int=None):
    """
    conditions: {
        'column1': 'value',                    #  =
        'column2': ('LIKE', '%value%'),        # LIKE
        'column3': ('>', 100),                 # > , <, >=, <=, !=
        'column4': ('IN', [1, 2, 3]),          # IN
        'column5': ('BETWEEN', [100, 1000]),   # BETWEEN
        'column6': ('IS NULL',),               # IS NULL
        'column7': ('IS NOT NULL',),           # IS NOT NULL
    }
    limit: maximum returned tuples
    """
    with psycopg2.connect("host=82.148.28.116 user=student password=Wd9hVzfB dbname=student") as conn:
        with conn.cursor() as cur:
            if not conditions:
                cur.execute("SELECT * FROM medical_data_breaches_table_st5")
            else:
                where_conditions = []
                params = []

                for column, condition in conditions.items():
                    if condition is None:
                        continue
                    if not isinstance(condition, (tuple, list)):
                        where_conditions.append(f"{column} = %s")
                        params.append(condition)
                    elif isinstance(condition, (tuple, list)) and len(condition) == 2:
                        operator, value = condition
                        operator = operator.upper()
                        if operator == 'LIKE':
                            where_conditions.append(f"{column} LIKE %s")
                            params.append(value)
                        elif operator == 'IN':
                            if not value:
                                where_conditions.append("1 = 0")
                            else:
                                placeholders = ','.join(['%s'] * len(value))
                                where_conditions.append(f"{column} IN ({placeholders})")
                                params.extend(value)
                        elif operator == 'BETWEEN':
                            where_conditions.append(f"{column} BETWEEN %s AND %s")
                            params.extend(value)
                        elif operator in ('=', '!=', '<>', '>', '<', '>=', '<='):
                            where_conditions.append(f"{column} {operator} %s")
                            params.append(value)
                    elif isinstance(condition, (tuple, list)) and len(condition) == 1:
                        operator = condition[0].upper()
                        if operator == 'IS NULL':
                            where_conditions.append(f"{column} IS NULL")
                        elif operator == 'IS NOT NULL':
                            where_conditions.append(f"{column} IS NOT NULL")
                query = "SELECT * FROM medical_data_breaches_table_st5"
                if where_conditions:
                    query += " WHERE " + " AND ".join(where_conditions)
                if limit is not None:
                    query += " LIMIT %s"
                    params.append(limit)
                debug_query = query
                for param in params:
                    if isinstance(param, str):
                        debug_query = debug_query.replace('%s', f"'{param}'", 1)
                    elif param is None:
                        debug_query = debug_query.replace('%s', 'NULL', 1)
                    else:
                        debug_query = debug_query.replace('%s', str(param), 1)
                print(f"SQL: {debug_query}")
                cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            data = cur.fetchall()
            df = pd.DataFrame(data, columns=columns)
            return df

def showExamples(num: int = 0, limit: int=None):
    match num:
        case 0:
            print("state = VA\n")
            df = findTuplesAdvanced({
                "state": "VA"
            }, limit=limit)
            return df
        case 1:
            print("name_of_covered_entity LIKE Hospital\n")
            df = findTuplesAdvanced({
                "name_of_covered_entity": ("LIKE", "%Hospital%")
            }, limit=limit)
            return df
        case 2:
            print("individuals_affected > 1000000\n")
            df = findTuplesAdvanced({
                "individuals_affected": (">", 1000000)
            }, limit=limit)
            return df
        case 3:
            print("state IN (CA, NY, TX, FL)\n")
            df = findTuplesAdvanced({
                "state": ("IN", ["CA", "NY", "TX", "FL"])
            }, limit=limit)
            return df
        case 4:
            print("individuals_affected BETWEEN 1000 AND 10000\n")
            df = findTuplesAdvanced({
                "individuals_affected": ("BETWEEN", [1000, 10000])
            }, limit=limit)
            return df
        case 5:
            print("covered_entity_type != Health Plan\n")
            df = findTuplesAdvanced({
                "covered_entity_type": ("!=", "Health Plan")
            }, limit=limit)
            return df
        case 6:
            print("web_description IS NOT NULL\n")
            df = findTuplesAdvanced({
                "web_description": ("IS NOT NULL",)
            }, limit=limit)
            return df
        case 7:
            print("state = CA individuals_affected > 50000 type_of_breach LIKE %Hacking%\n")
            df = findTuplesAdvanced({
                "state": "CA",
                "individuals_affected": (">", 50000),
                "type_of_breach": ("LIKE", "%Hacking%")
            }, limit=limit)
            return df
        case -1:
            df = getAggregatedStats('count')
            return df
        case -2:
            print("covered_entity_type ordered by counts")
            df = getAggregatedStats('group_by', 'covered_entity_type')
            return df
        case _:
            pass


def getAggregatedStats(aggregation_type: str, column: str = None, conditions: dict = None):
    SAFE_QUERIES = {
        'count': "SELECT COUNT(*) as total_count FROM medical_data_breaches_table_st5",
        'sum_affected': "SELECT SUM(individuals_affected) as total_sum FROM medical_data_breaches_table_st5",
        'avg_affected': "SELECT AVG(individuals_affected) as average_value FROM medical_data_breaches_table_st5",
        'min_affected': "SELECT MIN(individuals_affected) as min_value FROM medical_data_breaches_table_st5",
        'max_affected': "SELECT MAX(individuals_affected) as max_value FROM medical_data_breaches_table_st5",
        'group_by_state': "SELECT state, COUNT(*) as count FROM medical_data_breaches_table_st5 GROUP BY state ORDER BY count DESC",
        'group_by_entity': "SELECT covered_entity_type, COUNT(*) as count FROM medical_data_breaches_table_st5 GROUP BY covered_entity_type ORDER BY count DESC",
        'group_by_breach': "SELECT type_of_breach, COUNT(*) as count FROM medical_data_breaches_table_st5 GROUP BY type_of_breach ORDER BY count DESC",
        'group_by_location': "SELECT location_of_breached_information, COUNT(*) as count FROM medical_data_breaches_table_st5 GROUP BY location_of_breached_information ORDER BY count DESC",
        'group_by_associate': "SELECT business_associate_present, COUNT(*) as count FROM medical_data_breaches_table_st5 GROUP BY business_associate_present ORDER BY count DESC"
    }
    query_map = {
        'count': 'count',
        'sum': 'sum_affected',
        'avg': 'avg_affected',
        'min': 'min_affected',
        'max': 'max_affected',
        'group_by': {
            'state': 'group_by_state',
            'covered_entity_type': 'group_by_entity',
            'type_of_breach': 'group_by_breach',
            'location_of_breached_information': 'group_by_location',
            'business_associate_present': 'group_by_associate'
        }
    }
    if aggregation_type == 'group_by' and column:
        if column not in query_map['group_by']:
            raise ValueError(f"Столбец {column} не разрешен для группировки")
        safe_query_key = query_map['group_by'][column]
    else:
        if aggregation_type not in query_map:
            raise ValueError(f"Недопустимый тип агрегации: {aggregation_type}")
        safe_query_key = query_map[aggregation_type]
    query = SAFE_QUERIES[safe_query_key]
    with psycopg2.connect("host=82.148.28.116 user=student password=Wd9hVzfB dbname=student") as conn:
        with conn.cursor() as cur:
            where_conditions = []
            params = []
            if conditions:
                ALLOWED_CONDITION_COLUMNS = ['state', 'covered_entity_type', 'type_of_breach',
                                             'business_associate_present']
                for col, cond in conditions.items():
                    if col not in ALLOWED_CONDITION_COLUMNS:
                        raise ValueError(f"Недопустимый столбец в условиях: {col}")
                    if cond is None:
                        continue
                    if not isinstance(cond, (tuple, list)):
                        where_conditions.append(f"{col} = %s")
                        params.append(cond)
                    elif isinstance(cond, (tuple, list)) and len(cond) == 2:
                        operator, value = cond
                        operator = operator.upper()
                        if operator == 'LIKE':
                            where_conditions.append(f"{col} LIKE %s")
                            params.append(value)
                        elif operator == 'IN':
                            placeholders = ','.join(['%s'] * len(value))
                            where_conditions.append(f"{col} IN ({placeholders})")
                            params.extend(value)
            if where_conditions:
                query += " WHERE " + " AND ".join(where_conditions)
            debug_query = query
            for param in params:
                if isinstance(param, str):
                    debug_query = debug_query.replace('%s', f"'{param}'", 1)
                elif isinstance(param, list):
                    param_str = ', '.join([f"'{p}'" if isinstance(p, str) else str(p) for p in param])
                    debug_query = debug_query.replace('%s', param_str, len(param))
                elif param is None:
                    debug_query = debug_query.replace('%s', 'NULL', 1)
                else:
                    debug_query = debug_query.replace('%s', str(param), 1)
            print(f"SQL: {debug_query}")
            cur.execute(query, params)
            data = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            df = pd.DataFrame(data, columns=columns)
            return df

#dropDataLeakTable()

#createDataLeakTable()

#addTuple(name_of_covered_entity="Grand Lake Hospital",state="NY",breach_submission_date="2024-12-25",individuals_affected=100, web_description="www.rt.ru/post/10213")
"""
SELECT *
FROM "medical_data_breaches_table_st5"
WHERE "name_of_covered_entity" LIKE 'Grand Lake Hospital'
"""

#0: =
#1: LIKE
#2: > , <, >=, <=, !=
#3: IN
#4: BETWEEN
#5: !=
#6: IS NOT NULL
#7: complex
#-1: total_count of tuples
#-2: number of incidents ordered by type of organization

"""var = 2
df = showExamples(var)
print(df.to_string(index=False))"""
