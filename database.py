import sqlite3

PATH = 'database.db'   # path to database file


def create_tables():
    '''
    Creates tables if they wasn't created
    '''
    with sqlite3.connect(PATH) as db:
        cursor = db.cursor()
        query = """ CREATE TABLE IF NOT EXISTS queries(id INTEGER, text TEXT, is_completed INTEGER) """
        query2 = """ CREATE TABLE IF NOT EXISTS houses(id INTEGER, address TEXT, emergency INTEGER,
                    commissioning_year INTEGER, floors INTEGER, edit_date TEXT, series TEXT, type TEXT,
                     cadastral_number TEXT, floor_type TEXT, material TEXT, query_id INTEGER) """
        cursor.execute(query)
        cursor.execute(query2)
        db.commit()


def get_max_id():
    '''
    Retrives max id in queries table, if no records in the table returns 0
    '''
    with sqlite3.connect(PATH) as db:
        cursor = db.cursor()
        query = """ SELECT MAX(id) FROM queries"""
        cursor.execute(query)
        max_id = cursor.fetchone()[0]
        if max_id is not None:
            return max_id
        else:
            return 0


def write_query(query_text):
    '''
    Writing in query table record with query text and so on
    :param query_text: text of query, string
    :return: query_id of created record in query table
    '''
    def query_exist(text):
        '''
        Check if query with such text already exists in db
        :param text: text of query
        :return: true if exists, false otherwise
        '''
        with sqlite3.connect(PATH) as db:
            cursor = db.cursor()
            query = f" SELECT id FROM queries WHERE text = '{text}' "
            cursor.execute(query)
            return bool(cursor.fetchall())

    if not query_exist(query_text):
        with sqlite3.connect(PATH) as db:
            cursor = db.cursor()
            query = """ INSERT INTO queries(id, text, is_completed) VALUES(?, ?, ?) """
            query_id = get_max_id() + 1
            data = [(query_id, query_text, 1)]
            cursor.executemany(query, data)
            db.commit()
            return query_id
    else:
        return None


def write_data(data, query_id):
    '''
    Writing data in houses table
    :param data: list of house characteristics in tuples
    :param query_id:
    :return:
    '''
    if not data:  # if data is empty just return
        return
    with sqlite3.connect(PATH) as db:
        cursor = db.cursor()
        query = """ INSERT INTO houses(id, address, emergency, commissioning_year, floors, edit_date, series, type,
                     cadastral_number, floor_type, material, query_id) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) """
        # adding query_id to new_data
        new_data = []
        for tupl in data:
            new_data.append(list(tupl))
        i = 0
        for lst in new_data:
            new_data[i].append(query_id)
            new_data[i] = tuple(new_data[i])
            i += 1
        print(new_data)
        cursor.executemany(query, new_data)
        db.commit()


def clear_tables():
    '''
    Clears both tables. Just for tests
    '''
    with sqlite3.connect(PATH) as db:
        cursor = db.cursor()
        query = """ DELETE FROM houses """
        query2 = """ DELETE FROM queries """
        cursor.execute(query)
        cursor.execute(query2)
        db.commit()


def get_new_records():
    '''
    Getting ids and text of new records from queries where is_completed != 1
    :return: list of tuples (id, text) of new records
    '''
    with sqlite3.connect(PATH) as db:
        cursor = db.cursor()
        query = """ SELECT id, text FROM queries WHERE (is_completed != 1) """
        cursor.execute(query)
        return cursor.fetchall()


def mark_as_completed(id):
    '''
    Marks query with corresponding id as completed (is_completed = 1)
    :param ids: int
    '''
    with sqlite3.connect(PATH) as db:
        cursor = db.cursor()
        query = f" UPDATE queries SET is_completed = 1 WHERE id = '{id}'"
        cursor.execute(query)
        db.commit()


def is_exist(id):
    '''
    Check if house with such id already exists in houses table
    :param id: id of house
    :return: true if exists, false otherwise
    '''
    with sqlite3.connect(PATH) as db:
        cursor = db.cursor()
        query = f" SELECT id FROM houses WHERE id = '{id}' "
        cursor.execute(query)
        return bool(cursor.fetchall())


def finded_count(query):
    '''
    Returns count of houses which found by this query
    :param query: text of query
    '''
    with sqlite3.connect(PATH) as db:
        cursor = db.cursor()
        query = f" SELECT id FROM queries WHERE text = '{query}' "
        cursor.execute(query)
        query_id = cursor.fetchone()
        if query_id is not None:
            with sqlite3.connect(PATH) as db2:
                cursor2 = db2.cursor()
                query = f" SELECT id FROM houses WHERE query_id = '{query_id[0]}' "
                cursor2.execute(query)
                ids = cursor2.fetchall()
                return len(ids)
        else:
            return 0


def brick_houses_count():
    '''
    Returns count of brick houses in each region
    :return: {region: count of brick houses, ...}
    '''
    res = {}
    with sqlite3.connect(PATH) as db:
        cursor = db.cursor()
        query = " SELECT address FROM houses WHERE material = 'Кирпич' "
        cursor.execute(query)
        houses = cursor.fetchall()
        for house in houses:
            region = house[0].split(',')[0]
            if region not in res:
                res[region] = 1
            else:
                res[region] += 1
    return res


def max_floors4material():
    '''
    Returns max floors count for each material and city
    Will be problems with cities Moscow and St Petersburg, because they don't have region
    :return:    dict of cities dict of materials dict
    '''
    res = {}
    with sqlite3.connect(PATH) as db:
        cursor = db.cursor()
        query = "SELECT address, material, floors FROM houses"
        cursor.execute(query)
        houses = cursor.fetchall()
        for house in houses:
            city = house[0].split(',')[1].strip()
            if city not in res:
                res[city] = {}
            if (house[1] not in res[city]) or (res[city][house[1]] < house[2]):
                res[city][house[1]] = house[2]
    return res

