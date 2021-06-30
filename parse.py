import requests
from bs4 import BeautifulSoup
import database
import time

# https://www.reformagkh.ru/search/houses?all=on&query=татарстан+казань+батыршина&page=1&limit=100
PASPORT = 'https://www.reformagkh.ru/myhouse/profile/passport/'
URL = 'https://www.reformagkh.ru/search/houses?query='
HEADERS = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36 OPR/77.0.4054.146',
           'accept': '*/*'}


def dict_to_tuples(dict):
    '''
    Converts dict of dicts into list of tuples
    Need only to fast write data in db
    :param dict: {{}, ...}
    :return: [(), ...]
    '''
    tupl = []
    array = []
    i = 0
    for key in dict:    # converts dict of dicts to list of lists
        array.append([key])
        for value in list(dict[key].values()):
            array[i].append(value)
        i += 1
    for ar in array:    # converts dict of dicts to dict of tuples
        tupl.append(tuple(ar))
    return tupl


def get_houses(query):
    """
    Getting a dict of houses with their ids and full address from site
    :param query: string, which contain address of home (region, city, street, number of house)
    :return: dict of houses, which located at specified area
    """
    result = {}
    query = query.replace(' ', '+')
    response = requests.get(URL, headers=HEADERS, params={'query': query})
    if response.ok:
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
        try:    # if such house is not exists, just return {}
            objects = soup.find('tbody').find_all('tr')
            for object in objects:
                tds = object.find_all('td')
                for td in tds:
                    if 'Жилищный фонд' in td.get_text(strip=True):  # choose houses only from 'Жилищный фонд' category. Works only on russian version of site/
                        id = int(object.find_all('td')[-1].find('a').get('data-favorite-house-id'))
                        if database.is_exist(id):   # breaks if we have this house in house table
                            break
                        result[id] = {'address': object.find('td').find('a').get_text(strip=True),
                                      'emergency': 0, 'commissioning_year': None, 'floors': None,
                                      'edit_date': None, 'series': None, 'type': None, 'cadastral_number': None,
                                      'floor_type': None, 'material': None}
        except AttributeError:
            return {}
    return result


def get_data(query):
    '''
    Getting characteristics for houses
    :param query: query string
    :return: [(id, address, ....), ...] list of house characteristics in tuples
    '''
    houses = get_houses(query)
    for house_id in houses:
        print(PASPORT + str(house_id))
        response = requests.get(PASPORT + str(house_id), headers=HEADERS)    # open the site for each house
        if response.ok:
            html = response.text
            soup = BeautifulSoup(html, 'html.parser')
            trs = soup.find('table', id='profile-house-style').find_all('tr')
            for tr in trs:  # getting main house characteristics on from table
                tds = tr.find_all('td')
                for td in tds:  # check rows values and writing corresponding house characteristics
                    if 'Год ввода дома в эксплуатацию:' in td.get_text(strip=True):
                        houses[house_id]['commissioning_year'] = int(tds[-1].get_text(strip=True))
                    if 'Количество этажей' in td.get_text(strip=True):
                        houses[house_id]['floors'] = int(tds[-1].get_text(strip=True))
                    if 'По данным Фонда ЖКХ информация последний раз актуализировалась:' in td.get_text(strip=True):
                        houses[house_id]['edit_date'] = tds[-1].get_text(strip=True)
                    if 'Серия, тип постройки здания:' in td.get_text(strip=True):
                        houses[house_id]['series'] = tds[-1].get_text(strip=True)
                    if 'Тип дома:' in td.get_text(strip=True):
                        houses[house_id]['type'] = tds[-1].get_text(strip=True)
                    if 'кадастровый номер' in td.get_text(strip=True):
                        houses[house_id]['cadastral_number'] = tds[-1].get_text(strip=True)
                    if 'Факт признания дома аварийным' in td.get_text(strip=True):  # using int instead of bool because sqlite don't have bool type
                        houses[house_id]['emergency'] = 1

            # getting constructive elements of house from table
            trs = soup.find('table', id='house-passport-constructive').find_all('tr')
            for tr in trs:
                tds = tr.find_all('td')
                for td in tds:  # check rows values and writing corresponding constructive elements
                    if 'Тип перекрытий' in td.get_text(strip=True):
                        houses[house_id]['floor_type'] = tds[-1].get_text(strip=True)
                    if 'Материал несущих стен' in td.get_text(strip=True):
                        houses[house_id]['material'] = tds[-1].get_text(strip=True)

    print('Houses added: ', len(houses))
    print(houses)
    return dict_to_tuples(houses)   # convert to list of tuples for writing to database


def check_for_changes(delay=10):
    '''
    Just checks if in query table appears record with is_complited != 1
    Probably, better do this with using any triggers or threads
    :param delay: delay for requests to db
    '''
    while True:
        new_records = database.get_new_records()    # [(id, text), ...]
        if new_records:
            for record in new_records:
                database.mark_as_completed(record[0])
                data = get_data(record[1])
                database.write_data(data, record[0])
                print('Updated')
        else:
            pass
        time.sleep(delay)


def search(query):
    '''
    Search specified houses on site, parsing data and load data to database.db
    :param query: query string, address of house (region, city, street, number) like 'казань баумана 22'
    '''
    database.create_tables()
    query_id = database.write_query(query)
    if query_id is not None:  # query_id == None if query table already had such query
        houses = get_data(query)
        database.write_data(houses, query_id)


search('казань восход 3')

# check_for_changes()
# database.clear_tables()

