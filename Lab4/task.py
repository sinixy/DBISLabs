import os
import re
import csv
import logging
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from vars import *
from time import sleep
from datetime import datetime

# Налаштування логеру
now = datetime.now().strftime('%d-%m-%Y_%H-%M-%S')
logging.basicConfig(
    filename=f'logs_{now}.log',
    filemode='w',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S'
)
logging.info('START')


# ======== Встновлюємо з'єднання з базою ========
connected = False
connection_config = {
    'database': DATABASE,
    'host': HOST,
    'port': PORT
}

while not connected:
    try:
        # Зміна конфігурації підключення до БД
        config_confirmed = False
        while not config_confirmed:
            print(f"""\nКонфігурація з'єднання з БД:
database: {connection_config['database']}
host: {connection_config['host']}
port: {connection_config['port']}""")
            c = input("""\nВведіть назву параметру (database, host, port), який бажаєте змінити в конфігурації.
Для підключення з поточними параметрами введіть confirm.
>>> """)
            if c == 'confirm':
                config_confirmed = True
            elif c in ['database', 'host', 'port']:
                val = input(f'Введіть нове значення {c}: ')
                connection_config[c] = val
            else:
                print('Невідома команда!')
        logging.info('Connecting to the DB...')

        client = MongoClient(host=connection_config['host'], port=connection_config['port'])
        db = client[connection_config['database']]

        connected = True
    except Exception as e:
        logging.error("Connection failed! " + str(e))
        print("Не вдалося встановити з'єднання з базою!", e)

logging.info("Connected to " + DATABASE)


# ======== Створення колекцій ========
collections_created = False
while not collections_created:
    try:
        zno = db.zno
        zno_tmp = db.tmp
        collections_created = True
    except Exception as e:
        logging.error("Can't create tables! Retrying after 10 seconds...")
        print("Не вдалося створити таблиці! Друга спроба через 10 секунд...")
        sleep(10)

logging.info('ZNO and TMP collections has been created!')


def get_insert_values(row, year):
    # функція для перетворення даних
    res = {}
    for field, value in row.items():
        if value == 'null':
            res[field] = None
        elif field in FLOAT_FIELDS:
            res[field] = float(value.replace(',', '.'))
        elif field in INTEGER_FIELDS:
            res[field] = int(value)
        else:
            res[field] = value
    res['YEAR'] = year
    return res


# ======== Занесення даних до колекцій ========
start = datetime.now()

for filename in os.listdir('D:\\AppMathOwO\\6sem\\dbis\\Lab1\\data'):
    year = re.findall(r'Odata(\d{4})File.csv', filename)  # знаходимо потрібний файл
    if year:
        year = int(year[0])
        with open(os.path.join('D:\\AppMathOwO\\6sem\\dbis\\Lab1\\data', filename), encoding='cp1251') as file:
            logging.info('Inserting data from ' + filename)
            counter = 0  # лічильник, який показує скільки документів записано.

            info = zno_tmp.find_one({'YEAR': year})
            if info:  # в базі вже є певні дані
                if info['done']:  # якщо done == True, то запис завершено
                    logging.info(f'All {info["inserted_count"]} docs have already been inserted!')
                    continue
                else:  # інакше, у базі присутня лише якась частина даних
                    counter = info['inserted_count']  # змінюємо значення лічильника
                    logging.info('Already inserted docs count: ' + str(counter))
            else:  # якщо даних для цього року немає – створюємо запис у допоміжній таблиці
                zno_tmp.insert_one({'YEAR': year, 'inserted_count': 0, 'done': False})

            reader = csv.DictReader(file, delimiter=';')

            # Сама вставка даних
            for i, row in enumerate(reader, start=1):
                if i <= counter or not row:
                    continue
                try:
                    zno.insert_one(get_insert_values(row, year))
                    counter += 1
                    zno_tmp.update_one(
                        {'YEAR': year},
                        {'$set': {'inserted_count': counter}}
                    )
                    if i % 1000 == 0:
                        print(f'Створено {counter} документів...')
                except ConnectionFailure as e:
                    print("Втрачено з'єднання з базою!", e)
                    logging.error("Connection lost! " + str(e))
                    connected = False
                    while not connected:
                        try:
                            print("Намагаємося встановити з'єднання...")
                            logging.info("Trying to reconnect...")
                            client = MongoClient(host=connection_config['host'], port=connection_config['port'])
                            db = client[connection_config['database']]
                            zno = db.zno
                            zno_tmp = db.tmp
                            connected = True
                        except ConnectionFailure as e:
                            print("Не вдалося встановити з'єднання!", e)
                            logging.error("Failed to reconnect! " + str(e))
                            print('Retrying after 10 seconds...')
                            logging.info('Наступна спроба через 10 секунд...')
                            sleep(10)
                    print("З'єднання успішно відновлено!")
                    logging.info("Connection restored!")
            print(f"Дані з файлу {filename} успішно завантажені до бази! Всього в базі {counter} документів!")
            logging.info(f"Data from {filename} inserted to the DB! Total docs: {counter}!")
            zno_tmp.update_one(
                {'YEAR': year},
                {'$set': {'done': True}}
            )




end = datetime.now()
duration = (end - start)
if duration.seconds == 0:
    t = duration.microseconds / 1000  # мілісекунди
    print('Час завантаження даних до БД:', t, 'мс')
    logging.info('======================')
    logging.info('Insertion time: ' + str(t) + ' ms')
    logging.info('======================')
else:
    t = duration.seconds
    print('Час завантаження даних до БД:', t, 'с')
    logging.info('======================')
    logging.info('Insertion time: ' + str(t) + ' s')
    logging.info('======================')

query = [
    {'$match': {'UkrTestStatus': 'Зараховано'}},
    {
        '$group':
        {
            '_id': {
               "REGNAME": "$REGNAME",
               "YEAR": "$YEAR"
            },
            "Ball100": {"$avg": "$UkrBall100"},
            "Ball12": {"$avg": "$UkrBall12"},
            "Ball": {"$avg": "$UkrBall"}
        }
    },
    {'$sort': {'_id.REGNAME': 1}}
]

header = ['year', 'regname', 'Ball100', 'Ball12', 'Ball']

with open(os.path.join('data', 'result.csv'), 'w', encoding='cp1251') as file:
    print("Виконуємо запит до бази...")
    logging.info("Executing query...")
    result = zno.aggregate(query)

    writer = csv.writer(file, dialect='excel')
    writer.writerow(header)
    for doc in result:
        writer.writerow([doc['_id']['YEAR'], doc['_id']['REGNAME'], doc['Ball100'], doc['Ball12'], doc['Ball']])
    print("Результати запиту записано до файлу data/result.csv!")
    logging.info("Query result is saved to data/result.csv")

logging.info("END")