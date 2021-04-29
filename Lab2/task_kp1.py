import os
import re
import csv
import logging
import psycopg2
import psycopg2.extras
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
    'user': USER,
    'password': PASSWORD
}


while not connected:
    try:
        # Зміна конфігурації підключення до БД
        config_confirmed = False
        while not config_confirmed:
            print(f"""\nКонфігурація з'єднання з БД:
database: {connection_config['database']}
user: {connection_config['user']}
password: {connection_config['password']}""")
            c = input("""\nВведіть назву параметру (database, user, password), який бажаєте змінити в конфігурації.
Для підключення з поточними параметрами введіть confirm.
>>> """)
            if c == 'confirm':
                config_confirmed = True
            elif c in ['database', 'user', 'password']:
                val = input(f'Введіть нове значення {c}: ')
                connection_config[c] = val
            else:
                print('Невідома команда!')
        logging.info('Connecting to the DB...')

        con = psycopg2.connect(**connection_config)
        cur = con.cursor()

        connected = True
    except Exception as e:
        logging.error("Connection failed! " + str(e))
        print("Не вдалося встановити з'єднання з базою!", e)

logging.info("Connected to " + DATABASE)


query = '''
SELECT 
    YEAR,
    REGNAME,
    avg(UkrBall100) AS Ball100,
    avg(UkrBall12) AS Ball12,
    avg(UkrBall) AS Ball
FROM ZNO
WHERE UkrTestStatus = 'Зараховано' 
GROUP BY REGNAME, YEAR
'''
header = ['year', 'regname', 'ball100', 'ball12', 'ball']
with open(os.path.join('data', 'result_kp1.csv'), 'w', encoding='cp1251') as file:
    print("Виконуємо запит до бази...")
    logging.info("Executing query...")
    writer = csv.writer(file, dialect='excel')
    writer.writerow(header)
    cur.execute(query)
    writer.writerows(cur.fetchall())
    print("Результати запиту записано до файлу data/result_kp1.csv!")
    logging.info("Query result is saved to data/result_kp1.csv")

logging.info("END")