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


# ======== Створення таблиць ========
tbl_created = False
while not tbl_created:
    try:
        cur.execute(CREATE_MAIN)  # Головна таблиця, до якої будуть заноситися дані
        cur.execute(CREATE_TMP)   # Допоміжна таблиця, де буде відслідковуватися к-сть занесених рядків до осн. табл.
        con.commit()
        tbl_created = True
    except Exception as e:
        logging.error("Can't create tables! Retrying after 10 seconds...")
        print("Не вдалося створити таблиці! Друга спроба через 10 секунд...")
        sleep(10)

logging.info('Created tables ZNO and TMP!')


def get_insert_values(row):
    # функція для перетворення даних
    res = []
    for field, value in row.items():
        if value == 'null':
            res.append(None)
        elif field in REAL_FIELDS:
            res.append(float(value.replace(',', '.')))
        elif field in INTEGER_FIELDS:
            res.append(int(value))
        else:  # для VARCHAR
            res.append(value)
    return res


# ======== Занесення даних до таблиці ========
start = datetime.now()

for filename in os.listdir('data'):
    year = re.findall(r'Odata(\d{4})File.csv', filename)  # знаходимо потрібний файл
    if year:
        year = int(year[0])
        with open(os.path.join('data', filename), encoding='cp1251') as file:
            logging.info('Inserting data from ' + filename)
            counter = 0  # лічильник, який показує скільки рядків записано до бази.
            # Перевіряємо наявність уже занесених даних
            cur.execute('SELECT YEAR, COUNTER, DONE FROM TMP WHERE YEAR = %s', [year])
            info = cur.fetchone()
            if info:  # в базі вже є певні дані
                if info[2]:  # якщо DONE == True, то всі рядки даного файлу вже присутні в базі
                    logging.info(f'All {info[1]} rows have already been inserted!')
                    continue
                else:  # інакше, у базі присутня лише якась частина даних
                    counter = info[1]  # змінюємо значення лічильника
                    logging.info('Already inserted rows count: ' + str(info[1]))
            else:  # якщо даних для цього року немає – створюємо запис у допоміжній таблиці
                cur.execute('INSERT INTO TMP VALUES (%s, %s, %s)', [year, counter, False])

            reader = csv.DictReader(file, delimiter=';')
            header = ['YEAR'] + reader.fieldnames
            insert_query = "INSERT INTO ZNO VALUES " + '(' + ', '.join(['%s'] * len(header)) + ')'

            # Сама вставка даних
            for i, row in enumerate(reader, start=1):
                if i <= counter:  # пропускаємо рядки, які вже записані до бази 
                    continue
                try:
                    if row:
                        cur.execute(insert_query, [year] + get_insert_values(row))
                        counter += 1
                        if counter % 100 == 0:
                            cur.execute("UPDATE TMP SET COUNTER = %s WHERE YEAR = %s", [counter, year])
                            con.commit()
                except psycopg2.OperationalError as e:
                    print("Втрачено з'єднання з базою!", e)
                    logging.error("Connection lost! " + str(e))
                    connected = False
                    while not connected:
                        try:
                            print("Намагаємося встановити з'єднання...")
                            logging.info("Trying to reconnect...")
                            con = psycopg2.connect(**connection_config)
                            cur = con.cursor()
                            connected = True
                        except psycopg2.OperationalError as e:
                            print("Не вдалося встановити з'єднання!", e)
                            logging.error("Failed to reconnect! " + str(e))
                            print('Retrying after 10 seconds...')
                            logging.info('Наступна спроба через 10 секунд...')
                            sleep(10)
                    print("З'єднання успішно відновлено!")
                    logging.info("Connection restored!")
            cur.execute("UPDATE TMP SET DONE = TRUE, COUNTER = %s WHERE YEAR = %s", [counter, year])
            con.commit()
            print(f"Дані з файлу {filename} успішно завантажені до бази! Всього в базі {counter} рядків!")
            logging.info(f"Data from {filename} inserted to the DB! Total rows: {counter}!")

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


logging.info("END")