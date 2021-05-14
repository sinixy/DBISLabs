# Лабораторна робота №2. Сін Г.П. КМ-83
## Інструкція по запуску
1. Завантажити репозиторій
2. Додати файли Odata2019File.csv та Odata2020File.csv у папку data
3. Запустити скрипт populate_kp1.py, який створить основну та наповнить основну таблицю з КП1
4. Запустити скрипт task_kp1.py, який виконає запит до бази з КП1 та збереже результати у data/result_kp1.csv
5. Відредагувати за потреби файл конфігурації conf/flyway.conf:
flyway.url=jdbc:postgresql://[LOCALHOST]:[PORT]/[DBNAME]
flyway.user=[USER]
flyway.password=[PASSWORD]
де замість плейсхолдерів треба підставити свої параметри. У рамках лабораторної роботи взяті стандартні значення:
flyway.url=jdbc:postgresql://localhost:5432/postgres
flyway.user=postgres
flyway.password=password
6. Виконати команду `flyway migrate` у терміналі та дочекатися закінчення міграції. Результат виконання команди в терміналі повинен виглядати так: ![alt text](https://github.com/sinixy/DBISLabs/blob/master/Lab2/terminal.png?raw=true)
7. Запустити скрипт task_kp2.py, який виконає аналогічний запит з КП1, але вже на мігрованій базі та збереже результати у data/result_kp2.csv


## Додаткові файли
- logical_erd.png – логічна діграма "сутність-зв'язок"
- physical_erd.png – фізична діаграма "сутність-зв'язок"
- sql/V1_1__create.sql – створення таблиць КП2
- sql/V1_2__populate.sql – заповнення даними таблиць КП2
- data/result_kp1.csv – файл з результатами виконання запиту task_kp1.py
- data/result_kp2.csv – файл з результатами виконання запиту task_kp2.py
- vars – файл з деякими константами (початкова конфігурація подключення, create statements), що імпортуються до основного скрипта
