import os
import requests
import libarchive


zno_urls = [
    "https://zno.testportal.com.ua/yearstat/uploads/OpenDataZNO2019.7z",
    "https://zno.testportal.com.ua/yearstat/uploads/OpenDataZNO2020.7z"
]

try:
    os.mkdir('data')
except FileExistsError as e:
    print('"data" directory is already exists!')

for url in zno_urls:
    filename = url.split('/')[-1]
    with open('data/' + filename, 'wb') as file:
        archive = requests.get(url, allow_redirects=True)
        file.write(archive.content)

for file in os.listdir('data'):
    with libarchive.file_reader('data/' + file) as reader:
        for e in reader:
            if e.path.split('.')[-1] == 'csv':
                print('writing', e.path)
                with open('data/' + e.path, 'w') as csv:
                    for block in e.get_blocks():
                        csv.write(block.decode('cp1251'))
                print('Done!')