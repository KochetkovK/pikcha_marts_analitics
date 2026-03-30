import os
import json
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

# Загружаем переменные окружения
load_dotenv()

path_data = Path(__file__).resolve().parent.parent / 'data'

# Подключение к MongoDB
client = MongoClient(f"mongodb://localhost:{os.getenv('MONGO_PORT', '27017')}/")
db = client['shop_database']

# Список папок плюс будем их использовать для названий коллекций
folders = ['customers', 'products', 'stores', 'purchases']

for folder in folders:
    path_folder = path_data / folder
    if not path_folder.exists():
        print(f'⚠️ {path_folder} не существует')
        continue

    # удаление коллекции
    #db[folder].drop()

    lst_json = []    
    for path_file in path_folder.glob('*.json'):
        with open(path_file, encoding='utf-8') as json_file:
            try:
                data = json.load(json_file)
                lst_json.append(data)
            except Exception as e:
                print(f'❌ Ошибка в файле {path_file}: {e}')
    if lst_json:                  
        db[folder].insert_many(lst_json)   
        print(f'Количество файлов в папке {folder}: {len(lst_json)}')     
        print(f'Количество вставленых документов в коллекцию {folder}: {db[folder].count_documents({})}')
        #print(db[folder].find_one({}))
        
print(f'✅ Готово! База "shop_database" создана')

