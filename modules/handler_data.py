import os
import sys
import json
import hashlib
import re
import secrets
import string
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient
from pathlib import Path


# -> $PROJECT_PATH при запуске в Airflow
# -> иначе - текущая директория при локальном запуске
path = os.environ.get('PROJECT_PATH', '..')

def clean_phone(phone: str) -> str:
    '''Функция приводит номер телефона к единому виду.
       Возвращает номер, состоящий только из цифр и начинается с восьмерки'''
    new_phone = phone.replace(' ', '').replace('(', '').replace(')', '').replace('-', '')
    if re.fullmatch(r'\+?\d{11}', new_phone):
        new_phone = re.sub(r'^\+7', '8', new_phone, 1)
        return new_phone
    else:
        return None
    
def clean_email(email: str) -> str:
    '''Функция очищает email'''
    if email:
        return email.replace(' ', '').lower()
    else:
        return None

    
def hash_and_sault(obj: str) -> str:
    ''' Функция возвращает хеш объекта, измененный солью'''
    # соль отдельно не сохраняем
    characters = string.ascii_uppercase + string.digits
    sault = ''.join(secrets.choice(characters) for _ in range(12))
 
    md5_hash_object = hashlib.md5((obj + sault).encode("utf-8")).hexdigest()

    return md5_hash_object


def handler_data(data):
    '''Рекурсивная функция, которая обрабатывает данные
       в словаре с неопределенной глубиной вложенности, ищет номер телефона и email и хеширует их'''
    if not isinstance(data, dict):
        return data
    
    new_data = data.copy()

    if "_id" in new_data:
        new_data["_id"] = str(new_data["_id"])


    for key, value in new_data.items():
        if isinstance(value, str):
            if key == "phone":
                new_data[key] = hash_and_sault(clean_phone(value))
            elif key == "email":
                new_data[key] = hash_and_sault(clean_email(value))

        elif isinstance(value, dict):
            new_data[key] = handler_data(value)

        elif isinstance(value, list):
            res = []
            for el in value:
                if isinstance(el, dict):
                    res.append(handler_data(el))
                else:
                    res.append(el) 
            new_data[key] = res   

    return new_data


if __name__ == "__main__":
    # Загружаем переменные окружения
    load_dotenv()
    test_phone = ['+7 (513) 412-52-86', 
                  '8 (528) 953-92-64', 
                  '+7 (480) 396-5223', 
                  '8 (008) 860-08-48',
                  '8 827 109 1043', 
                  '+71918255371', 
                  '82330958498'
                 ]
    
    test_email = "paramon_1999@example.org"
    
    for p in test_phone:
        print(clean_phone(p))
        print(hash_and_sault(clean_phone(p)))

    print()
    print(hash_and_sault(clean_email(test_email)))
        
    # Подключение к MongoDB
    client = MongoClient(f"mongodb://localhost:{os.getenv('MONGO_PORT', '27017')}/")
    db = client['shop_database']
    collections = ['customers', 'products', 'stores', 'purchases']
    for collection in collections:
        data = db[collection].find_one()
        print(collection)
        print('Сырые данные')
        print(data)
        print('Обработанные данные')
        print(handler_data(data))
 
