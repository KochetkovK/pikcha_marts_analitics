import os
import json
import logging
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaProducer
from handler_data import handler_data

# Загружаем переменные окружения
load_dotenv()

# Создадим логгер
logger = logging.getLogger(__name__)

def producer_mongo_to_kafka():
    try:
        producer = KafkaProducer(
                        bootstrap_servers='localhost:9092',
                        value_serializer=lambda v: json.dumps(v).encode('utf-8')
                    )
        
        # Подключение к MongoDB
        client = MongoClient(f"mongodb://localhost:{os.getenv('MONGO_PORT', '27017')}/")
        db = client['shop_database']
        collections = ['customers', 'stores', 'products', 'purchases']

        for collection in collections:
            #Проверим, что бы в коллекции были данные
            count_doc = db[collection].count_documents({})
            logger.info(f"В коллекции: {collection} {count_doc} документов")

            count = 0
            current_time = datetime.now().isoformat(timespec="seconds")
            record_source = f"mongo_db:{db.name}_{collection}"

            if count_doc:
                for doc in db[collection].find():
                    data = handler_data(doc)
                    data["load_date"] = current_time
                    data["record_source"] = record_source
                    
                    producer.send(collection, value=data)
                    count += 1
            logger.info(f"Отправлено в топик: {collection} {count} документов") 
    
        producer.flush()
                        

    except Exception as e:
        logger.error(f"Error: {e}")

if __name__ == "__main__":
    logger.setLevel(logging.INFO)
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    producer_mongo_to_kafka()
