import os
import json
import logging
import time
import clickhouse_connect
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaConsumer

# Загружаем переменные окружения
load_dotenv()

logger = logging.getLogger(__name__)

def consumer_to_clickhouse():
    topics = ['customers', 'stores', 'products', 'purchases']
    batch_customers = []
    batch_stores = []
    batch_products = []
    batch_purchases = []
    BATCH_SIZE = 10000  # Максимальный размер пачки
    MAX_WAIT_TIME = 5   # Тайм-аут в секундах
    last_send_time_customers = time.time()
    last_send_time_stores = time.time()
    last_send_time_products = time.time()
    last_send_time_purchases = time.time()
    
    query_customers = """INSERT INTO mart_raw.customers (
                            record_id,
                            customer_id,
                            first_name,
                            last_name,
                            email,
                            phone, 
                            birth_date,
                            gender,
                            registration_date,
                            is_loyalty_member,
                            loyalty_card_number,
                            purchase_location_country,
                            purchase_location_city,
                            purchase_location_street,
                            purchase_location_house,
                            purchase_location_postal_code,
                            purchase_location_coordinates_latitude,
                            purchase_location_coordinates_longitude,
                            delivery_address_country,
                            delivery_address_city,
                            delivery_address_street,
                            delivery_address_house,
                            delivery_address_apartment,
                            delivery_address_postal_code,
                            preferences_preferred_language,
                            preferences_preferred_payment_method,
                            preferences_receive_promotions,
                            load_date,
                            record_source) 
                         VALUES """
    
    query_stores = """INSERT INTO mart_raw.stores (
                        record_id,
                        store_id,
                        store_name,
                        store_network,
                        store_type_description,
                        type,
                        categories,
                        manager_name,
                        manager_phone,
                        manager_email,
                        location_country,
                        location_city,
                        location_street,
                        location_house,
                        location_postal_code,
                        location_coordinates_latitude,
                        location_coordinates_longitude,
                        opening_hours_mon_fri,
                        opening_hours_sat,
                        opening_hours_sun,
                        accepts_online_orders,
                        delivery_available,
                        warehouse_connected,
                        last_inventory_date,
                        load_date,
                        record_source)
                      VALUES """
    
    query_products = """INSERT INTO mart_raw.products (
                            record_id,
                            product_id,
                            name,
                            group,
                            description,
                            kbju_calories,
                            kbju_protein,
                            kbju_fat,
                            kbju_carbohydrates,
                            price,
                            unit,
                            origin_country,
                            expiry_days,
                            is_organic,
                            barcode,
                            manufacturer_name,
                            manufacturer_country,
                            manufacturer_website,
                            manufacturer_inn,
                            load_date,
                            record_source)
                        VALUES """
    
    query_purchases = """INSERT INTO mart_raw.purchases (
                            record_id,
                            purchase_id,
                            customer_customer_id,
                            customer_first_name,
                            customer_last_name,
                            store_store_id,
                            store_store_name,
                            store_store_network,
                            store_location_city,
                            store_location_street,
                            store_location_house,
                            store_location_postal_code,
                            store_location_coordinates_latitude,
                            store_location_coordinates_longitude,
                            items,
                            total_amount,
                            payment_method,
                            is_delivery,
                            delivery_address_city,
                            delivery_address_street,
                            delivery_address_house,
                            delivery_address_apartment,
                            delivery_address_postal_code,
                            purchase_datetime,
                            load_date,
                            record_source)
                         VALUES """

    def get_values_customers(topic_name, data):
        if topic_name == 'customers':
            return f"""(
                '{data['_id']}',
                '{data['customer_id']}',
                '{data['first_name']}',
                '{data['last_name']}',
                '{data['email']}',
                '{data['phone']}', 
                '{data['birth_date']}',
                '{data['gender']}',
                '{data['registration_date']}',
                '{data['is_loyalty_member']}',
                '{data['loyalty_card_number']}',
                '{data['purchase_location']['country']}',
                '{data['purchase_location']['city']}',
                '{data['purchase_location']['street']}',
                '{data['purchase_location']['house']}',
                '{data['purchase_location']['postal_code']}',
                '{data['purchase_location']['coordinates']['latitude']}',
                '{data['purchase_location']['coordinates']['longitude']}',
                '{data['delivery_address']['country']}',
                '{data['delivery_address']['city']}',
                '{data['delivery_address']['street']}',
                '{data['delivery_address']['house']}',
                '{data['delivery_address']['apartment']}',
                '{data['delivery_address']['postal_code']}',
                '{data['preferences']['preferred_language']}',
                '{data['preferences']['preferred_payment_method']}',
                '{data['preferences']['receive_promotions']}',
                toDateTime('{data['load_date']}'),
                '{data['record_source']}')"""
    
    def get_values_stores(topic_name, data):
        if topic_name == 'stores':
            return f"""(
                '{data['_id']}',
                '{data['store_id']}',
                '{data['store_name']}',
                '{data['store_network']}',
                '{data['store_type_description']}',
                '{data['type']}',
                '{'|'.join(data['categories'])}',
                '{data['manager']['name']}',
                '{data['manager']['phone']}',
                '{data['manager']['email']}',
                '{data['location']['country']}',
                '{data['location']['city']}',
                '{data['location']['street']}',
                '{data['location']['house']}',
                '{data['location']['postal_code']}',
                '{data['location']['coordinates']['latitude']}',
                '{data['location']['coordinates']['longitude']}',
                '{data['opening_hours']['mon_fri']}',
                '{data['opening_hours']['sat']}',
                '{data['opening_hours']['sun']}',
                '{data['accepts_online_orders']}',
                '{data['delivery_available']}',
                '{data['warehouse_connected']}',
                '{data['last_inventory_date']}',
                toDateTime('{data['load_date']}'),
                '{data['record_source']}')"""
        
    def get_values_products(topic_name, data):
        if topic_name == 'products':
            return f"""(
                    '{data['_id']}',
                    '{data['id']}',
                    '{data['name']}',
                    '{data['group']}',
                    '{data['description']}',
                    '{data['kbju']['calories']}',
                    '{data['kbju']['protein']}',
                    '{data['kbju']['fat']}',
                    '{data['kbju']['carbohydrates']}',
                    '{data['price']}',
                    '{data['unit']}',
                    '{data['origin_country']}',
                    '{data['expiry_days']}',
                    '{data['is_organic']}',
                    '{data['barcode']}',
                    '{data['manufacturer']['name']}',
                    '{data['manufacturer']['country']}',
                    '{data['manufacturer']['website']}',
                    '{data['manufacturer']['inn']}',
                    toDateTime('{data['load_date']}'),
                    '{data['record_source']}')"""
    
    def get_values_purchases(topic_name, data):
        if topic_name == 'purchases':
            return f"""(
                    '{data['_id']}',
                    '{data['purchase_id']}',
                    '{data['customer']['customer_id']}',
                    '{data['customer']['first_name']}',
                    '{data['customer']['last_name']}',                    
                    '{data['store']['store_id']}',
                    '{data['store']['store_name']}',
                    '{data['store']['store_network']}',
                    '{data['store']['location']['city']}',
                    '{data['store']['location']['street']}',
                    '{data['store']['location']['house']}',
                    '{data['store']['location']['postal_code']}',
                    '{data['store']['location']['coordinates']['latitude']}',
                    '{data['store']['location']['coordinates']['longitude']}',
                    '{json.dumps(data['items'], ensure_ascii=False)}',
                    '{data['total_amount']}',
                    '{data['payment_method']}',
                    '{data['is_delivery']}',
                    '{data['delivery_address']['city']}',
                    '{data['delivery_address']['street']}',
                    '{data['delivery_address']['house']}',
                    '{data['delivery_address']['apartment']}',
                    '{data['delivery_address']['postal_code']}',
                    '{data['purchase_datetime']}',
                    toDateTime('{data['load_date']}'),
                    '{data['record_source']}')"""

    consumer = KafkaConsumer(    
        *topics,
        bootstrap_servers="localhost:9092",
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id="mart_migration",
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    consumer.subscribe(topics)
    
    client = clickhouse_connect.get_client(host='localhost', 
                                           port=f"{os.getenv('CLICKHOUSE_PORT_1', '8123')}", 
                                           username='user_mart', 
                                           password='strongpassword', 
                                           database='mart_raw') 
    try:
        while True:
            messages = consumer.poll(timeout_ms=1000)
            for topic_partition, msg_list in messages.items():
                for msg in msg_list:
                    if msg.topic == 'customers':                
                        batch_customers.append(get_values_customers(msg.topic, msg.value))
                    elif msg.topic == 'stores':
                        batch_stores.append(get_values_stores(msg.topic, msg.value))
                    elif msg.topic == 'products':
                        batch_products.append(get_values_products(msg.topic, msg.value))
                    elif msg.topic == 'purchases':
                        batch_purchases.append(get_values_purchases(msg.topic, msg.value))
    
            now = time.time()
            if len(batch_customers) >= BATCH_SIZE or (len(batch_customers) > 0 and now - last_send_time_customers > MAX_WAIT_TIME):  
                client.command(query_customers + ''.join(batch_customers))         
                consumer.commit()
                logger.info(f'Вставлено в таблицу mart_raw.customers {len(batch_customers)} строк')
    
                batch_customers = []
                last_send_time_customers = now

            if len(batch_stores) >= BATCH_SIZE or (len(batch_stores) > 0 and now - last_send_time_stores > MAX_WAIT_TIME):  
                client.command(query_stores + ''.join(batch_stores))         
                consumer.commit()
                logger.info(f'Вставлено в таблицу mart_raw.stores {len(batch_stores)} строк')
    
                batch_stores = []
                last_send_time_stores = now

            if len(batch_products) >= BATCH_SIZE or (len(batch_products) > 0 and now - last_send_time_products > MAX_WAIT_TIME):  
                client.command(query_products + ''.join(batch_products))         
                consumer.commit()
                logger.info(f'Вставлено в таблицу mart_raw.products {len(batch_products)} строк')
    
                batch_products = []
                last_send_time_products = now
            
            if len(batch_purchases) >= BATCH_SIZE or (len(batch_purchases) > 0 and now - last_send_time_purchases > MAX_WAIT_TIME):  
                client.command(query_purchases + ''.join(batch_purchases))         
                consumer.commit()
                logger.info(f'Вставлено в таблицу mart_raw.purchases {len(batch_purchases)} строк')
    
                batch_purchases = []
                last_send_time_purchases = now

            #  ждем минуту и завершаем работу цикла, в нашем случае больше сообщений не будет
            if (len(batch_customers) == 0 and len(batch_products) == 0 and len(batch_purchases) == 0 and len(batch_stores) == 0 and
                now - last_send_time_customers >= 60 and now - last_send_time_products >= 60 and now - last_send_time_purchases >= 60 and
                now - last_send_time_stores >= 60):
                logger.info("Все данные получены и отправлены в clickhouse")
                break


    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        consumer.close()
        client.close()
        logger.info("Миграция завершена")

if __name__ == "__main__":
    logger.setLevel(logging.INFO)
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    consumer_to_clickhouse()