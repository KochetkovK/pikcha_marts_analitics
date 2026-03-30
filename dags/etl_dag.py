import boto3
import os
import sys
import json
import logging
import clickhouse_connect
import tempfile
import glob
import time

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

from datetime import datetime, date
from dotenv import load_dotenv
from pymongo import MongoClient

from kafka import KafkaProducer
from kafka import KafkaConsumer

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

path = "/opt/airflow/"
# Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
os.environ['PROJECT_PATH'] = path
# Добавим путь к коду проекта в $PATH, чтобы импортировать функции
sys.path.insert(0, path)

from modules.handler_data import handler_data

# Загружаем переменные окружения
load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo_db/")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse_final_task")
CLICKHOUSE_PORT = "8123"
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "user_mart")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "strongpassword")
CLICKHOUSE_DATABASE_RAW = os.getenv("CLICKHOUSE_DATABASE_RAW", "mart_raw")
S3_ENDPOINT = "https://s3.ru-7.storage.selcloud.ru"
S3_ACCESS = os.getenv("access_key")
S3_SECRET = os.getenv("secret")
S3_BUCKET = "data-engineer-s3-practice"

def producer_mongo_to_kafka():
    try:
        producer = KafkaProducer(
                        bootstrap_servers=KAFKA_BROKER,
                        value_serializer=lambda v: json.dumps(v).encode('utf-8')
                    )
        
        # Подключение к MongoDB
        client = MongoClient(MONGO_URI)
        db = client['shop_database']
        collections = ['customers', 'stores', 'products', 'purchases']

        for collection in collections:
            #Проверим, что бы в коллекции были данные
            count_doc = db[collection].count_documents({})
            logging.info(f"В коллекции: {collection} {count_doc} документов")

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

            logging.info(f"Отправлено в топик: {collection} {count} документов") 
    
        producer.flush()                        

    except Exception as e:
        logging.error(f"Error: {e}")
        raise # Поднимем ошибку, чтобы таска упала
    
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
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id="mart_migration",
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    consumer.subscribe(topics)
    
    client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, 
                                           port=CLICKHOUSE_PORT, 
                                           username=CLICKHOUSE_USER, 
                                           password=CLICKHOUSE_PASSWORD, 
                                           database=CLICKHOUSE_DATABASE_RAW) 
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
                logging.info(f'Вставлено в таблицу mart_raw.customers {len(batch_customers)} строк')
    
                batch_customers = []
                last_send_time_customers = now

            if len(batch_stores) >= BATCH_SIZE or (len(batch_stores) > 0 and now - last_send_time_stores > MAX_WAIT_TIME):  
                client.command(query_stores + ''.join(batch_stores))         
                consumer.commit()
                logging.info(f'Вставлено в таблицу mart_raw.stores {len(batch_stores)} строк')
    
                batch_stores = []
                last_send_time_stores = now

            if len(batch_products) >= BATCH_SIZE or (len(batch_products) > 0 and now - last_send_time_products > MAX_WAIT_TIME):  
                client.command(query_products + ''.join(batch_products))         
                consumer.commit()
                logging.info(f'Вставлено в таблицу mart_raw.products {len(batch_products)} строк')
    
                batch_products = []
                last_send_time_products = now
            
            if len(batch_purchases) >= BATCH_SIZE or (len(batch_purchases) > 0 and now - last_send_time_purchases > MAX_WAIT_TIME):  
                client.command(query_purchases + ''.join(batch_purchases))         
                consumer.commit()
                logging.info(f'Вставлено в таблицу mart_raw.purchases {len(batch_purchases)} строк')
    
                batch_purchases = []
                last_send_time_purchases = now

            #  ждем минуту и завершаем работу цикла, в нашем случае больше сообщений не будет
            if (len(batch_customers) == 0 and len(batch_products) == 0 and len(batch_purchases) == 0 and len(batch_stores) == 0 and
                now - last_send_time_customers >= 60 and now - last_send_time_products >= 60 and now - last_send_time_purchases >= 60 and
                now - last_send_time_stores >= 60):
                logging.info("Все данные получены и отправлены в clickhouse")
                break
        logging.info("Миграция завершена")


    except Exception as e:
        logging.error(f"Error: {e}")
        raise 
    finally:
        consumer.close()
        client.close()
        
def run_spark_job(**context):
    # Достаем IP из XCom предыдущей таски
    ch_ip = context['ti'].xcom_pull(task_ids='get_clickhouse_ip')
    
    if not ch_ip:
        raise ValueError("Не удалось получить IP ClickHouse!")

    jdbc_url = f"jdbc:clickhouse://{ch_ip.strip()}:8123/mart"

    #current_data = date.today().strftime('%Y_%m_%d')
    #file_name =  f"analytic_result_{current_data}"
    out_dir = tempfile.mkdtemp(prefix="analytic_result")
    logging.info(f"OUT_DIR: {out_dir}")
    
    #SparkSession.getActiveSession().stop() if SparkSession.getActiveSession() else None

    spark = SparkSession.builder \
        .appName("Airflow_PySpark_ETL") \
        .master("local[*]") \
        .config("spark.jars", "/opt/spark/jars/clickhouse-jdbc-0.6.3.jar") \
        .config("spark.driver.extraJavaOptions", "-Dsun.net.inetaddr.ttl=0") \
        .config("spark.executor.extraJavaOptions", "-Dsun.net.inetaddr.ttl=0") \
        .getOrCreate()
    
    jdbc_url = f"jdbc:clickhouse://{ch_ip.strip()}:8123/mart"
    properties = {
    "user": "user_mart",
    "password": "strongpassword",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"}   

    # так как фоновые процессы слияния частей данных могут происходить в неопределенное время, 
    # и движок ReplacingMergeTree может не успеть удалить дубликаты, 
    # мы отфильтруем и возьмем последние загруженные данные
    sql_query_purchases = """(
    SELECT purchase_id,
           store_id,
           customer_id,
           product_id,
           total_amount,
           payment_method,
           is_delivery,
           purchase_datetime,
           load_date,
    FROM mart.purchases 
    WHERE load_date = (SELECT max(load_date) FROM mart.purchases)
    )"""
    df_purchases = spark.read.jdbc(
        url=jdbc_url, 
        table=sql_query_purchases, 
        properties=properties
    )

    sql_query_products = """(
    SELECT product_id,
           product_name,
           category_name,
           is_organic,
           load_date
    FROM mart.products 
    WHERE load_date = (SELECT max(load_date) FROM mart.products)
    )"""
    df_products = spark.read.jdbc(
        url=jdbc_url, 
        table=sql_query_products, 
        properties=properties
    )

    sql_query_customers = """(
    SELECT customer_id,
           registration_date,
           is_loyalty_member,
           load_date
    FROM mart.customers 
    WHERE load_date = (SELECT max(load_date) FROM customers)
    )"""
    df_customers = spark.read.jdbc(
        url=jdbc_url, 
        table=sql_query_customers, 
        properties=properties
    )

    sql_query_stores = """(
    SELECT store_id, 
           address_id, 
           load_date 
    FROM mart.stores
    WHERE load_date = (SELECT max(load_date) FROM mart.stores)
    )"""
    df_stores = spark.read.jdbc(
        url=jdbc_url,
        table=sql_query_stores, 
        properties=properties
    )

    sql_query_address = """(
    SELECT address_id,
           city,
           load_date
    FROM mart.address
    WHERE load_date = (SELECT max(load_date) FROM mart.address)
    )"""
    df_address = spark.read.jdbc(
        url=jdbc_url, 
        table=sql_query_address, 
        properties=properties)
    
    # Объединяем все таблицы в одну, берем необходимые колонки и кэшируем
    df_final = df_customers.alias("c") \
        .join(df_purchases.alias("pu"), df_customers.customer_id == df_purchases.customer_id , "left") \
        .join(df_products.alias("pr"), df_purchases.product_id == df_products.product_id, "left") \
        .join(df_stores.alias("s"), df_purchases.store_id == df_stores.store_id, "left") \
        .join(df_address.alias("a"), df_stores.address_id == df_address.address_id, "left") \
        .select(
            "pu.purchase_id",
            "pu.store_id",
            "c.customer_id",
            "pu.product_id",
            "pr.category_name",
            "pr.is_organic",
            "total_amount",
            "payment_method",
            "is_delivery",
            "a.city",
            "purchase_datetime",
            F.to_date(F.col("c.registration_date")).alias("registration_date"),
            "c.is_loyalty_member",
            "pu.load_date") \
        .cache() 
    
    df_matrix_features = df_final.groupBy("customer_id") \
        .agg(F.max(
                # Покупал молочные продукты за последние 30 дней
                F.when((F.col("category_name") == "молочные продукты") &
                       (F.col("purchase_datetime") >= F.date_sub(F.current_date(), 30)), True) 
                    .otherwise(False)).alias("bought_milk_last_30d"),
            # Покупал фрукты и ягоды за последние 14 дней
            F.max(            
                F.when((F.col("category_name") == "фрукты и ягоды") &
                       (F.col("purchase_datetime") >= F.date_sub(F.current_date(), 14)), True) 
                    .otherwise(False)).alias("bought_fruits_last_14d"),
            # Количество покупок овощей и зелени за последние 14 дней
            F.count(
                F.when((F.col("category_name") == "овощи и зелень") &
                       (F.col("purchase_datetime") >= F.date_sub(F.current_date(), 14)), F.col("product_id")) 
                    ).alias("count_purchase_vegetables_14"),
            # Количество покупок за последние 30 дней, что бы потом определить делал ли клиент больше 2 покупок за последние 30 дней                        
            F.countDistinct(
                F.when(F.col("purchase_datetime") >= F.date_sub(F.current_date(), 30), 
                                    F.col("purchase_id"))).alias("count_purchase_30d"),
            # Количество покупок 
            F.countDistinct(F.col("purchase_id")).alias("count_purchase"),
            # Используется для loyal_customer
            F.max("is_loyalty_member").alias("is_loyalty_member"),
            # Дата последней покупки
            F.max("purchase_datetime").alias("last_purchase"),
            # Покупатель зарегистрировался менее 30 дней назад
            F.max(
                F.when((F.col("registration_date") >= F.date_sub(F.current_date(), 30)), True)
                    .otherwise(False)).alias("new_customer"),
            # Пользовался доставкой хотябы раз
            F.max(F.when(F.col("is_delivery") == True, True).otherwise(False)).alias("delivery_user"),
            # Купил хотя бы 1 органический продукт
            F.max(F.when(F.col("is_organic") == True, True).otherwise(False)).alias("organic_preference"),
            # Средняя корзина > 1000 р
            F.when(F.avg("total_amount") > 1000, True).otherwise(False).alias("bulk_buyer"),
            # Средняя корзина < 200 р
            F.when(F.avg("total_amount") < 200, True).otherwise(False).alias("low_cost_buyer"),
            # Покупал хлеб/выпечку хотя бы раз
            F.max(            
                F.when((F.col("category_name") == "зерновые и хлебобулочные изделия"), True)
                    .otherwise(False)).alias("buys_bakery"),
            # Количество городов в которых покупатель делает покупки
            F.countDistinct(F.col("city")).alias("count_city"),
            # Покупал мясо/рыбу/яйца за последнюю неделю
            F.max(            
                F.when((F.col("category_name") == "мясо, рыба, яйца и бобовые") &
                       (F.col("purchase_datetime") >= F.date_sub(F.current_date(), 7)), True) 
                    .otherwise(False)).alias("bought_meat_last_week"),
            # Делал покупки после 20:00
            F.max(
                F.when((F.hour(F.col("purchase_datetime")) > 20), True).otherwise(False)).alias("night_shopper"),
            # Делал покупки до 10:00
            F.max(
                F.when((F.hour(F.col("purchase_datetime")) < 10), True).otherwise(False)).alias("morning_shopper"),
            # Количество покупок, оплаченные наличными
            F.countDistinct(
                F.when(F.col("payment_method") == "cash", F.col("purchase_id"))).alias("count_purchase_cash"),
            # Количество покупок, оплаченные картой
            F.countDistinct(
                F.when(F.col("payment_method") == "card", F.col("purchase_id"))).alias("count_purchase_card"),
            # Количество покупок в выходной день
            F.countDistinct(
                F.when((F.dayofweek(F.col("purchase_datetime")) == 1) | (F.dayofweek(F.col("purchase_datetime")) == 7), 
                       F.col("purchase_id"))).alias("count_purchase_weekend"),
            # Количество категорий в покупках
            F.countDistinct("category_name").alias("count_category_name"),
            # Количество магазинов в которые ходит клиент
            F.countDistinct("store_id").alias("count_store"),
            # Покупка в промежутке между 12 и 15 часами дня
            F.max(
                F.when((F.hour(F.col("purchase_datetime"))).between(12, 14), True).otherwise(False)).alias("early_bird"),
            # Купил на сумму > 2000р за последние 7 дней
            F.max(            
                F.when((F.col("total_amount") >= 2000) &
                       (F.col("purchase_datetime") >= F.date_sub(F.current_date(), 7)), True) 
                    .otherwise(False)).alias("recent_high_spender"),
            # Количество покупок с фруктами за последние 30 дней
            F.countDistinct(
                F.when((F.col("purchase_datetime") >= F.date_sub(F.current_date(), 30)) &
                       (F.col("category_name") == "фрукты и ягоды"),
                F.col("category_name"))).alias("count_purchase_fruit_30d"),
            # Количество купленных мясных продуктов за 90 дней
            F.count(
                F.when((F.col("category_name") == "мясо, рыба, яйца и бобовые") & 
                       (F.col("purchase_datetime") >= F.date_sub(F.current_date(), 90)), F.col("product_id")) 
                    ).alias("count_meat_90"),
            # Количество покупок с одним товаром в корзине
            # Присоединим к этому датафрейму расчет количества товаров в корзине
        ).join(
            df_final.groupBy("customer_id", "purchase_id") \
                .agg(
                    # Количество товаров в корзине
                    F.count(F.col("product_id")).alias("count_items")
                ).groupBy("customer_id") \
                .agg(
                    # Количество покупок с одним товаром
                    F.count(F.when(F.col("count_items") == 1, F.col("count_items"))).alias("count_purchase_items_1"),
                    # Среднее количество товаров в корзине
                    F.avg(F.col("count_items")).alias("avg_items")
        ),
            "customer_id", "left" 
    
        # Не покупал овощи и зелень за последние 14 дней
        ).withColumn(
            "not_bought_veggies_14d",
            (F.col("count_purchase_vegetables_14") == 0) 
        # Делал более 2 покупок за последние 30 дней
        ).withColumn(
             "recurrent_buyer", F.col("count_purchase_30d") > 2
        # Не покупал 14-30 дней (ушедший клиент?)
        ).withColumn(
            "inactive_14_30", 
            (F.col("last_purchase") < F.date_sub(F.current_date(), 14)) &
            (F.col("last_purchase") >= F.date_sub(F.current_date(), 30))
        # Лояльный клиент (карта и >= 3 покупки)
        ).withColumn(
            "loyal_customer",
            ((F.col("count_purchase") >= 3) & (F.col("is_loyalty_member") == True))
        # Делал покупки в разных городах
        ).withColumn(
            "multycity_buyer",
            F.col("count_city") >= 2
        # Оплачивал наличными >= 70% покупок
        ).withColumn(
            "prefers_cash",
            (F.col("count_purchase_cash") / F.col("count_purchase") >= 0.7)
        # Оплачивал картой >= 70% покупок
        ).withColumn(
            "prefers_card",
            (F.col("count_purchase_card") / F.col("count_purchase") >= 0.7)
        # Делал >= 60% покупок в выходные
        ).withColumn(
            "weekend_shopper",
            (F.col("count_purchase_weekend") / F.col("count_purchase") >= 0.6)
        # Делал >= 60% покупок в будни
        ).withColumn(
            "weekday_shopper",
            ((F.col("count_purchase") - F.col("count_purchase_weekend")) / F.col("count_purchase") >= 0.6)
        # >= 50% покупок - 1 товар в корзине
        ).withColumn(
            "single_item_buyer",
            (F.col("count_purchase_items_1") / F.col("count_purchase") >= 0.5)
        # Покупал >= 4 разных категорий продуктов
        ).withColumn(
            "varied_shopper",
            (F.col("count_category_name") >= 4)
        # Ходит только в один магазин
        ).withColumn(
            "store_loyal",
            (F.col("count_store") == 1)
        # Ходит в разные магазины
        ).withColumn(
            "switching_loyal",
            (F.col("count_store") > 1)
        # Среднее количество позиций в корзине >= 4
        ).withColumn(
            "family_shopper",
            (F.col("avg_items") >= 4)
        # Не совершал ни одной покупки (только регистрация)
        ).withColumn(
            "no_purchases",
            (F.col("count_purchase") == 0)
        # >= 3 покупок фруктов за 30 дней
        ).withColumn(
            "fruit_lover",
            (F.col("count_purchase_fruit_30d") >= 3)
        # Не купил ни одного мясного продукта за 90 дней
        ).withColumn(
            "vegetarian_profile",
            (F.col("count_meat_90") == 0)
        )
    
    df_res = df_matrix_features.select(
        "customer_id", 
        "bought_milk_last_30d", # Покупал молочные продукты за последние 30 дней
        "bought_fruits_last_14d", # Покупал фрукты и ягоды за последние 14 дней
        "not_bought_veggies_14d", # Не покупал овощи и зелень за последние 14 дней
        "recurrent_buyer", # Делал более 2 покупок за последние 30 дней
        "inactive_14_30", # Не покупал 14-30 дней (ушедший клиент?)
        "new_customer", # Покупатель зарегистрировался менее 30 дней назад
        "delivery_user", # Пользовался доставкой хотябы раз
        "organic_preference", # Купил хотя бы 1 органический продукт
        "bulk_buyer", # Средняя корзина > 1000 р
        "low_cost_buyer", # Средняя корзина < 200 р
        "buys_bakery", # Покупал хлеб/выпечку хотя бы раз
        "loyal_customer", # Лояльный клиент (карта и >= 3 покупки)
        "multycity_buyer", # Делал покупки в разных городах
        "bought_meat_last_week", # Покупал мясо/рыбу/яйца за последнюю неделю
        "night_shopper", # Делал покупки после 20:00
        "morning_shopper", # Делал покупки до 10:00,
        "prefers_cash", # Оплачивал наличными >= 70% покупок
        "prefers_card", # Оплачивал картой >= 70% покупок
        "weekend_shopper", # Делал >= 60% покупок в выходные
        "weekday_shopper", # Делал >= 60% покупок в будни
        "single_item_buyer",  # >= 50% покупок - 1 товар в корзине
        "varied_shopper", # Покупал >= 4 разных категорий продуктов
        "store_loyal", # Ходит только в один магазин
        "switching_loyal", # Ходит в разные магазины
        "family_shopper", # Среднее количество позиций в корзине >= 4
        "early_bird", # Покупка в промежутке между 12 и 15 часами дня
        "no_purchases", # Не совершал ни одной покупки (только регистрация)
        "recent_high_spender", # Купил на сумму > 2000р за последние 7 дней 
        "fruit_lover", # >= 3 покупок фруктов за 30 дней
        "vegetarian_profile" # Не купил ни одного мясного продукта за 90 дней
    ).fillna(False).orderBy("customer_id")
    
    df_res.show(5, truncate=False, vertical=True)

    df_res.coalesce(1) \
            .write.format("csv") \
            .mode("overwrite") \
            .option("header", "true") \
            .save(out_dir)


    df_final.unpersist()
    spark.stop()

    return out_dir#, file_name

def ensure_bucket(s3):
    # Создаём bucket, если его нет
    names = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    
    if S3_BUCKET not in names:
        s3.create_bucket(Bucket=S3_BUCKET)

def load_to_s3(**context):
    # Загружаем parquet-файл в S3
    ti = context["ti"]
    out_dir = ti.xcom_pull(task_ids="run_pyspark_task")
    logging.info(out_dir)
    if not out_dir:
        return

    files = glob.glob(os.path.join(out_dir, "part-*.csv"))
    logging.info(files)
    if not files:
        return
    
    session = boto3.Session(
            aws_access_key_id=S3_ACCESS,
            aws_secret_access_key=S3_SECRET,
        )        
    s3_client = session.client('s3', endpoint_url=S3_ENDPOINT, verify=False)

    ensure_bucket(s3_client)
    
    current_date = date.today().strftime('%Y_%m_%d')
    key = f"analitic_for_pikcha_marts/analytic_result_{current_date}.csv"
   
    s3_client.upload_file(files[0],
                          S3_BUCKET,
                          (key))
    
    logging.info(f"Результат загружен в S3 - {S3_BUCKET + '/' + key}")



def check_db_exists():
    hook = ClickHouseHook(clickhouse_conn_id='clickhouse_connection')
    # Проверяем базу
    result = hook.execute("SELECT count() FROM system.databases WHERE name = 'mart'")
    logging.info(result)
    exists = result[0][0] > 0
    
    if exists:
        return 'already_exists_db' # Имя таски для ветки "База есть"
    else:
        return 'setup_clickhouse_task'     # Имя таски для ветки "Базы нет"
    
def init_db():
    hook = ClickHouseHook(clickhouse_conn_id='clickhouse_connection')

    with open('dags/mart_clickhouse.sql', 'r', encoding='utf-8') as f:
        sql_commands = f.read().split(';')
        
    for command in sql_commands:
        clean_command = command.strip()
        if clean_command: 
            hook.execute(clean_command)
    
def setup_clickhouse():
    hook = ClickHouseHook(clickhouse_conn_id='clickhouse_connection')
    
    # Создание баз данных
    hook.execute("CREATE DATABASE IF NOT EXISTS mart_raw")
    hook.execute("CREATE DATABASE IF NOT EXISTS mart")


#======DAG=======
with DAG(
        dag_id="etl_dag",
        start_date=datetime(2026, 3, 29),        
        schedule_interval='0 10 * * *',
        catchup=False,
        tags=["mongo", "kafka", "clickhouse", "spark", "s3", "csv"],
) as dag:
    
    branch_task = BranchPythonOperator(
        task_id='check_db_branch',
        python_callable=check_db_exists
    ) 
    # Инициализация базы в ClickHouse (выполнится, если баз НЕТ)
    setup_clickhouse_task = PythonOperator(
        task_id='setup_clickhouse_task',
        python_callable=setup_clickhouse
    )

    init_db_task = PythonOperator(
        task_id='init_db',
        python_callable=init_db
    )
    # Заглушка (выполнится, если база ЕСТЬ)
    already_exists_db = EmptyOperator(
       task_id="already_exists_db"
    )
    # Извлекаем данные из Mongo_db
    extract_mongo_db = PythonOperator(
        task_id="extract_mongo_db",
        python_callable=producer_mongo_to_kafka,
        trigger_rule='none_failed_min_one_success'
    )
    # Загружаем данные в ClickHouse
    load_clickhouse = PythonOperator(
        task_id="load_clickhouse",
        python_callable=consumer_to_clickhouse
    )
    # у JVM есть проблемы с разрешением DNS-имен Docker-сети
    # пришлось напрямую подставлять IP в url строку
    get_clickhouse_ip = BashOperator(
    task_id='get_clickhouse_ip',
    # getent hosts выведет строку вида "172.18.0.4 clickhouse_final_task"
    # awk '{print $1}' заберет только первую часть (IP)
    bash_command="getent hosts clickhouse_final_task | awk '{print $1}'",
    do_xcom_push=True
    )

    run_pyspark_task = PythonOperator(
        task_id="run_pyspark_task",
        python_callable=run_spark_job,
        provide_context=True
    )

    load_to_s3_task = PythonOperator(
        task_id ="load_to_s3_task",
        python_callable=load_to_s3,
        provide_context=True
    )    


branch_task >> [setup_clickhouse_task, already_exists_db] 
setup_clickhouse_task >> init_db_task
([init_db_task, already_exists_db] >> extract_mongo_db >> load_clickhouse >> 
 get_clickhouse_ip >> run_pyspark_task >> load_to_s3_task)

