-- DROP DATABASE IF EXISTS mart_raw;
-- DROP DATABASE IF EXISTS mart;
CREATE DATABASE IF NOT EXISTS mart_raw;
CREATE DATABASE IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart_raw.customers (
    record_id String,
    customer_id String,
    first_name String,
    last_name String,
    email String,
    phone String,
    birth_date String,
    gender String,
    registration_date String,
    is_loyalty_member String,
    loyalty_card_number String,
    purchase_location_country String,
    purchase_location_city String,
    purchase_location_street String,
    purchase_location_house String,
    purchase_location_postal_code String,
    purchase_location_coordinates_latitude String,
    purchase_location_coordinates_longitude String,
    delivery_address_country String,
    delivery_address_city String,
    delivery_address_street String,
    delivery_address_house String,
    delivery_address_apartment String,
    delivery_address_postal_code String,
    preferences_preferred_language String,
    preferences_preferred_payment_method String,
    preferences_receive_promotions String,
    load_date DateTime,
    record_source String)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(load_date)
ORDER BY (load_date, customer_id)
TTL load_date + INTERVAL 1 YEAR DELETE
COMMENT 'Таблица клиентов для сырых данных';

CREATE TABLE IF NOT EXISTS mart_raw.products (
    record_id String,
    product_id String,
    name String,
    "group" String,
    description String,
    kbju_calories String,
    kbju_protein String,
    kbju_fat String,
    kbju_carbohydrates String,
    price String,
    unit String,
    origin_country String,
    expiry_days String,
    is_organic String,
    barcode String,
    manufacturer_name String,
    manufacturer_country String,
    manufacturer_website String,
    manufacturer_inn String,
    load_date DateTime,
    record_source String)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(load_date)
ORDER BY (load_date, product_id)
TTL load_date + INTERVAL 1 YEAR DELETE
COMMENT 'Таблица товаров для сырых данных';

CREATE TABLE IF NOT EXISTS mart_raw.stores (
    record_id String,
    store_id String,
    store_name String,
    store_network String,
    store_type_description String,
    "type" String,
    categories String,
    manager_name String,
    manager_phone String,
    manager_email String,
    location_country String,
    location_city String,
    location_street String,
    location_house String,
    location_postal_code String,
    location_coordinates_latitude String,
    location_coordinates_longitude String,
    opening_hours_mon_fri String,
    opening_hours_sat String,
    opening_hours_sun String,
    accepts_online_orders String,
    delivery_available String,
    warehouse_connected String,
    last_inventory_date String,
    load_date DateTime,
    record_source String)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(load_date)
ORDER BY (load_date, store_id)
TTL load_date + INTERVAL 1 YEAR DELETE
COMMENT 'Таблица магазинов для сырых данных';

CREATE TABLE IF NOT EXISTS mart_raw.purchases (
    record_id String,
    purchase_id String,
    customer_customer_id String,
    customer_first_name String,
    customer_last_name String,    
    store_store_id String,
    store_store_name String,
    store_store_network String,    
    store_location_city String,
    store_location_street String,
    store_location_house String,
    store_location_postal_code String,
    store_location_coordinates_latitude String,
    store_location_coordinates_longitude String,
    items String,
    total_amount String,
    payment_method String,
    is_delivery String,
    delivery_address_city String,
    delivery_address_street String,
    delivery_address_house String,
    delivery_address_apartment String,
    delivery_address_postal_code String,
    purchase_datetime String,
    load_date DateTime,
    record_source String)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(load_date)
ORDER BY (load_date, purchase_id)
TTL load_date + INTERVAL 1 YEAR DELETE
COMMENT 'Таблица покупок для сырых данных';

-- Таблица всех адресов
CREATE TABLE IF NOT EXISTS mart.address (
    address_id UInt64,
    country LowCardinality(String),
    city LowCardinality(String),
    street String,
    house String,
    apartment String COMMENT 'Может быть пропущенным значением, у магазина номера квартиры нет',
    postal_code String,
    coordinates_latitude Float64 COMMENT 'Может быть пропущенным значением',
    coordinates_longitude Float64 COMMENT 'Может быть пропущенным значением',
    load_date DateTime)
ENGINE = ReplacingMergeTree(load_date)
ORDER BY (country, city, street, house, postal_code)
COMMENT 'Справочник адресов';

-- Материализованное представление для таблицы адресов из таблицы клиентов, адрес покупки
CREATE MATERIALIZED VIEW mart.mv_address_from_purchase_location
TO mart.address
AS 
-- Вычисляем id детерминировано из данных, что бы не было ситуации, когда в таблице address address_id есть,
-- а в других таблицах где будет использован address_id его нет
SELECT sipHash64(lowerUTF8(trim(purchase_location_country)),
	             lowerUTF8(trim(purchase_location_city)),
	             lowerUTF8(trim(purchase_location_street)),
	             lowerUTF8(trim(purchase_location_house)),
	             lowerUTF8(trim(purchase_location_postal_code))) AS address_id,
       lowerUTF8(trim(purchase_location_country)) AS country,
	   lowerUTF8(trim(purchase_location_city)) AS city,
	   lowerUTF8(trim(purchase_location_street)) AS street,
	   lowerUTF8(trim(purchase_location_house)) AS house,
	   lowerUTF8(trim(purchase_location_postal_code)) AS postal_code,
	   toFloat64(trim(purchase_location_coordinates_latitude)) AS coordinates_latitude,
	   toFloat64(trim(purchase_location_coordinates_longitude)) AS coordinates_longitude,
	   load_date
FROM mart_raw.customers
WHERE purchase_location_country IS NOT NULL AND purchase_location_country != ''
      AND purchase_location_city IS NOT NULL AND purchase_location_city != ''
	  AND purchase_location_street IS NOT NULL AND purchase_location_street != ''
	  AND purchase_location_house IS NOT NULL AND purchase_location_house != ''
	  AND purchase_location_postal_code IS NOT NULL AND purchase_location_postal_code != '';

-- Материализованное представление для таблицы адресов из таблицы клиентов, адрес доставки
CREATE MATERIALIZED VIEW mart.mv_address_from_delivery_address
TO mart.address
AS 
SELECT sipHash64(lowerUTF8(trim(delivery_address_country)),
	             lowerUTF8(trim(delivery_address_city)),
	             lowerUTF8(trim(delivery_address_street)),
	             lowerUTF8(trim(delivery_address_house)),
	             lowerUTF8(trim(delivery_address_apartment)),
	             lowerUTF8(trim(delivery_address_postal_code)))	AS address_id,   
       lowerUTF8(trim(delivery_address_country)) AS country,
	   lowerUTF8(trim(delivery_address_city)) AS city,
	   lowerUTF8(trim(delivery_address_street)) AS street,
	   lowerUTF8(trim(delivery_address_house)) AS house,
	   lowerUTF8(trim(delivery_address_apartment)) AS apartment,
	   lowerUTF8(trim(delivery_address_postal_code)) AS postal_code,
	   load_date
FROM mart_raw.customers
WHERE delivery_address_country IS NOT NULL AND delivery_address_country != ''
      AND delivery_address_city IS NOT NULL AND delivery_address_city != ''
	  AND delivery_address_street IS NOT NULL AND delivery_address_street != ''
	  AND delivery_address_house IS NOT NULL AND delivery_address_house != ''
	  AND delivery_address_postal_code IS NOT NULL AND delivery_address_postal_code != '';

-- Материализованное представление для таблицы адресов из таблицы магазинов
CREATE MATERIALIZED VIEW mart.mv_address_from_stores
TO mart.address
AS 
SELECT sipHash64(lowerUTF8(trim(location_country)),
                 lowerUTF8(trim(location_city)),
                 lowerUTF8(trim(location_street)),
                 lowerUTF8(trim(location_house)),
                 lowerUTF8(trim(location_postal_code))) AS address_id,       
       lowerUTF8(trim(location_country)) AS country,
       lowerUTF8(trim(location_city)) AS city,
       lowerUTF8(trim(location_street)) AS street,
       lowerUTF8(trim(location_house)) AS house,
       lowerUTF8(trim(location_postal_code)) AS postal_code,
       toFloat64(trim(location_coordinates_latitude)) AS coordinates_latitude,
       toFloat64(trim(location_coordinates_longitude)) AS coordinates_longitude
FROM mart_raw.stores
WHERE location_country IS NOT NULL AND location_country != ''
      AND location_city IS NOT NULL AND location_city != ''
	  AND location_street IS NOT NULL AND location_street != ''
	  AND location_house IS NOT NULL AND location_house != ''
	  AND location_postal_code IS NOT NULL AND location_postal_code != '';

-- Таблица производителей товаров
CREATE TABLE IF NOT EXISTS mart.manufacturers (
    manufacturer_id UInt64,
    manufacturer_name String,
    country String,
    website String, 
    inn String,
    load_date DateTime)
ENGINE = ReplacingMergeTree(load_date)
ORDER BY (inn)
COMMENT 'Справочник производителей товаров';

-- Материализованное представление для таблицы производителей из таблицы продуктов
CREATE MATERIALIZED VIEW mart.mv_manufacturers_from_products
TO mart.manufacturers
AS
SELECT sipHash64(lowerUTF8(trim(manufacturer_name)),
                 lowerUTF8(trim(manufacturer_inn))) AS manufacturer_id,
       lowerUTF8(trim(manufacturer_name)) AS manufacturer_name,
       lowerUTF8(trim(manufacturer_country)) AS country,
       lowerUTF8(trim(manufacturer_website)) AS website,
       lowerUTF8(trim(manufacturer_inn)) AS inn,
       load_date
FROM mart_raw.products
WHERE manufacturer_name IS NOT NULL AND manufacturer_name != ''
      AND manufacturer_inn IS NOT NULL AND manufacturer_inn != ''
      AND match(manufacturer_inn, '^[0-9]{10}$'); 

-- Таблица продуктов
CREATE TABLE IF NOT EXISTS mart.products (
    product_id String,
    product_name String,
    category_name String,
    description String,
    kbju_calories Float32,
    kbju_protein Float32,
    kbju_fat Float32,
    kbju_carbohydrates Float32,
    unit LowCardinality(String),
    price Decimal(18, 2),
    origin_country LowCardinality(String),
    expiry_days SmallInt,
    is_organic Boolean,
    barcode String,
    manufacturer_id UInt64,
    load_date DateTime)
ENGINE = ReplacingMergeTree(load_date)
ORDER BY (product_id)
COMMENT 'Каталог товаров';

-- Материализованное представление для таблицы продуктов из таблицы продуктов

CREATE MATERIALIZED VIEW mart.mv_products_from_products 
TO mart.products
AS   
SELECT product_id,
       lowerUTF8(trim(name)) AS product_name,
       lowerUTF8(trimLeft(trimLeft("group", '🥖, 🥩, 🥛'), '🍏 ,🥦')) AS category_name,
       lowerUTF8(trim(description)) AS description,
       kbju_calories,
       kbju_protein,
       kbju_fat,
       kbju_carbohydrates,
       lowerUTF8(trim(unit)) AS unit,
       toDecimal64(price, 2) AS price,
       lowerUTF8(trim(origin_country)) AS origin_country,
       expiry_days,
       is_organic,
       lowerUTF8(trim(barcode)) AS barcode,
       sipHash64(lowerUTF8(trim(manufacturer_name)),
                 lowerUTF8(trim(manufacturer_inn))) AS manufacturer_id,
       load_date AS load_date
FROM mart_raw.products
WHERE product_id IS NOT NULL AND product_id != ''
  AND barcode IS NOT NULL AND barcode != ''
  AND unit IS NOT NULL AND unit != ''
  AND toDecimal64(price, 2) > 0;
  
-- Таблица клиентов
CREATE TABLE IF NOT EXISTS mart.customers (
    customer_id String,
    first_name String,
    last_name String,
    email String,
    phone String, 
    birth_date Date,
    gender LowCardinality(String),
    registration_date DateTime,
    is_loyalty_member Boolean,
    loyalty_card_number String,
    purchase_address_id UInt64,
    delivery_address_id UInt64,
    preferences_preferred_language LowCardinality(String),
    preferences_preferred_payment_method LowCardinality(String),
    preferences_receive_promotions Boolean,
    load_date DateTime)
ENGINE = ReplacingMergeTree(load_date)
ORDER BY (customer_id)
COMMENT 'Таблица клиентов';

-- Материализованное представление для таблицы клиентов из таблицы клиентов
CREATE MATERIALIZED VIEW mart.mv_customers_from_customers
TO mart.customers
AS
SELECT customer_id,
       lowerUTF8(trim(first_name)) AS first_name,
       lowerUTF8(trim(last_name)) AS last_name,
       email,
       phone, 
       toDate(birth_date) AS birth_date,
       lowerUTF8(trim(gender)) AS gender,
       parseDateTimeBestEffort(registration_date) AS registration_date,
       is_loyalty_member,
       loyalty_card_number,
       sipHash64(lowerUTF8(trim(purchase_location_country)),
	             lowerUTF8(trim(purchase_location_city)),
	             lowerUTF8(trim(purchase_location_street)),
	             lowerUTF8(trim(purchase_location_house)),
	             lowerUTF8(trim(purchase_location_postal_code))) AS purchase_address_id,
       sipHash64(lowerUTF8(trim(delivery_address_country)),
	             lowerUTF8(trim(delivery_address_city)),
	             lowerUTF8(trim(delivery_address_street)),
	             lowerUTF8(trim(delivery_address_house)),
	             lowerUTF8(trim(delivery_address_apartment)),
	             lowerUTF8(trim(delivery_address_postal_code))) AS delivery_address_id,
       lowerUTF8(trim(preferences_preferred_language)) AS preferences_preferred_language,
       lowerUTF8(trim(preferences_preferred_payment_method)) AS preferences_preferred_payment_method,
       preferences_receive_promotions,
       load_date
FROM mart_raw.customers
WHERE customer_id IS NOT NULL AND customer_id != ''
  AND first_name IS NOT NULL AND first_name != ''
  AND toDate(birth_date) < toDate(load_date)
  AND toDate(registration_date) <= toDate(load_date)
  AND is_loyalty_member = 'True'
  AND loyalty_card_number IS NOT NULL AND loyalty_card_number != '';
  
-- Таблица магазинов
CREATE TABLE IF NOT EXISTS mart.stores (
    store_id String,
    store_name String,
    store_network String,
    store_type_description String,
    "type" LowCardinality(String),
    categories Array(String),
    manager_name String,
    manager_phone String,
    manager_email String,
    address_id UInt64,
    opening_hours_mon_fri String,
    opening_hours_sat String,
    opening_hours_sun String,
    accepts_online_orders Boolean,
    delivery_available Boolean,
    warehouse_connected Boolean,
    last_inventory_date Date,
    load_date DateTime)
ENGINE = ReplacingMergeTree(load_date)
ORDER BY (store_id)
COMMENT 'Таблица магазинов';

-- Материализованное представление для таблицы магазинов из таблицы магазинов
CREATE MATERIALIZED VIEW mart.mv_stores_from_stores
TO mart.stores
AS
SELECT store_id,
       lowerUTF8(trim(store_name)) AS store_name,
       lowerUTF8(trim(store_network)) AS store_network,
       lowerUTF8(trim(store_type_description)) AS store_type_description,
       "type",
       arrayMap(x -> lowerUTF8(trimLeft(trimLeft(x, '🥖, 🥩, 🥛'), '🍏 ,🥦')), splitByString('|', categories)) AS categories,
       lowerUTF8(trim(manager_name)) AS manager_name,
       manager_phone,
       manager_email,
       sipHash64(lowerUTF8(trim(location_country)),
                 lowerUTF8(trim(location_city)),
                 lowerUTF8(trim(location_street)),
                 lowerUTF8(trim(location_house)),
                 lowerUTF8(trim(location_postal_code))) AS address_id,
       opening_hours_mon_fri,
       opening_hours_sat,
       opening_hours_sun,
       accepts_online_orders,
       delivery_available,
       warehouse_connected,
       last_inventory_date,
       load_date
FROM mart_raw.stores
WHERE store_id IS NOT NULL AND store_id != ''
  AND store_name IS NOT NULL AND store_name != '';

-- Таблица покупок
CREATE TABLE IF NOT EXISTS mart.purchases (
    purchase_id String,
    store_id String,
    customer_id String, 
    product_id String,
    quantity Float32,
    unit LowCardinality(String),
    price_per_unit Decimal(18, 2),
    total_item_price Decimal(18, 2),
    total_amount Decimal(18, 2),
    payment_method LowCardinality(String),
    is_delivery Boolean,
    delivery_address_id UInt64,
    purchase_datetime DateTime,
    load_date DateTime)
ENGINE = ReplacingMergeTree(load_date)
ORDER BY (purchase_id, product_id)
COMMENT 'Таблица покупок';

-- Материализованное представление для таблицы покупок из таблицы покупок
CREATE MATERIALIZED VIEW mart.mv_purchases_from_purchases
TO mart.purchases
AS
WITH array_items AS 
	(SELECT purchase_id,
	        store_store_id,
	        customer_customer_id,
	        arrayJoin(JSONExtractArrayRaw(items)) AS item,
	        total_amount,
	        payment_method, 
	        is_delivery,
	        sipHash64(lowerUTF8(trim(delivery_address_city)),
                      lowerUTF8(trim(delivery_address_street)),
                      lowerUTF8(trim(delivery_address_house)),
                      lowerUTF8(trim(delivery_address_apartment)),
                      lowerUTF8(trim(delivery_address_postal_code))) AS delivery_address_id,
	        purchase_datetime,
	        load_date
	FROM mart_raw.purchases)
SELECT purchase_id AS purchase_id,
	   store_store_id AS store_id,
	   customer_customer_id AS customer_id,
	   JSONExtractString(item, 'product_id') AS product_id,
       JSONExtractString(item, 'quantity') AS quantity,
       lowerUTF8(trim(JSONExtractString(item, 'unit'))) AS unit,
       JSONExtractString(item, 'price_per_unit') AS price_per_unit,
       JSONExtractString(item, 'total_price') AS total_item_price,
	   total_amount,
	   lowerUTF8(trim(payment_method)) AS payment_method, 
	   is_delivery,
	   delivery_address_id AS delivery_address_id,
	   parseDateTimeBestEffort(purchase_datetime) AS purchase_datetime,
	   ai.load_date AS load_date
FROM array_items ai
WHERE toFloat32(JSONExtractString(item, 'quantity')) > 0
  AND toFloat32(JSONExtractString(item, 'price_per_unit')) > 0
  AND JSONExtractString(item, 'unit') IS NOT NULL AND JSONExtractString(item, 'unit') != ''
  AND JSONExtractString(item, 'product_id') IS NOT NULL AND JSONExtractString(item, 'product_id') != ''
  AND toDecimal64(total_amount, 2) > 0
  AND purchase_id IS NOT NULL AND purchase_id != ''
  AND customer_customer_id IS NOT NULL AND customer_customer_id != '';



/*
SELECT * FROM mart_raw.stores;
SELECT * FROM mart_raw.customers;
SELECT * FROM mart_raw.products;
SELECT * FROM mart_raw.purchases;
SELECT * FROM mart.managers;
SELECT * FROM mart.address;
SELECT * FROM mart.customers;
SELECT * FROM mart.stores;
SELECT * FROM mart.products FINAL;
SELECT * FROM mart.purchases FINAL
ORDER BY purchase_id;
*/


























