import os
import json
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker
from pathlib import Path



random.seed(42)
Faker.seed(42)

fake = Faker("ru_RU")

# Создание директорий
path_data = Path(__file__).resolve().parent.parent / 'data'
path_data.mkdir(parents=True, exist_ok=True)
path_stores = path_data / 'stores'
path_stores.mkdir(parents=True, exist_ok=True)
path_products = path_data / 'products'
path_products.mkdir(parents=True, exist_ok=True)
path_customers = path_data / 'customers'
path_customers.mkdir(parents=True, exist_ok=True)
path_purchases = path_data / 'purchases'
path_purchases.mkdir(parents=True, exist_ok=True)


# Категории
categories = [
    "🥖 Зерновые и хлебобулочные изделия",
    "🥩 Мясо, рыба, яйца и бобовые",
    "🥛 Молочные продукты",
    "🍏 Фрукты и ягоды",
    "🥦 Овощи и зелень"
]

store_networks = [("Большая Пикча", 30), ("Маленькая Пикча", 15)]
stores = []

# === 1. Генерация магазинов ===
for network, count in store_networks:
    for i in range(count):
        store_id = f"store-{len(stores)+1:03}"
        city = fake.city()
        store = {
            "store_id": store_id,
            "store_name": f"{network} — Магазин на {fake.street_name()}",
            "store_network": network,
            "store_type_description": f"{'Супермаркет более 200 кв.м.' if network == 'Большая Пикча' else 'Магазин у дома менее 100 кв.м.'} Входит в сеть из {count} магазинов.",
            "type": "offline",
            "categories": categories,
            "manager": {
                "name": fake.name(),
                "phone": fake.phone_number(),
                "email": fake.email()
            },
            "location": {
                "country": "Россия",
                "city": city,
                "street": fake.street_name(),
                "house": str(fake.building_number()),
                "postal_code": fake.postcode(),
                "coordinates": {
                    "latitude": float(fake.latitude()),
                    "longitude": float(fake.longitude())

                }
            },
            "opening_hours": {
                "mon_fri": "09:00-21:00",
                "sat": "10:00-20:00",
                "sun": "10:00-18:00"
            },
            "accepts_online_orders": True,
            "delivery_available": True,
            "warehouse_connected": random.choice([True, False]),
            "last_inventory_date": datetime.now().strftime("%Y-%m-%d")
        }
        stores.append(store)
        with open(path_stores / f"{store_id}.json", "w", encoding="utf-8") as f:
            json.dump(store, f, ensure_ascii=False, indent=2)

# === 2. Генерация товаров ===
products = []
for i in range(20):
    product = {
        "id": f"prd-{1000+i}",
        "name": fake.word().capitalize(),
        "group": random.choice(categories),
        "description": fake.sentence(),
        "kbju": {
            "calories": round(random.uniform(50, 300), 1),
            "protein": round(random.uniform(0.5, 20), 1),
            "fat": round(random.uniform(0.1, 15), 1),
            "carbohydrates": round(random.uniform(0.5, 50), 1)
        },
        "price": round(random.uniform(30, 300), 2),
        "unit": "шт",
        "origin_country": "Россия",
        "expiry_days": random.randint(5, 30),
        "is_organic": random.choice([True, False]),
        "barcode": fake.ean(length=13),
        "manufacturer": {
            "name": fake.company(),
            "country": "Россия",
            "website": f"https://{fake.domain_name()}",
            "inn": fake.bothify(text='##########')
        }
    }
    products.append(product)
    with open(path_products / f"{product['id']}.json", "w", encoding="utf-8") as f:
        json.dump(product, f, ensure_ascii=False, indent=2)

# === 3. Генерация покупателей ===
customers = []
for store in stores:
    customer_id = f"cus-{1000 + len(customers)}"
    customer = {
        "customer_id": customer_id,
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=70).isoformat(),
        "gender": random.choice(["male", "female"]),
        "registration_date": datetime.now().isoformat(),
        "is_loyalty_member": True,
        "loyalty_card_number": f"LOYAL-{uuid.uuid4().hex[:10].upper()}",
        "purchase_location": store["location"],
        "delivery_address": {
            "country": "Россия",
            "city": store["location"]["city"],
            "street": fake.street_name(),
            "house": str(fake.building_number()),
            "apartment": str(random.randint(1, 100)),
            "postal_code": fake.postcode()
        },
        "preferences": {
            "preferred_language": "ru",
            "preferred_payment_method": random.choice(["card", "cash"]),
            "receive_promotions": random.choice([True, False])
        }
    }
    customers.append(customer)
    with open(path_customers / f"{customer_id}.json", "w", encoding="utf-8") as f:
        json.dump(customer, f, ensure_ascii=False, indent=2)

# === 4. Генерация покупок ===
for i in range(200):
    customer = random.choice(customers)
    store = random.choice(stores)
    items = random.sample(products, k=random.randint(1, 3))
    purchase_items = []
    total = 0
    for item in items:
        qty = random.randint(1, 5)
        total_price = round(item["price"] * qty, 2)
        total += total_price
        purchase_items.append({
            "product_id": item["id"],
            "name": item["name"],
            "category": item["group"],
            "quantity": qty,
            "unit": item["unit"],
            "price_per_unit": item["price"],
            "total_price": total_price,
            "kbju": item["kbju"],
            "manufacturer": item["manufacturer"]
        })
    purchase = {
        "purchase_id": f"ord-{i+1:05}",
        "customer": {
            "customer_id": customer["customer_id"],
            "first_name": customer["first_name"],
            "last_name": customer["last_name"]
        },
        "store": {
            "store_id": store["store_id"],
            "store_name": store["store_name"],
            "store_network": store["store_network"],
            "location": store["location"]
        },
        "items": purchase_items,
        "total_amount": round(total, 2),
        "payment_method": random.choice(["card", "cash"]),
        "is_delivery": random.choice([True, False]),
        "delivery_address": customer["delivery_address"],
        "purchase_datetime": (datetime.now() - timedelta(days=random.randint(0, 90))).isoformat()
    }
    with open(path_purchases / f"{purchase['purchase_id']}.json", "w", encoding="utf-8") as f:
        json.dump(purchase, f, ensure_ascii=False, indent=2)

print("✅ Данные успешно сгенерированы")