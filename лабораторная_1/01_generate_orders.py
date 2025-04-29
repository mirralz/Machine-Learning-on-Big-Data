from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand, expr
import random
import csv
import os

def generate_data(output_path, num_orders=330000):
    spark = SparkSession.builder.appName("OrderDataGenerator").getOrCreate()

    cities = {
        "Moscow": 1000, "Saint Petersburg": 1000, "Novosibirsk": 250,
        "Yekaterinburg": 250, "Kazan": 250, "Nizhny Novgorod": 100,
        "Chelyabinsk": 100, "Samara": 100, "Rostov-on-Don": 100, "Ufa": 100
    }

    menu = [
        ('Pizza', 800), ('Burger', 500), ('Sushi', 700), ('Salad', 400), ('Pasta', 550),
        ('Coffee', 300), ('Cake', 900), ('Steak', 1300), ('Soup', 350), ('Sandwich', 400),
        ('Fries', 600), ('Fish steak', 1000), ("Smoothie", 400), ("Borsh", 600),
        ("Hot Dog", 230), ("Tacos", 380), ("Waffles", 290), ("Omelette", 270)
    ]

    adjectives = ['Funny', 'Little', 'Old', 'Original', 'Beautiful', 'Amazing', 'Cool', 'Good', 'Bad', 'Hungry', 'Angry', 'Wild', 'Big', 'Small', 'Pretty', 'Tiny', 'Honey', 'Sweet', 'Wet', 'Windy', 'Tasty', 'Smart', 'Clever', 'Stupid', 'Delicious', 'Rich', 'Poor', 'Sad', 'Interesting', 'Young', 'Free', 'Warm', 'Hot', 'Cold', 'Noisy']
    nouns = ['Cat', 'Dog', 'Duck', 'Worm', 'Table', 'Ocean', 'Bottle', 'Cup', 'Eye', 'Chocolate', 'Cake', 'Cupcake', 'Coffee', 'Idea', 'Harry', 'Tom', 'Jerry', 'Jack', 'Charlie', 'Flower', 'Smile', 'Emoji', 'Night', 'Day', 'Juice', 'Lemon', 'Orange', 'Strawberry', 'Melon', 'Watermelon', 'Kitty', 'Bird', 'Joke', 'Bar', 'Song', 'Cloud', 'Light']

    facilities = []
    for city, num_facilities in cities.items():
        for _ in range(num_facilities):
            facility_name = f"{random.choice(adjectives)} {random.choice(nouns)}"
            facilities.append((city, facility_name,
                               round(random.uniform(50.0, 60.0), 6),
                               round(random.uniform(30.0, 40.0), 6)))

    orders = []
    for order_id in range(1, num_orders + 1):
        city, name, lat, lng = random.choice(facilities)
        num_items = random.randint(1, 5)
        timestamp = f"2024-{random.randint(1, 12):02}-{random.randint(1, 28):02} " \
                    f"{random.randint(0, 23):02}:{random.randint(0, 59):02}:{random.randint(0, 59):02}"

        for _ in range(num_items):
            item, price = random.choice(menu)

            if random.random() < 0.1:  # 10% испорченные данные
                if random.random() < 0.5:
                    lat, lng = -999.999, -999.999
                else:
                    item = None

            orders.append((order_id, city, "Russia", name, lat, lng, item, price, timestamp))

    os.makedirs(output_path, exist_ok=True)
    output_file = os.path.join(output_path, "orders.csv")

    with open(output_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Order_id", "city", "country", "facility_name", "lat", "lng", "menu_item", "price", "datetime"])
        writer.writerows(orders)

    print(f"CSV файл создан: {output_file}")

    spark.stop()

# Запуск генерации данных
generate_data("./spark_orders")
