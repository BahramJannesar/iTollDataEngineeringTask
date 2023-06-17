import random
from faker import Faker
import pandas as pd
from typing import List
from datetime import datetime, timedelta


def generate_fake_users(num_users : int) -> pd.DataFrame:
    fake = Faker()
    dataset = []
    for i in range(num_users):
        user = {
            'id': 999 + i,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'cellphone': "+98912{}".format(i),
            'username': fake.user_name(),
            'email': fake.email(),
            'created_at': fake.date_time_this_decade(),
            'updated_at': fake.date_time_this_decade(),
            'gender': random.choice(['Male', 'Female'])
        }
        dataset.append(user)
    df = pd.DataFrame(dataset)
    return df


def generate_fake_cars(num_cars : int, user_ids : List) -> pd.DataFrame:
    fake = Faker()
    dataset = []
    for i in range(num_cars):
        car = {
            'id': 9999 + i,
            'user_id': random.choice(user_ids),
            'model': fake.year(),
            'fuel':  random.choice(["Gasoline", "CNG", "Diesel"]),
            'manufacturer':fake.company(),
            'color': fake.color_name(),
            'created_at': fake.date_time_this_decade(),
            'updated_at': fake.date_time_this_decade()
        }
        dataset.append(car)
    df = pd.DataFrame(dataset)
    return df


def generate_fake_orders(num_orders : int, list_users_cars : List) -> pd.DataFrame:
    dataset = []
    for i in range(num_orders):
        user_id, car_id = random.choice(list_users_cars)
        current_time = datetime.now()
        order = {
            'id': 99999 + i,
            'car_id': car_id,
            'user_id': user_id,
            'order_status': random.choice([123, 125, 128]),
            'payment_status': random.choice(['Pending', 'Paid']),
            'total_value':  random.uniform(100000, 500000),
            'order_discount': random.uniform(0, 0.4),
            'created_at': current_time,
            'updated_at':current_time + timedelta(seconds=random.choice([5,6,7,8,9,10]))
        }
        dataset.append(order)
    df = pd.DataFrame(dataset)
    return df
