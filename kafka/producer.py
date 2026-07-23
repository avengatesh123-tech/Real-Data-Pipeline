import json
import uuid
import random
import time

from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "enable.idempotence": True,
    "acks": "all",
    "retries": 5
})

users = [
    "lara", "arun", "vengatesh",
    "priya", "karthik", "divya", "suresh"
]

items = [
    "milk",
    "bread",
    "eggs",
    "rice",
    "paneer",
    "butter",
    "curd"
]

cities = [
    "Chennai",
    "Bangalore",
    "Hyderabad",
    "Mumbai"
]

payments = [
    "UPI",
    "Credit Card",
    "Debit Card",
    "COD"
]


def delivery_report(err, msg):
    if err:
        print(err)
    else:
        print(
            f"Delivered -> "
            f"Partition={msg.partition()} "
            f"Offset={msg.offset()}"
        )


while True:

    order = {

        "order_id": str(uuid.uuid4()),

        "user": random.choice(users),

        "item": random.choice(items),

        "quantity": random.randint(1,5),

        "price": round(random.uniform(50,500),2),

        "city": random.choice(cities),

        "payment": random.choice(payments),

        "timestamp": time.time()

    }

    producer.produce(

        "orders",

        key=order["order_id"],

        value=json.dumps(order),

        callback=delivery_report

    )

    producer.poll(0)

    time.sleep(1)
