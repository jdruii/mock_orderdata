from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timedelta
import json
import random
import time
import hidden

secrets = hidden.secrets()

consumer = KafkaConsumer(
    'paid_orders',
    bootstrap_servers = secrets['bootstrap_servers'],
    auto_offset_reset = 'earliest',
    value_deserializer = lambda m: json.loads(m.decode('utf-8')),
    security_protocol = 'SASL_SSL',
    sasl_mechanism = 'PLAIN',
    sasl_plain_username = secrets['sasl_plain_username'],
    sasl_plain_password = secrets['sasl_plain_password'],
)

producer = KafkaProducer(
    bootstrap_servers = secrets['bootstrap_servers'],
    value_serializer = lambda v: json.dumps(v).encode('utf-8'),
    security_protocol = 'SASL_SSL',
    sasl_mechanism = 'PLAIN',
    sasl_plain_username = secrets['sasl_plain_username'],
    sasl_plain_password = secrets['sasl_plain_password'],
)

print('Waiting for paid orders to be shipped out...')

def process_order(order_data):
    wait_time = round(random.uniform(0.25, 1), 2)
    time.sleep(wait_time)

    payment_date = datetime.fromisoformat(order_data['payment_info']['payment_date'])
    order_id = order_data['order_id']

    if random.random() < 0.95:
        order_data['shipping_info']['order_status'] = 'Shipped'
        is_express = order_data["shipping_info"]["express_shipping"]
        shipping_time = random.randint(12, 36) if is_express else random.randint(24, 60)
        shipped_date = payment_date + timedelta(hours = shipping_time,
                                                minutes = random.randint(0, 59),
                                                seconds = random.randint(0, 59))
        order_data['shipping_info']['shipped_date'] = shipped_date.strftime('%Y-%m-%dT%H:%M:%S')
        print(f'Order {order_id} has been shipped. | Express Shipping: {is_express} | Shipped Date: {shipped_date}')
    else:
        order_data['shipping_info']['order_status'] = 'Cancelled'
        order_data['shipping_info']['finished_date'] = (payment_date + timedelta(hours = random.randint(6, 54),
                                                                                 minutes=random.randint(0, 59),
                                                                                 seconds=random.randint(0, 59)
                                                                                 )).strftime('%Y-%m-%dT%H:%M:%S')
        cancellation_date = order_data['shipping_info']['finished_date']
        print(f'Order {order_id} has been cancelled during shipping. | Cancellation Date: {cancellation_date}')

    def determine_kafka_topic(order_status):
        if order_status == 'Shipped':
            return 'shipped_orders'
        else:
            return 'failed_orders'

    order_status = order_data['shipping_info']['order_status']
    kafka_topic = determine_kafka_topic(order_status)
    producer.send(kafka_topic, value = order_data)

def main():
    for message in consumer:
        order_data = message.value
        process_order(order_data)

if __name__ == '__main__':
    main()
