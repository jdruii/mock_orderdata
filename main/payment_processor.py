from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timedelta
import json
import random
import time
import hidden

secrets = hidden.secrets()

consumer = KafkaConsumer(
    'unpaid_orders',
    bootstrap_servers = secrets['bootstrap_servers'],
    auto_offset_reset = 'earliest',
    value_deserializer = lambda m: json.loads(m.decode('utf-8')),
    security_protocol='SASL_SSL',
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

print('Waiting for new orders to arrive...')

def process_order(order_data):
    wait_time = round(random.uniform(0.25, 1), 2)
    time.sleep(wait_time)

    order_id = order_data['order_id']
    order_data['payment_info']['payment_status'] = random.random() < 0.77
    order_date = datetime.fromisoformat(order_data['order_date'])
    if order_data['payment_info']['payment_status']:
        order_data['payment_info']['payment_method'] = random.choices(['Credit Card', 'Bank Transfer', 'Digital Wallet'], [0.24, 0.57, 0.19])[0]
        order_data['payment_info']['payment_date'] = (order_date + timedelta(days = random.randint(0, 2),
                                                                             hours = random.randint(0, 23),
                                                                             minutes = random.randint(0, 59),
                                                                             seconds = random.randint(0, 59),
                                                                             )).strftime('%Y-%m-%dT%H:%M:%S')

        is_express = order_data['shipping_info']['express_shipping']
        delivery_time = random.randint(3, 5) if is_express else random.randint(5, 10)
        order_data['shipping_info']['eta'] = (datetime.now() + timedelta(days = delivery_time,
                                                                         hours = random.randint(0, 23),
                                                                         minutes = random.randint(0, 59),
                                                                         seconds = random.randint(0, 59)
                                                                         )).strftime('%Y-%m-%dT%H:%M:%S')

        payment_method = order_data['payment_info']['payment_method']
        payment_date = order_data['payment_info']['payment_date']
        print(f'Payment for order {order_id} has been verified | Payment Method: {payment_method} | Payment Date: {payment_date}')
    else:
        order_data['shipping_info']['order_status'] = 'Timed Out'
        order_data['shipping_info']['finished_date'] = (order_date + timedelta(days = 7)).strftime('%Y-%m-%dT%H:%M:%S')
        timed_out_date = order_data['shipping_info']['finished_date']
        print(f'No payment for order {order_id} has been verified. The order has timed out. | Timed Out Date: {timed_out_date}')

    payment_status = order_data['payment_info']['payment_status']
    kafka_topic = 'paid_orders' if payment_status else 'failed_orders'
    producer.send(kafka_topic, value = order_data)

def main():
    for message in consumer:
        order_data = message.value
        process_order(order_data)

if __name__ == '__main__':
    main()

