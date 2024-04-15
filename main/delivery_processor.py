from kafka import KafkaConsumer
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from datetime import datetime, timedelta
import json
import random
import time
import hidden

secrets = hidden.secrets()

consumer = KafkaConsumer(
    'shipped_orders',
    bootstrap_servers = secrets[' bootstrap_servers'],
    auto_offset_reset = 'earliest',
    value_deserializer = lambda m: json.loads(m.decode('utf-8')),
    security_protocol = 'SASL_SSL',
    sasl_mechanism = 'PLAIN',
    sasl_plain_username = secrets['sasl_plain_username'],
    sasl_plain_password = secrets['sasl_plain_password'],
)

schema_str = """
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Order",
  "type": "object",
  "properties": {
    "order_id": {
      "type": "integer"
    },
    "order_date": {
      "type": "string",
      "format": "date-time"
    },
    "customer_info": {
      "type": "object",
      "properties": {
        "customer_id": {"type": "integer"},
        "customer_name": {"type": "string"},
        "company": {"type": "string"},
        "email": {"type": "string", "format": "email"},
        "street_address": {"type": "string"},
        "lgu": {"type": "string"},
        "provincial_code": {"type": "string"},
        "contact_number": {"type": "string"}
      },
      "required": ["customer_id", "customer_name", "email", "company", "street_address", "lgu", "provincial_code", "contact_number"]
    },
    "payment_info": {
      "type": "object",
      "properties": {
        "payment_status": {"type": "boolean"},
        "payment_method": {"type": "string"},
        "payment_date": {"type": "string", "format": "date-time"}
      },
      "required": ["payment_status", "payment_method", "payment_date"]
    },
    "shipping_info": {
      "type": "object",
      "properties": {
        "express_shipping": {"type": "boolean"},
        "shipped_date": {"type": "string", "format": "date-time"},
        "eta": {"type": "string", "format": "date-time"},
        "finished_date": {"type": "string", "format": "date-time"},
        "shipping_id": {"type": "integer"},
        "order_status": {"type": "string"}
      },
      "required": ["shipping_id", "order_status", "express_shipping", "shipped_date", "eta", "finished_date"]
    },
    "item_info": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "sku": {"type": "integer"},
          "quantity": {"type": "integer"},
          "unit_price": {"type": "number"},
          "discount_percent": {"type": "number"},
          "subtotal": {"type": "number"}
        },
        "required": ["sku", "quantity", "unit_price", "discount_percent", "subtotal"]
      }
    },
    "order_total": {
      "type": "number"
    },
    "item_variety": {
      "type": "integer"
    }
  },
  "required": ["order_id", "order_date", "customer_info", "payment_info", "shipping_info", "item_info", "order_total", "item_variety"]
}
"""

schema_registry_conf = {
    'url': secrets['url'],
    'basic.auth.user.info': secrets['basic.auth.user.info']
}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)

json_serializer = JSONSerializer(schema_str, schema_registry_client)

producer_conf = {
    'bootstrap.servers': secrets['bootstrap.servers'],
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': secrets['sasl.username'],
    'sasl.password': secrets['sasl.password'],
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': json_serializer
}

serializing_producer = SerializingProducer(producer_conf)

print('Waiting for shipped orders to be delivered...')

def process_order(order_data):
    is_express = order_data['shipping_info']['express_shipping']
    wait_time = round(random.uniform(0.25, 1), 2) if is_express else round(random.uniform(1.25, 2), 2)
    time.sleep(wait_time)

    order_data['shipping_info']['order_status'] = random.choices(['Delivered', 'Cancelled', 'Returned'],
                                                                 weights = [0.95, 0.035, 0.015])[0]

    order_id = order_data['order_id']

    eta = datetime.fromisoformat(order_data['shipping_info']['eta'])
    finished_date = eta + timedelta(hours = random.randint(-48, 36),
                                    minutes = random.randint(0, 59),
                                    seconds = random.randint(0, 59))
    order_data['shipping_info']['finished_date'] = finished_date.strftime('%Y-%m-%dT%H:%M:%S')

    if order_data['shipping_info']['order_status'] == 'Delivered':
        print(f'Order {order_id} has been delivered. | Delivery Date: {finished_date}')
    elif order_data['shipping_info']['order_status'] == 'Cancelled':
        print(f'Order {order_id} has been cancelled during delivery. | Cancellation Date: {finished_date}')
    elif order_data['shipping_info']['order_status'] == 'Returned':
        print(f'Order {order_id} has been returned after delivery. | Return Date: {finished_date}')
    else:
        print(f'Order status of order {order_id} is unknown.')

    def determine_kafka_topic(order_status):
        if order_status == 'Delivered':
            return 'finished_orders'
        else:
            return 'failed_orders'

    order_status = order_data['shipping_info']['order_status']
    kafka_topic = determine_kafka_topic(order_status)
    serializing_producer.produce(topic = kafka_topic, key = str(order_data['order_id']), value = order_data)
    serializing_producer.flush()

def main():
    for message in consumer:
        order_data = message.value
        process_order(order_data)

if __name__ == '__main__':
    main()
