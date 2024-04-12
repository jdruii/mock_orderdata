from kafka import KafkaProducer
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from faker import Faker
import json
from datetime import datetime, timedelta
import random
import time
from collections import Counter
import iso_mapping
import sku_info
import secrets

fake = Faker('fil_PH')
fake_bgy = Faker('es_CO')

secrets = hidden.secrets()

producer = KafkaProducer(
    bootstrap_servers = secrets['bootstrap_servers'],
    value_serializer = lambda v: json.dumps(v).encode('utf-8'),
    security_protocol = 'SASL_SSL',
    sasl_mechanism = 'PLAIN',
    sasl_plain_username = secrets['sasl_plain_username'],
    sasl_plain_password = secrets['sasl_plain_password'],
)

schema_str = """
{
  '$schema': 'http://json-schema.org/draft-07/schema#',
  'title': 'Order',
  'type': 'object',
  'properties': {
    'order_id': {
      'type': 'integer'
    },
    'order_date': {
      'type': 'string',
      'format': 'date-time'
    },
    'customer_info': {
      'type': 'object',
      'properties': {
        'customer_id': {'type': 'integer'},
        'customer_name': {'type': 'string'},
        'company': {'type': 'string'},
        'email': {'type': 'string', 'format': 'email'},
        'street_address': {'type': 'string'},
        'lgu': {'type': 'string'},
        'provincial_code': {'type': 'string'},
        'contact_number': {'type': 'string'}
      },
      'required': ['customer_id', 'customer_name', 'email', 'company', 'street_address', 'lgu', 'provincial_code', 'contact_number']
    },
    'payment_info': {
      'type': 'object',
      'properties': {
        'payment_status': {'type': 'boolean'},
        'payment_method': {'type': 'string'},
        'payment_date': {'type': 'string', 'format': 'date-time'}
      },
      'required': ['payment_status', 'payment_method', 'payment_date']
    },
    'shipping_info': {
      'type': 'object',
      'properties': {
        'express_shipping': {'type': 'boolean'},
        'shipped_date': {'type': 'string', 'format': 'date-time'},
        'eta': {'type': 'string', 'format': 'date-time'},
        'finished_date': {'type': 'string', 'format': 'date-time'},
        'shipping_id': {'type': 'integer'},
        'order_status': {'type': 'string'}
      },
      'required': ['shipping_id', 'order_status', 'express_shipping', 'shipped_date', 'eta', 'finished_date']
    },
    'item_info': {
      'type': 'array',
      'items': {
        'type': 'object',
        'properties': {
          'sku': {'type': 'integer'},
          'quantity': {'type': 'integer'},
          'unit_price': {'type': 'number'},
          'discount_percent': {'type': 'number'},
          'subtotal': {'type': 'number'}
        },
        'required': ['sku', 'quantity', 'unit_price', 'discount_percent', 'subtotal']
      }
    },
    'order_total': {
      'type': 'number'
    },
    'item_variety': {
      'type': 'integer'
    }
  },
  'required': ['order_id', 'order_date', 'customer_info', 'payment_info', 'shipping_info', 'item_info', 'order_total', 'item_variety']
}
"""

schema_registry_conf = {
    'url': secrets['url'],
    'basic.auth.user.info': secrets['basic.auth.user.inf']
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

order_counter = 0
min_qty = 1
max_qty = 12
excess_duration = 0
total_sales, cost_of_goods, gross_profit = 0, 0, 0
order_volume_adjuster = timedelta(days = 0, hours = 0, minutes = 0, seconds = 0)

repeat_customers = []
top_customers = []
max_top_customers = 25
max_repeat_customers = 75

current_date = datetime.now()
#start_date = datetime.now() - timedelta(days = 90)
start_date = datetime(2021, 3, 14, 9, 0, 0)
start_time = time.time()

future_orders = False
chosen_event = 'sale_event_0'
order_frequencies = Counter()


def generate_random_email_pattern(first_name: str, last_name: str, company_name: str) -> str:
    patterns = [
        f'{first_name.lower()}{last_name.lower()}@{"".join(word[0] for word in company_name.lower().split())}.com',
        f'{first_name.lower()}{last_name.lower()}@{"".join(company_name.lower().split()[:2])}.com',
        f'{first_name.lower()}_{last_name.lower()}@{"".join(word[0] for word in company_name.lower().split())}.com',
        f'{first_name.lower()}_{last_name.lower()}@{"".join(company_name.lower().split()[:2])}.com',
        f'{first_name.lower()[0]}{last_name.lower()}{random.randint(0, 99)}@{"".join(word[0] for word in company_name.lower().split())}.com',
        f'{first_name.lower()[0]}{last_name.lower()}{random.randint(0, 99)}@{"".join(company_name.lower().split()[:2])}.com',
        f'{first_name.lower()[0]}.{last_name.lower()}{random.randint(0, 99)}@{"".join(word[0] for word in company_name.lower().split())}.com',
        f'{first_name.lower()[0]}.{last_name.lower()}{random.randint(0, 99)}@{"".join(company_name.lower().split()[:2])}.com',
    ]

    return random.choice(patterns)


def generate_order_date(start_date: datetime) -> str:
    date_day = start_date.day
    date_month = start_date.month
    global order_volume_adjuster

    slow_day = (start_date + timedelta(hours = random.randint(1, 4),
                                       minutes = random.randint(29, 59),
                                       seconds = random.randint(0, 59)
                                       )).strftime('%Y-%m-%dT%H:%M:%S')

    normal_day = (start_date + timedelta(hours = random.randint(0, 3),
                                         minutes = random.randint(19, 59),
                                         seconds = random.randint(0, 59)
                                         )).strftime('%Y-%m-%dT%H:%M:%S')

    fast_day = (start_date + timedelta(hours = random.randint(0, 2), 
                                       minutes = random.randint(29, 59),
                                       seconds = random.randint(0, 59)
                                       )).strftime('%Y-%m-%dT%H:%M:%S')

    ### Chooses base order_date interval
    if date_day in [14, 15]:  # mid-month sale event
        order_date = (start_date + timedelta(hours = random.randint(0, 1),
                                             minutes = random.randint(39, 59),
                                             seconds = random.randint(0, 59)
                                             )).strftime('%Y-%m-%dT%H:%M:%S')
    elif date_month == 2 and date_day in [27, 28, 29]:  # end of month sale event for february
        order_date = (start_date + timedelta(minutes = random.randint(29, 59),
                                             seconds = random.randint(0, 59)
                                             )).strftime('%Y-%m-%dT%H:%M:%S')
    elif date_month != 2 and date_day in [29, 30, 31]:  # end of month sale event for other months
        order_date = (start_date + timedelta(minutes = random.randint(29, 59),
                                             seconds = random.randint(0, 59)
                                             )).strftime('%Y-%m-%dT%H:%M:%S')
    else: 
        order_date = random.choices([slow_day, normal_day, fast_day], weights = [0.15, 0.7, 0.15])[0]

    ### Adds a modifier to the base order_date interval
    less_order_volume = order_volume_adjuster + timedelta(minutes = random.randint(2, 4), 
                                                          seconds = random.randint(0, 29)
                                                          )  # greater interval between orders = less orders per day on avg

    more_order_volume = order_volume_adjuster - timedelta(minutes = random.randint(2, 4),  
                                                          seconds = random.randint(29, 59)
                                                          )  # lesser interval between orders = more orders per day on avg

    if order_counter % 400 == 0:  # Every 400th order, modifies the order_volume_adjuster to simulate growth over time
        order_volume_adjuster = random.choices([less_order_volume, order_volume_adjuster, more_order_volume], weights = [0.39, 0.14, 0.47])[0]

    order_date = (datetime.fromisoformat(order_date) + order_volume_adjuster).strftime('%Y-%m-%dT%H:%M:%S')

    # if order_date of current order < order_date of prev order, then set minimum time interval instead
    if datetime.fromisoformat(order_date) < start_date + timedelta(minutes = 8):
        order_date = (start_date + timedelta(minutes = random.randint(4, 12),  # avg of 8.75 min of between orders
                                             seconds = random.randint(30, 59)
                                             )).strftime('%Y-%m-%dT%H:%M:%S')
        # order_volume_adjuster = less_order_volume
    return order_date


def get_customer_info() -> dict:

    def generate_barangay() -> str:
        barangay = 'Barangay ' + random.choice(['San ' + fake_bgy.first_name_male(),
                                                'Santo ' + fake_bgy.first_name_male(),
                                                'Santa ' + fake_bgy.first_name_female(),
                                                str(random.randint(1, 999)),
                                                fake.word().capitalize()
                                                ])
        return barangay

    def generate_lgu() -> str:
        lgu_list = {
            'metro_manila': iso_mapping.metro_manila,
            'large_province': iso_mapping.large_province,
            'medium_province': iso_mapping.medium_province,
            'small_province': iso_mapping.small_province,
            'poor_provinces': iso_mapping.poor_provinces
        }

        lgu_category = random.choices(list(lgu_list.keys()), weights = [0.04, 0.27, 0.25, 0.32, 0.12])[0]
        lgu = random.choice(lgu_list[lgu_category])

        return lgu

    def generate_provincial_code(lgu: str) -> str:
        if lgu in iso_mapping.provincial_code:
            return iso_mapping.provincial_code[lgu]
        else:
            return 'Unknown'

    global top_customers, repeat_customers, max_top_customers, max_repeat_customers, order_counter

    # Affects repeat customer rate (lower divisor and/or higher increment leads to higher repeat customer rate)
    if order_counter % 100 == 0:
        max_top_customers += 1
        max_repeat_customers += 16

    # Simulate repeating customers
    # Affects percentage of orders from repeat customers (roughly equal to the odds) by increasing the number of new customers relative to the repeat customers
    if len(top_customers) + len(repeat_customers) >= max_top_customers + max_repeat_customers and random.random() < 0.7:
        if random.random() < 0.7 and top_customers:  # Affects order shares of top_customers
            return random.choice(top_customers).copy()
        elif repeat_customers:
            return random.choice(repeat_customers).copy()
    else:
        # Generate new customer_info
        customer_info = {
            'customer_id': random.randint(10000, 99999),
            'customer_name': fake.name(),
            'company': fake.company(),
            'street_address': random.choice([
                fake.street_address() + ', ' + generate_barangay(),
                fake.building_number() + ' ' + fake.building_name() + ', ' + fake.street_name() + ', ' + generate_barangay()
            ]),
            'lgu': generate_lgu(),
            'provincial_code': '',
            'contact_number': random.choice([fake.area2_landline_number(), fake.mobile_number()]),
            'email': ''
        }
        customer_info['email'] = generate_random_email_pattern(
            customer_info['customer_name'].split()[0],
            customer_info['customer_name'].split()[-1],
            customer_info['company']
        )
        lgu = customer_info['lgu']
        customer_info['provincial_code'] = generate_provincial_code(lgu)

        if random.choice([True, False]):
            customer_info['email'] = customer_info['email'] + '.ph'

        # Save the new customer_info to either top_customers or repeat_customers
        if len(top_customers) < max_top_customers and random.random() < 0.35:
            top_customers.append(customer_info.copy())
        elif len(repeat_customers) < max_repeat_customers and random.random() < 0.5:
            repeat_customers.append(customer_info.copy())
        else:
            pass

        return customer_info


def set_payment_and_shipping_details(order_data):
    order_data['shipping_info']['express_shipping'] = random.random() < 0.4
    is_express = order_data['shipping_info']['express_shipping']
    order_data['payment_info']['payment_status'] = random.random() < 0.65 if future_orders else random.random() < 0.90  # to reduce failed_orders on past orders

    if order_data['payment_info']['payment_status']:
        insta_pay = order_data['order_date']
        non_insta_pay = (datetime.fromisoformat(order_data['order_date']) + timedelta(days = random.randint(0, 2),
                                                                                      hours = random.randint(0, 23),
                                                                                      minutes = random.randint(0, 59),
                                                                                      seconds = random.randint(0, 59),
                                                                                      )).strftime('%Y-%m-%dT%H:%M:%S')
        payment_date = random.choices([insta_pay, non_insta_pay], weights = [0.63, 0.37])[0]

        order_data['payment_info']['payment_date'] = payment_date

        delivery_time = random.randint(3, 5) if is_express else random.randint(5, 10)
        eta = (datetime.fromisoformat(payment_date) + timedelta(days = delivery_time,
                                                                hours = random.randint(0, 23),
                                                                minutes = random.randint(0, 59),
                                                                seconds = random.randint(0, 59)
                                                                )).strftime('%Y-%m-%dT%H:%M:%S')
        order_data['shipping_info']['eta'] = eta


def set_order_status_and_finished_date(order_data):

    order_date = datetime.fromisoformat(order_data['order_date'])
    current_date = datetime.now()
    cutoff_date = current_date - timedelta(days = 7)

    if not order_data['payment_info']['payment_status']:
        order_data['payment_info']['payment_method'] = 'Unpaid'
        if order_date < cutoff_date:  # times out unpaid orders older than 7 days
            order_data['shipping_info']['order_status'] = 'Timed Out'
            order_data['shipping_info']['finished_date'] = (order_date + timedelta(days = 7)).strftime('%Y-%m-%dT%H:%M:%S')
        else:  # all unpaid orders in the last 7 days are still being processed or waiting to be processed
            order_data['shipping_info']['order_status'] = 'Processing'
    else:
        order_data['payment_info']['payment_method'] = random.choices(['Credit Card', 'Bank Transfer', 'Digital Wallet'], [0.24, 0.57, 0.19])[0]
        payment_date = datetime.fromisoformat(order_data['payment_info']['payment_date'])
        if payment_date < current_date - timedelta(hours = 72):  # if the payment was made more than 72 hours ago, order_status progresses
            eta = datetime.fromisoformat(order_data['shipping_info']['eta'])
            if current_date > eta - timedelta(hours = 24):  # if the current date is within 24 hours of the estimated eta, order is past the shipping stage
                order_data['shipping_info']['order_status'] = random.choices(['Delivered', 'Returned', 'Cancelled'],
                                                                             weights = [0.97, 0.01, 0.02])[0]
            else: # otherwise, they are still being shipped
                order_data['shipping_info']['order_status'] = 'Shipped'
        elif payment_date < current_date - timedelta(hours = 18):  # if payment was made over 18 hours but less than 72 hours ago, order is still being processed
            order_data['shipping_info']['order_status'] = 'Processing'
        else:  # every other order is still being processed
            order_data['shipping_info']['order_status'] = 'Processing'

        # sets shipped_date for all orders past the shipping stage
        if order_data['shipping_info']['order_status'] in ['Shipped', 'Delivered', 'Cancelled', 'Returned']:
            is_express = order_data['shipping_info']['express_shipping']
            shipping_time = random.randint(12, 36) if is_express else random.randint(24, 60)
            shipped_date = (payment_date + timedelta(hours = shipping_time,
                                                     minutes = random.randint(0, 59),
                                                     seconds = random.randint(0, 59)
                                                     )).strftime('%Y-%m-%dT%H:%M:%S')
            order_data['shipping_info']['shipped_date'] = shipped_date

        # sets finished_date for all orders that are done
        if order_data['shipping_info']['order_status'] in ['Delivered', 'Cancelled', 'Returned']:
            eta = datetime.fromisoformat(order_data['shipping_info']['eta'])
            finished_date = eta + timedelta(hours = random.randint(-48, 48),
                                            minutes = random.randint(0, 59),
                                            seconds = random.randint(0, 59))
            finished_date = min(finished_date, current_date)
            order_data['shipping_info']['finished_date'] = finished_date.strftime('%Y-%m-%dT%H:%M:%S')


def get_discount_event(order_date_day, order_date_month):
    global max_qty
    if not hasattr(get_discount_event, 'active_sale_events'):
        get_discount_event.active_sale_events = {}

    def clear_active_event_if_needed():
        active_event_info = get_discount_event.active_sale_events.get(order_date_month)
        if active_event_info:
            start_day = active_event_info[1]
            # Clear the event based on the start day and current day
            if (start_day in [14] and order_date_day > 15) or (start_day in [27] and order_date_day > 29) or (start_day in [30] and order_date_day > 31):
                get_discount_event.active_sale_events.pop(order_date_month, None)

    # Check if there's an active sale event
    active_event_info = get_discount_event.active_sale_events.get(order_date_month)
    active_event = active_event_info[0] if active_event_info else None

    if order_date_day in [14] and active_event is None:
        chosen_event = random.choices([('sale_event_1', sku_info.sale_event_1), ('sale_event_2', sku_info.sale_event_2)], weights = [0.7, 0.3])[0]
        max_qty = 12
        get_discount_event.active_sale_events[order_date_month] = (chosen_event, order_date_day)

    elif order_date_month == 2 and order_date_day in [27] and active_event is None:
        chosen_event = random.choices([('sale_event_2', sku_info.sale_event_2), ('sale_event_3', sku_info.sale_event_3)], weights = [0.65, 0.35])[0]
        max_qty = 15
        get_discount_event.active_sale_events[order_date_month] = (chosen_event, order_date_day)

    elif order_date_month != 2 and order_date_day in [30] and active_event is None:
        chosen_event = random.choices([('sale_event_2', sku_info.sale_event_2), ('sale_event_3', sku_info.sale_event_3)], weights = [0.65, 0.35])[0]
        max_qty = 15
        get_discount_event.active_sale_events[order_date_month] = (chosen_event, order_date_day)

    elif active_event:
        chosen_event = active_event
        max_qty = 18 if chosen_event[0] in ['sale_event_2', 'sale_event_3'] else 15

    else:
        chosen_event = ('sale_event_0', sku_info.sale_event_0)
        max_qty = 10

    clear_active_event_if_needed()

    return chosen_event[1], chosen_event[0]


def generate_item_info(discount_event):
    global max_qty

    def add_final_variety(items_count, max_qty):
        low_variety = items_count - random.randint(1, 3) if items_count > 3 else items_count
        normal_variety = items_count
        high_variety = items_count + random.randint(1, 2)
        items_count_variety = random.choices([low_variety, normal_variety, high_variety], weights = [0.29, 0.56, 0.15])[0]

        low_qty = max_qty - random.randint(1, 4) if max_qty > 4 else max_qty
        normal_qty = max_qty
        high_qty = max_qty + random.randint(1, 2)
        max_qty = random.choices([low_qty, normal_qty, high_qty], weights = [0.29, 0.56, 0.15])[0]

        return items_count_variety, max_qty

    item_info = []
    sku_count = set()

    total, total_cost, order_profit = 0, 0, 0

    available_item_ids = list(range(1, 56))
    random.shuffle(available_item_ids)

    items_count = random.randint(1, 5)
    items_count, max_qty = add_final_variety(items_count, max_qty)
    for _ in range(items_count):
        item_id = available_item_ids.pop()

        discount = round(discount_event[item_id], 2)
        unit_price = sku_info.sku_prices[item_id]

        expected_quantity = max(min_qty, max_qty * (1 - (unit_price / 1200)))
        quantity = max(min(round(random.gauss(expected_quantity, 2)), max_qty), min_qty)

        subtotal = round(unit_price * quantity * (1 - discount), 2) if discount != '0' else round((unit_price * quantity), 2)
        total += round(subtotal, 2)

        sku_count.add(item_id)

        item_info.append({
            'sku': item_id,
            'unit_price': unit_price,
            'quantity': quantity,
            'discount_percent': round((discount * 100), 2),
            'subtotal': subtotal,
        })

    return item_info, sku_count, round(total, 2), round(total_cost, 2)

def determine_kafka_topic(order_status: str, payment_status: str) -> str:
    if order_status == 'Processing':
        if payment_status:
            return 'paid_orders'
        else:
            return 'unpaid_orders'
    elif order_status == 'Shipped':
        return 'shipped_orders'
    elif order_status == 'Delivered':
        return 'finished_orders'
    elif order_status in ['Cancelled', 'Returned', 'Timed Out']:
        return 'failed_orders'
    else:
        return 'unknown_orders'

def main():
    global order_counter, start_date, future_orders, total_sales
    while True:
        order_counter += 1
        sku_count = set()
        order_date = generate_order_date(start_date)

        if datetime.fromisoformat(order_date) > datetime.now():
            future_orders = True
            # order_date = current_date.strftime('%Y-%m-%dT%H:%M:%S')
            time.sleep(2.5)
            elapsed_time = time.time() - start_time
            if elapsed_time >= excess_duration:
                break

        order_data = {
            'order_id' : '',
            'order_date' : '',
            'customer_info' : {
                'customer_id' : '',
                'customer_name' : '',
                'company' : '',
                'email' : '',
                'street_address' : '',
                'lgu' : '',
                'provincial_code': '',
                'contact_number' : ''
            },
            'payment_info' : {
                'payment_status' : '',
                'payment_method' : '',
                'payment_date' : '',
            },
            'shipping_info' : {
                'shipping_id': '',
                'express_shipping' : '',
                'order_status' : '',
                'shipped_date' : '',
                'eta': '',
                'finished_date' : '',
            },
            'item_info' : [],
            'item_variety': '',
            'order_total' : '',
        }

        order_data['order_id'] = order_counter
        order_data['order_date'] = order_date

        ### CUSTOMER INFO

        order_data['customer_info'] = get_customer_info()

        customer_id = order_data['customer_info']['customer_id']
        order_frequencies[customer_id] += 1

        ### PAYMENT AND SHIPPING INFO

        order_data['shipping_info']['shipping_id'] = random.randint(1000000, 9999999)

        set_payment_and_shipping_details(order_data)

        set_order_status_and_finished_date(order_data)

        ### ITEM INFO

        order_date_day = datetime.fromisoformat(order_data['order_date']).day
        order_date_month = datetime.fromisoformat(order_data['order_date']).month
        discount_event, discount_event_name = get_discount_event(order_date_day, order_date_month)

        item_info, sku_count, total, total_cost = generate_item_info(discount_event)

        order_data['item_info'] = item_info
        order_data['item_variety'] = len(sku_count)

        is_express = order_data['shipping_info']['express_shipping']
        order_data['order_total'] = round(total, 2)

        if order_data['shipping_info']['order_status'] == 'Delivered':
            total_sales += order_data['order_total']

        ### END OF GENERATOR

        start_date = datetime.fromisoformat(order_data['order_date'])

        kafka_topic = determine_kafka_topic(order_data['shipping_info']['order_status'], order_data['payment_info']['payment_status'])
        payment_status = order_data['payment_info']['payment_status']
        if kafka_topic == 'finished_orders':
            serializing_producer.produce(topic = kafka_topic, key = str(order_data['order_id']), value = order_data)
            serializing_producer.flush()
            print(f'Incoming Order: {order_counter:06} | Payment Status: {payment_status} | Sending to: {kafka_topic}')
        else:
            producer.send(kafka_topic, value = order_data)
            print(f'Incoming Order: {order_counter:06} | Payment Status: {payment_status} | Sending to: {kafka_topic}')

    producer.close()

if __name__ == '__main__':
    main()
