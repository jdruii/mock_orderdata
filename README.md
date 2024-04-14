# Table of Contents
1. [Pipeline Overview](#pipeline-overview)
2. [Data Generator](#data-generator)
3. [Kafka Producer](#kafka-producer)
4. [Simulating Microservices](#simulating-microservices)
5. [ksqlDB Stream Processor](#ksqldb-stream-processor)
6. [Storing to PostgreSQL](#storing-to-postgresql)
7. [Visualization using Apache Superset](#visualization-using-apache-superset)

# Pipeline Overview

![](/images/data_pipeline.png)

This data pipeline simulates an e-commerce application composed of multiple microservices loosely coupled by Apache Kafka. All data starts with the Shopping Service — either a mobile app, a web service, or both — which gathers sales data of each orders. For this project, this microservice is simulated by our Data Generator (`producer.py`). Generated orders are then sorted by their order status and is sent as a message to their respective kafka topics on Confluent Cloud. 

These topics are then read by the appropriate microservice (all simulated using python). The Payment Processor (`payment_processor.py`) consumes the messages in the `unpaid_orders` topic, fill the appropriate fields, and produce messages to the topic `paid_orders`. These messages in the `paid_orders` are then consumed by the Shipping Processor (shipping_processor.py) which again enriches the data with the shipping information, and produce them to the topic `shipped_orders`. And finally, the messages in the `shipped_orders` are consumed by the Delivery Processor which produces a corresponding message to the `finished_orders` topic. Each of the microservice also has a small chance to fail an order, either due to the orders being timed out during payment, being cancelled by the customer during shipping, or being returned by the customer upon delivery. These failed orders are instead sent to the `failed_orders` topic. 

Data from successfully delivered orders in the `finished_orders` topic are then consumed by ksqlDB which transforms to a form suited for a Kafka Connector which will store it in a PostgreSQL database hosted on Supabase. Order information, customer information, and item information are separated and put into their own tables. The data in the database are then read by Apache Superset (via Preset.io) for exploratory data analysis, visualisation, and dashboarding. 

# Data Generator

## Essential Libraries
- [Faker](https://faker.readthedocs.io/) - used for generating a majority of the fields
- datetime - used for handling dates as well as timedeltas
- random - used for simulating randomness and variety

## Key Components
- sku_info = a separate python file that stores costs and prices of each item
- iso_mapping = a separate python file that stores ISO codes of each provinces as well as their categories

## Order Generation Process
1. Initializing order_data.
   
      This is the standard form of all order_data in the pipeline. For a more detailed schema, see `schema.py`.
   ```
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
   ```
3. Generate order_date.
   
   The function `generate_order_date` takes in the global variable start_date and adds a random timedelta to assign the order_date. The very first order_date uses the pre-determined start_date. Near the end of the function, the start_date is set to the order_date used. This ensures that the order_date of the next order is after the order_date of the previous order (barring any negative timedeltas).
   
   The function is also used to simulate the volume of orders throughout a given day. For example, days that fall in the last 3 days of the month have a shorter timedelta, which means that there would be more orders in those days. There's also another timedelta from the order_volume_adjuster that gets added at the end of the calculation. This timedelta can either decrease, remain the same, or increase every 400 orders (though the odds are skewed towards increase). This adds another layer of randomness through a short period, but generally points towards an increase in sales over time.  

5. Generate customer_info.
   
   The function `get_customer_info` mainly uses the Faker library for the data that are strings such as names, company, and street_address. While the email is generated by using the customer's first name, last name, and company. The lgu is randomly generated using the categorized lgu in iso_mapping.py which gives higher odds to cities within Metro Manila and provinces that contain a highly urbanized city (HUC). This function is also used to simulate repeating customers as well as the number of orders these repeating customers make on average by saving some of the generated customer_info. These saved customer_infos can then be re-used in future orders. On average, this produces a customer repeat rate of about 22%

7. Setting Payment, Shipping, and Delivery Details.
   
   The function `set_payment_and_shipping_details`  sets the payment, shipping, and delivery information of all past orders. For orders that are generated once the `future_orders` is flipped to False, the information for these fields are handled by the Payment Processor, Shipping Processor, and Delivery Processor. The `set_order_status_and_finished_date` then sets the order_status and finished_date of each past orders based on the payment, shipping, and delivery inforation.

9. Generate item_info.
    
   The function `generate_item_info` sets the items bought in each order, the variety of items, as well as the quantity of each items. It also applies a discount if there is an active discount as determined by the function `get_discount_event` before calculating the price of the item, as well as the total of the order.

# Kafka Producer

The `SerializingProducer` package from the confluent_kafka library is used to initialize the producers along with the `SchemaRegistryClient` as the topics needed to have a schema to be connected to ksqlDB down the line. Since there are five topics (unpaid_orders, paid_orders, shipped_orders, finished_orders, and failed_orders) in the data pipeline each has their own schema, `producer.py` needs to intialize five different serializing producers at the start of the program as well. Each producer sends a message for all the appropriate orders based on their order_status. 

# Simulating Microservices

The payment, shipping, and delivery processors are functionally the same. They simulate their respective microservices by "processing" each messages from the topic they consume from before producing the transformed message to the next stage in the pipeline where it will be processed by the next one until the order has finally been delivered. The `process_order` function in each of these processors uses the same logic as the ones in the `producer.py`. Each of these processors also simulates a chance that the orders fail in that stage and will produce the message for those orders to topic `failed_orders` instead.

# ksqlDB Stream Processor

Once the 
