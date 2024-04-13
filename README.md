# Mock Order Data Pipeline

### Table of Contents
- Pipeline Overview
- Data Generator
- Payment, Shipping, and Delivery Processors
- ksqlDB Stream Processor
- Storing Messages to PostgreSQL via Supabase
- Creating Dashboard using Apache Superset via Preset

### Pipeline Overview
![](/images/datapipeline.png)

This data pipeline simulates an e-commerce application composed of multiple microservices loosely coupled by Apache Kafka. All data starts with the Shopping Service — either a mobile app, a web service, or both — which gathers sales data of each orders. For this project, this microservice is simulated by our Data Generator (producer.py). Generated orders are then sorted by their order status and is sent as a message to their respective kafka topics on Confluent Cloud. 

These topics are then read by the appropriate microservice (all simulated using python). The Payment Processor (payment_processor.py) consumes the messages in the `unpaid_orders` topic, fill the appropriate fields, and produce messages to the topic `paid_orders`. These messages in the `paid_orders` are then consumed by the Shipping Processor (shipping_processor.py) which again enriches the data with the shipping information, and produce them to the topic `shipped_orders`. And finally, the messages in the `shipped_orders` are consumed by the Delivery Processor which produces a corresponding message to the `finished_orders` topic. Each of the microservice also has a small chance to fail an order, either due to the orders being timed out during payment, being cancelled by the customer during shipping, or being returned by the customer upon delivery. These failed orders are instead sent to the `failed_orders` topic. 

Data from successfully delivered orders in the `finished_orders` topic are then consumed by ksqlDB which transforms to a form suited for a Kafka Connector which will store it in a PostgreSQL database hosted on Supabase. Order information, customer information, and item information are separated and put into their own tables. The data in the database are then read by Apache Superset (via Preset.io) for exploratory data analysis, visualisation, and dashboarding. 
