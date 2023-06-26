import random
import sys
import time
from datetime import datetime
from json import dumps

from cassandra.cluster import Cluster
from kafka import KafkaProducer

CASSANDRA_HOST = 'localhost'
CASSANDRA_KEYSPACE = 'sales'
CASSANDRA_TABLE = 'orders'
KAFKA_BOOTSTRAP_SERVER = 'localhost:29092'

def get_last_order_id():
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(CASSANDRA_KEYSPACE)
    query = f"SELECT MAX(order_id) AS last_order_id FROM {CASSANDRA_TABLE}"
    result = session.execute(query)
    last_order_id = result.one().last_order_id
    return last_order_id if last_order_id else 0
        
def produce_message(order_id, max_product_id, max_platform_id):
    message = {}
    message["order_id"] = order_id
    message["created_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    message['platform_id'] = random.randint(1, max_platform_id)
    message['product_id'] = random.randint(1, max_product_id)
    message['quantity'] = random.randint(1, 10)
    message['customer_id'] = random.randint(1, 1000)
    message['payment_method'] = random.choice(['credit card', 'debit card', 'bank transfer', 'paypal'])
    return message

def main():
    KAFKA_TOPIC = sys.argv[1]
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                            value_serializer=lambda x: dumps(x).encode('utf-8'))
    last_order_id = get_last_order_id()
    print("Kafka producer application started.")
    
    try:
        while True:
            order_id = last_order_id + 1
            max_product_id = 20
            max_platform_id = 4
            message = produce_message(order_id, max_product_id, max_platform_id)
            print(f"Produced message: {message}")
            producer.send(KAFKA_TOPIC, message)
            time.sleep(0.2)
            last_order_id = order_id    
    except KeyboardInterrupt:
        producer.flush()
        producer.close()
        print("Kafka producer application completed.")

if __name__ == "__main__":
    main()
    