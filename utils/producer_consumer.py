from kafka import KafkaProducer


def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        #print("==> Message published successfully.")
    except Exception as ex:
        print("==> Exception in publishing message")
        print(str(ex))


def connect_kafka_producer():
    _producer = None

    try:
        _producer = KafkaProducer(bootstrap_servers='localhost:9093', security_protocol='SSL', ssl_check_hostname=False,
                                  ssl_cafile='kafka_ssl/CARoot.pem',
                                  ssl_certfile='kafka_ssl/certificate.pem',
                                  ssl_keyfile='kafka_ssl/key.pem')
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer