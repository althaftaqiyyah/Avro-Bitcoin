from confluent_kafka.avro import AvroConsumer

def read_message():
    consumer_config = {"bootstrap.servers": "localhost:9092",
                       "schema.registry.url": "http://localhost:8081",
                       "group.id": "data.dev",
                       "auto.offset.reset": "earliest"}
    
    consumer = AvroConsumer(consumer_config)
    consumer.subscribe(['bitcoin_price'])

    while True:
        try:
            message = consumer.poll(5)
        except Exception as e:
            print(f"Exception while trying to poll message - {e}")
        else:
            if message:
                print(f"Successfully poll a record from"
                      f"kafka topic:{message.topic()}, partition:{message.partition()}, offset:{message.offset()} "
                      f"message key:{message.key()}, message value: {message.value()}"
                      )
                consumer.commit()
            else:
                print("No new message at this point. Try again later ..")

    consumer.close()

if __name__ == "__main__":
    read_message()
    