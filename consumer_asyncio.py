import asyncio

from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic

BROKER_URL = "PLAINTEXT://localhost:9092"

async def consumer(topic_name):

    await asyncio.sleep(1.5)

    c = Consumer({
        "bootstrap.servers": BROKER_URL,
        "group.id" : "0",
        "auto.offset.reset": "earliest"
    })

    c.subscribe([topic_name], on_assign=on_assign)

    while True:
        message = c.consume(10, timeout=1.0)
        for message in messages:
            if message is None:
                print("no message received")
            elif message.error() is not None:
                print(f"error from consumer: {message.error()}")
            else:
                print(f"message {message.key()}, {message.value()}")
        await asyncio.sleep(.1)

def main():
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    try:
        asyncio.run(produce_consume("com.my_work"))
    except KeyboardInterrupt as e:
        print("shutting down")
