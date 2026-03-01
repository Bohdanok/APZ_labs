import hazelcast
import time

client = hazelcast.HazelcastClient()
queue = client.get_queue("bounded-queue").blocking()

print("Консьюмер запущено. Очікування даних...")
while True:
    item = queue.take()
    print(f"Вичитано елемент: {item}")
    time.sleep(0.1)