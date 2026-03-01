import hazelcast
import time
from multiprocessing import Process

def run_producer():
    client = hazelcast.HazelcastClient()
    queue = client.get_queue("bounded-queue").blocking()
    
    print("[Продюсер] Починаю запис 100 елементів...")
    for i in range(1, 101):
        print(f"[Продюсер] Спроба запису: {i}...", flush=True)
        queue.put(i)
        print(f"[Продюсер] Успішно записано: {i}")
        
    print("\n[Продюсер] Завершив запис усіх 100 елементів!")
    client.shutdown()

def run_consumer(consumer_id):
    client = hazelcast.HazelcastClient()
    queue = client.get_queue("bounded-queue").blocking()
    
    while True:
        item = queue.take()
        print(f">>> [Консьюмер {consumer_id}] Вичитав елемент: {item}")
        time.sleep(0.05)

if __name__ == "__main__":
    init_client = hazelcast.HazelcastClient()
    init_queue = init_client.get_queue("bounded-queue").blocking()
    init_queue.clear()
    init_client.shutdown()

    print("=== ЕТАП 1: Запуск тільки Продюсера (перевірка блокування) ===")
    p_producer = Process(target=run_producer)
    p_producer.start()
    
    time.sleep(3)
    
    print("\n=== ЕТАП 2: Черга заповнена! Продюсер заблокований. Запускаємо Консьюмерів... ===\n")
    
    p_consumer_1 = Process(target=run_consumer, args=(1,))
    p_consumer_2 = Process(target=run_consumer, args=(2,))
    
    p_consumer_1.start()
    p_consumer_2.start()
    
    p_producer.join()
    
    time.sleep(1)
    
    p_consumer_1.terminate()
    p_consumer_2.terminate()
    
    print("\n=== ТЕСТ ЗАВЕРШЕНО УСПІШНО! ===")