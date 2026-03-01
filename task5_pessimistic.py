import hazelcast
import time
from multiprocessing import Process

def run_client(client_id):
    client = hazelcast.HazelcastClient()
    my_map = client.get_map("pessimistic_map").blocking()
    key_name = "shared_key"
    
    print(f"Клієнт {client_id} почав роботу з песимістичним блокуванням...")
    start_time = time.time()
    
    for _ in range(10000):
        my_map.lock(key_name)
        try:
            value = my_map.get(key_name)
            value += 1
            my_map.put(key_name, value)
        finally:
            my_map.unlock(key_name)
            
    duration = time.time() - start_time
    print(f"Клієнт {client_id} завершив роботу за {duration:.2f} сек.")
    client.shutdown()

if __name__ == "__main__":
    init_client = hazelcast.HazelcastClient()
    init_map = init_client.get_map("pessimistic_map").blocking()
    init_map.put("shared_key", 0) 
    init_client.shutdown()

    print("--- Запуск тесту з Песимістичним Блокуванням ---")
    
    processes = []
    global_start = time.time()
    
    for i in range(1, 4):
        p = Process(target=run_client, args=(i,))
        processes.append(p)
        p.start()
        
    for p in processes:
        p.join()
        
    global_duration = time.time() - global_start
    
    check_client = hazelcast.HazelcastClient()
    final_map = check_client.get_map("pessimistic_map").blocking()
    final_value = final_map.get("shared_key")
    
    print("\n--- РЕЗУЛЬТАТИ ---")
    print(f"Очікуване кінцеве значення: 30000")
    print(f"Фактичне кінцеве значення : {final_value}")
    print(f"Загальний час виконання    : {global_duration:.2f} секунд")
    
    if final_value == 30000:
        print("Висновок: Втрати даних НЕМАЄ, блокування спрацювало успішно!")
    
    check_client.shutdown()