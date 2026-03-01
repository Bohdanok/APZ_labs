import hazelcast
import time
import statistics
from multiprocessing import Process

def worker_no_locks():
    client = hazelcast.HazelcastClient()
    my_map = client.get_map("benchmark_map").blocking()
    for _ in range(10000):
        val = my_map.get("shared_key")
        my_map.put("shared_key", val + 1)
    client.shutdown()

def worker_pessimistic():
    client = hazelcast.HazelcastClient()
    my_map = client.get_map("benchmark_map").blocking()
    for _ in range(10000):
        my_map.lock("shared_key")
        try:
            val = my_map.get("shared_key")
            my_map.put("shared_key", val + 1)
        finally:
            my_map.unlock("shared_key")
    client.shutdown()

def worker_optimistic():
    client = hazelcast.HazelcastClient()
    my_map = client.get_map("benchmark_map").blocking()
    for _ in range(10000):
        while True:
            old_val = my_map.get("shared_key")
            new_val = old_val + 1
            if my_map.replace_if_same("shared_key", old_val, new_val):
                break
    client.shutdown()

def run_test(worker_func):
    client = hazelcast.HazelcastClient()
    my_map = client.get_map("benchmark_map").blocking()
    my_map.put("shared_key", 0)
    client.shutdown()

    processes = []
    start_time = time.time()
    
    for _ in range(3):
        p = Process(target=worker_func)
        processes.append(p)
        p.start()

    for p in processes:
        p.join()
    
    duration = time.time() - start_time
    
    client = hazelcast.HazelcastClient()
    final_val = client.get_map("benchmark_map").blocking().get("shared_key")
    client.shutdown()
    
    return duration, final_val

if __name__ == "__main__":
    methods = {
        "Без блокувань (No Locks)": worker_no_locks,
        "Песимістичне (Pessimistic)": worker_pessimistic,
        "Оптимістичне (Optimistic)": worker_optimistic
    }
    
    results = {}
    
    print("Починаємо масштабний бенчмарк (5 запусків по кожному методу)...")
    
    for name, func in methods.items():
        print(f"\nТестування: {name}")
        times = []
        final_values = []
        
        for i in range(5):
            print(f"  Запуск {i+1}/5...", end="", flush=True)
            duration, final_val = run_test(func)
            times.append(duration)
            final_values.append(final_val)
            print(f" завершено за {duration:.2f} сек.")
            
        mean_time = statistics.mean(times)
        std_time = statistics.stdev(times) if len(times) > 1 else 0.0
        
        results[name] = {
            "times": times,
            "mean": mean_time,
            "std": std_time,
            "values": final_values
        }
        
    print("\n" + "="*60)
    print("ФІНАЛЬНА СТАТИСТИКА (БЕНЧМАРК НА 5 ЗАПУСКІВ)")
    print("="*60)
    for name, stats in results.items():
        print(f"--- {name} ---")
        print(f"Середній час (Mean) : {stats['mean']:.2f} сек")
        print(f"Відхилення (Std Dev): ±{stats['std']:.2f} сек")
        print(f"Час по запусках     : {[round(t, 2) for t in stats['times']]}")
        print(f"Фінальні значення   : {stats['values']}")
        print("-" * 60)

    p_mean = results["Песимістичне (Pessimistic)"]["mean"]
    o_mean = results["Оптимістичне (Optimistic)"]["mean"]
    diff = p_mean - o_mean
    ratio = p_mean / o_mean if o_mean > 0 else 1
    
    print("\n(Diff):")
    if diff > 0:
        print(f"Оптимістичне блокування в середньому на {diff:.2f} сек ШВИДШЕ за песимістичне.")
        print(f"Оптимістичний підхід відпрацював у {ratio:.2f} разів ефективніше!")
    else:
        print(f"Песимістичне блокування виявилось на {abs(diff):.2f} сек швидшим (аномалія високої конкуренції).")