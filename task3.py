import hazelcast

client = hazelcast.HazelcastClient()

my_map = client.get_map("my_distributed_map").blocking()

print("Починаємо запис даних...")

for i in range(1000):
    my_map.put(f"key_{i}", f"value_{i}")

print(f"Запис завершено! Поточний розмір мапи: {my_map.size()}")

client.shutdown()