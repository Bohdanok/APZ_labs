import hazelcast

client = hazelcast.HazelcastClient()
queue = client.get_queue("bounded-queue").blocking()

print("Продюсер починає запис 100 елементів...")
for i in range(1, 101):
    print(f"Спроба запису елемента {i}...", end=" ", flush=True)
    queue.put(i)
    print("Успішно!")

print("Продюсер завершив роботу!")
client.shutdown()