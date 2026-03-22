import time
import httpx
import asyncio

URL = "http://localhost:8000/facade"
NUM_REQUESTS = 100

async def run_performance_test():
    print(f"Починаємо тест продуктивності: відправка {NUM_REQUESTS} POST запитів...")
    
    async with httpx.AsyncClient() as client:
        start_time = time.time()
        
        tasks = [
            client.post(URL, json={"msg": f"perf_msg_{i}"}) 
            for i in range(NUM_REQUESTS)
        ]
        
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        
        end_time = time.time()
        
        successful = sum(1 for r in responses if isinstance(r, httpx.Response) and r.status_code == 200)
        total_time = end_time - start_time
        throughput = NUM_REQUESTS / total_time
        
        print("-" * 30)
        print(f"Успішних запитів: {successful}/{NUM_REQUESTS}")
        print(f"Загальний час виконання: {total_time:.3f} секунд")
        print(f"Пропускна здатність: {throughput:.2f} req/sec")
        print(f"Середня затримка: {(total_time / NUM_REQUESTS) * 1000:.2f} ms")

if __name__ == "__main__":
    asyncio.run(run_performance_test())
