import httpx
import threading

client = httpx.Client()
def worker():
    print(client)
    
t1 = threading.Thread(target=worker)
t1.start()
t1.join()
