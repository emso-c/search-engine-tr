import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import aiohttp
import socket
# from src.database.adapter import DBAdapter
# from src.services.IPService import IPService
from src.temp import DBAdapter, IPService, IPTable
from sqlalchemy.exc import SQLAlchemyError

MAX_CONCURRENT_REQUESTS = 2048
CLIENT_TIMEOUT = 10 # seconds

active_requests = 0
counter_lock = asyncio.Lock()

semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
db_adapter = DBAdapter("sqlite:///db/ip.db")
ip_service = IPService(db_adapter)
print("initial ips:", len(ip_service.get_ips()))

async def ip_scan_task(ip, ports, semaphore=semaphore):
    async with semaphore, aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=CLIENT_TIMEOUT)) as session:
        global active_requests
        
        for port in ports:
            async with counter_lock:
                active_requests += 1
            await asyncio.sleep(0.01)  # Add a small delay to offset the time it takes to acquire the lock
            # print(f"Active requests: {active_requests}")
            
            try:
                is_https = port == 443
                ip_template = "http{}://{}:{}"
                full_url = ip_template.format("s" if is_https else "", ip, port)
                
                # check if ip is already in the database
                if ip_service.get_ip(ip):
                    should_be_rechecked = True  # TODO implement recheck function, ex: if last_checked < 1 week ago
                    if not should_be_rechecked: 
                        print(f"Skipping {full_url} - already scanned")
                        continue
                
                async with session.get(full_url) as response:
                    if response.status == 200:
                        try:
                            domain_name = socket.gethostbyaddr(ip)[0]
                        except socket.herror:
                            domain_name = None
                        accepted_addr = ip_template.format("s" if is_https else "", domain_name if domain_name else ip, port)
                        ip_obj = ip_service.upsert_ip(ip, domain_name, port, response.status)
                        print(f"Valid address found: {accepted_addr} ({ip_obj.ip}) - {response.status}")
            
            # TODO handle exceptions
            except SQLAlchemyError as e:
                pass
            except aiohttp.ClientConnectorError as e:
                pass
            except aiohttp.ClientOSError as e:
                pass
            except asyncio.TimeoutError as e:
                pass
            except aiohttp.ServerDisconnectedError as e:
                pass
            except aiohttp.ClientResponseError as e:
                pass
            except Exception as e:
                print(e.__class__.__name__, e)
                exit()
            finally:
                async with counter_lock:
                    active_requests -= 1

async def ip_range_scan_task(ip_ranges = ((0, 16), (0, 16), (0, 16), (0, 16)), ports = (80, 443)):
    tasks = []
    
    for a in tqdm(range(ip_ranges[0][0], ip_ranges[0][1]), desc=f"Queueing IPs for chunk {ip_ranges}"):
        for b in range(ip_ranges[1][0], ip_ranges[1][1]):
            for c in range(ip_ranges[2][0], ip_ranges[2][1]):
                for d in range(ip_ranges[3][0], ip_ranges[3][1]):
                    ip = f"{a}.{b}.{c}.{d}"
                    tasks.append(ip_scan_task(ip, ports=ports))
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    print("Starting IP search...")

    chunk_size = 8
    chunks = []
    from tqdm import tqdm
    for a in tqdm(range(0, 256, chunk_size), total=256//chunk_size, desc="Generating chunks"):
        for b in range(0, 256, chunk_size):
            for c in range(0, 256, chunk_size):
                for d in range(0, 256, chunk_size):
                    chunks.append(((a, a+chunk_size), (b, b+chunk_size), (c, c+chunk_size), (d, d+chunk_size)))

    for chunk in chunks:
        print(f"Scanning chunk: {chunk}")
        asyncio.run(ip_range_scan_task(chunk))
        print("IP scan complete. Total valid IPs:", len(ip_service.get_valid_ips()))
