import json
import socket
import sys
import os

import tqdm

from src.exceptions import InvalidResponse
from src.models import Config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import aiohttp
from src.utils import ResponseConverter, ping
from lxml.etree import ParserError
from src.database.adapter import DBAdapter
from src.services.IPService import IPService
from sqlalchemy.exc import SQLAlchemyError

from src.modules.crawler import Crawler
from src.modules.response_validator import ResponseValidator

active_requests = 0
counter_lock = asyncio.Lock()

async def ip_scan_task(ip, ports, semaphore):
    async with semaphore, aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=config.crawler.req_timeout)) as session:
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
                
                # if ping(full_url) is False:
                #     print(f"❌ - {full_url} - [PING FAILED]")
                #     raise InvalidResponse("Ping failed")

                # check if ip is already in the database
                if ip_service.get_ip(ip):
                    should_be_rechecked = True  # TODO implement recheck function, ex: if last_checked < 1 week ago
                    if not should_be_rechecked: 
                        print(f"Skipping {full_url} - already scanned")
                        continue
                
                headers = {
                    "User-Agent": config.crawler.user_agent,
                }
                async with session.get(full_url, headers=headers) as response:
                    response = await ResponseConverter.from_aiohttp(response)
                    fails = validator.validate(response)
                    if fails:
                        print(f"❌ - {response.url} ({full_url}) [{response.status_code}] - {[fail.name for fail in fails]}")
                        raise InvalidResponse("Response failed validation")

                    meta_tags = crawler.get_meta_tags(response)
                    # links = crawler.get_links(response)
                    # document_frequency = crawler.get_document_frequency(response)

                    try:
                        domain_name = socket.gethostbyaddr(ip)[0]
                    except socket.herror:
                        domain_name = response.url if response.url != ip else None

                    ip_obj = ip_service.upsert_ip(
                        ip, domain_name, port, response.status_code,
                        title=meta_tags.title,
                        keywords=meta_tags.keywords,
                        description=meta_tags.description,
                        body=response.body
                    )
                    print(f"✅ - ({domain_name}) - ({ip_obj.ip}:{ip_obj.port}) - [{response.status_code}]")
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
            except InvalidResponse as e:
                pass
            except ParserError as e:
                pass
            except ValueError as e:
                # ValueError Unicode strings with encoding declaration are not supported. Please use bytes input or XML fragments without declaration.
                # Might need to handle this later
                pass
            except Exception as e:
                print(e.__class__.__name__, e)
                exit()
            finally:
                async with counter_lock:
                    active_requests -= 1

async def ip_range_scan_task(semaphore, ip_ranges = ((0, 16), (0, 16), (0, 16), (0, 16)), ports = (80, 443)):
    tasks = []
    
    for a in tqdm(range(ip_ranges[0][0], ip_ranges[0][1]), desc=f"Queueing IPs for chunk {ip_ranges}"):
        for b in range(ip_ranges[1][0], ip_ranges[1][1]):
            for c in range(ip_ranges[2][0], ip_ranges[2][1]):
                for d in range(ip_ranges[3][0], ip_ranges[3][1]):
                    ip = f"{a}.{b}.{c}.{d}"
                    tasks.append(ip_scan_task(ip, ports=ports, semaphore=semaphore))
    await asyncio.gather(*tasks)


if __name__ == "__main__":    

    with open("config.json") as f:
        config = Config(**json.load(f))

    crawler = Crawler(config.crawler)
    validator = ResponseValidator()
    db_adapter = DBAdapter("sqlite:///data/ip.db")
    ip_service = IPService(db_adapter)
    print("Initial ips:", len(ip_service.get_ips()))

    print("Starting IP scan...")
    chunks = []
    chunk_size = config.crawler.chunk_size
    from tqdm import tqdm
    for a in tqdm(range(0, 256, chunk_size), total=256//chunk_size, desc="Generating chunks"):
        for b in range(0, 256, chunk_size):
            for c in range(0, 256, chunk_size):
                for d in range(0, 256, chunk_size):
                    chunks.append(((a, a+chunk_size), (b, b+chunk_size), (c, c+chunk_size), (d, d+chunk_size)))

    for chunk in chunks:
        semaphore = asyncio.Semaphore(config.crawler.max_workers)
        asyncio.run(ip_range_scan_task(semaphore, chunk, ports=config.crawler.ports))
        print("IP scan complete. Total valid IPs:", len(ip_service.get_valid_ips()))
