import json
import socket
import sys
import os
from threading import Thread, Event
import threading
import time

from tqdm import tqdm


from src.exceptions import InvalidResponse
from src.models import Config, IPTable, SystemConfig
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import aiohttp
from src.utils import ResponseConverter, ping
from lxml.etree import ParserError
from src.database.adapter import DBAdapter, load_db_adapter
from src.services.IPService import IPService
from sqlalchemy.exc import SQLAlchemyError
from src.models import DocumentIndexTable
from src.services.DocumentIndexService import DocumentIndexService

from src.modules.crawler import Crawler
from src.modules.response_validator import ResponseValidator


async def ip_scan_task(ip, ports, semaphore):
    async with semaphore, aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=config.crawler.req_timeout)) as session:
        global active_requests
        
        for port in ports:
            is_https = port == 443
            ip_template = "http{}://{}:{}"
            full_url = ip_template.format("s" if is_https else "", ip, port)
            
            try:
                # if ping(full_url) is False:
                #     print(f"❌ - {full_url} - [PING FAILED]")
                #     raise InvalidResponse("Ping failed")

                # check if ip is already in the database
                # TEMPORARILY DISABLED
                # if ip_service.get_ip(ip):
                #     should_be_rechecked = True  # TODO implement recheck function, ex: if last_checked < 1 week ago
                #     if not should_be_rechecked: 
                #         print(f"Skipping {full_url} - already scanned")
                #         continue
                
                headers = {
                    "User-Agent": config.crawler.user_agent,
                }
                async with session.get(full_url, headers=headers) as response:
                    response = await ResponseConverter.from_aiohttp(response)
                    fails = validator.validate(response)
                    if fails:
                        print(f"❌ - {response.url} ({full_url}) [{response.status_code}] - {[fail.name for fail in fails]}")
                        raise InvalidResponse("Response failed validation")

                    # meta_tags = crawler.get_meta_tags(response)
                    # links = crawler.get_links(response)
                    # document_frequency = crawler.get_document_frequency(response)
                    
                    # if document_frequency:
                    #     for word, freq in document_frequency.items():
                    #         document_index = DocumentIndex(
                    #             document_id=response.url,
                    #             word=word,
                    #             frequency=freq
                    #         )
                    #         document_index_service.add_document_index(document_index)

                    try:
                        domain_name = socket.gethostbyaddr(ip)[0]
                        if not domain_name.startswith("http" if is_https else "https"):
                            domain_name = f"http{'s' if is_https else ''}://{domain_name}"
                    except socket.herror:
                        domain_name = response.url if response.url != ip else None

                    obj = IPTable(
                        ip=ip,
                        domain=domain_name,
                        port=port,
                        status=response.status_code,
                    )
                    ip_service.add_ip(obj)
                    print(f"✅ - ({domain_name}) - ({ip}:{port}) - [{response.status_code}] - added to the session to be committed.")
                    
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
            except KeyboardInterrupt:
                raise KeyboardInterrupt
            except Exception as e:
                print("CRITICAL ERROR:", e.__class__.__name__, e)


async def ip_range_scan_task(semaphore, ip_ranges = ((0, 16), (0, 16), (0, 16), (0, 16)), ports = (80, 443)):
    tasks = []
    for a in tqdm(range(ip_ranges[0][0], ip_ranges[0][1]), desc=f"Queueing IPs for chunk {ip_ranges}"):
        for b in range(ip_ranges[1][0], ip_ranges[1][1]):
            for c in range(ip_ranges[2][0], ip_ranges[2][1]):
                for d in range(ip_ranges[3][0], ip_ranges[3][1]):
                    ip = f"{a}.{b}.{c}.{d}"
                    tasks.append(ip_scan_task(ip, ports=ports, semaphore=semaphore))
    await asyncio.gather(*tasks)


def generate_chunks(config:Config) -> list[tuple[tuple[int, int], tuple[int, int], tuple[int, int], tuple[int, int]]]:
    chunk_size = config.crawler.chunk_size
    if chunk_size > 256 or chunk_size < 1 or 256 % chunk_size != 0:
        raise ValueError("Invalid chunk size")
    chunks = []
    for a in range(0, 256, chunk_size):
        for b in range(0, 256, chunk_size):
            for c in range(0, 256, chunk_size):
                for d in range(0, 256, chunk_size):
                    chunks.append(((a, a+chunk_size), (b, b+chunk_size), (c, c+chunk_size), (d, d+chunk_size)))
    
    # distrubuting the chunks among n amount of scripts running in parallel
    machines = config.system.total_machines
    machine_id = config.system.machine_id
    if machine_id >= machines:
        raise ValueError("Invalid machine id")

    # check if config is valid and no chunk is left out
    if len(chunks) % machines != 0:
        print("Warning: Chunk size is not divisible by total machines, trying to distribute chunks evenly")
        remaining_chunks = len(chunks) % machines
        # add remaining chunks to the last n machines
        for i in range(remaining_chunks):
            chunks[-(i+1)] = (chunks[-(i+1)],)
        
    responsible_chunks = [chunk for i, chunk in enumerate(chunks) if i % machines == machine_id]
    print("Total chunks:", len(chunks))
    print(f"Your Machine (Machine {machine_id}) is responsible for {len(responsible_chunks)} chunks and {len(responsible_chunks) * config.crawler.chunk_size ** 4} IPs")
    
    return responsible_chunks


if __name__ == "__main__":    

    with open("config.json") as f:
        config = Config(**json.load(f))

    validator = ResponseValidator()
    db_adapter = load_db_adapter()
    
    ip_service = IPService(db_adapter)
    document_index_service = DocumentIndexService(db_adapter)
    
    print("Initial ips:", len(ip_service.get_ips()))

    print("Starting IP scan...")
    chunks = generate_chunks(config)

    stop_event = threading.Event()

    def process_chunks(chunks):
        for chunk in chunks:
            if stop_event.is_set():
                print("Interrupted by user")
                break
            print("Processing chunk:", chunk)
            try:
                semaphore = asyncio.Semaphore(config.crawler.max_workers)
                asyncio.run(ip_range_scan_task(semaphore, chunk, ports=config.crawler.ports))
                print("IP scan complete for chunk")
            except Exception as e:
                print("CRITICAL ERROR:", e.__class__.__name__, e)
                raise e
            except KeyboardInterrupt:
                print("Interrupted by user")
                break
            finally:
                print("Committing changes...")
                ip_service.commit()
                document_index_service.commit()
                print("Total valid IPs:", len(ip_service.get_valid_ips()))
                time.sleep(1)

    parallelism = config.crawler.parallelism
    threads = []
    thread_chunk_size = len(chunks) // parallelism

    try:
        for i in range(parallelism):
            t = threading.Thread(target=process_chunks, args=(chunks[i*thread_chunk_size:(i+1)*thread_chunk_size],))
            t.daemon = True
            print(f"Starting thread {t.name} ({i+1}/{parallelism})")
            t.start()
            threads.append(t)

        # Wait for interruption
        while not stop_event.is_set():
            time.sleep(1)

    except (KeyboardInterrupt, SystemExit):
        print('Received keyboard interrupt, safely quitting threads. Wait for threads to finish...')
        stop_event.set()

    for t in threads:
        t.join()  # Wait for all threads to finish