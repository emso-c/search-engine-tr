import json
import random
import socket
import sys
import os
import threading
import time


from src.exceptions import InvalidResponse
from src.models import Config, IPTable, URLFrontierTable
from src.services import URLFrontierService
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import aiohttp
from src.utils import ResponseConverter, ping
from lxml.etree import ParserError
from src.database.adapter import load_db_adapter
from src.services.IPService import IPService
from sqlalchemy.exc import SQLAlchemyError

from src.modules.response_validator import ResponseValidator


def _resolve_domain(domain):
    from urllib.parse import urlparse
    return urlparse(domain).hostname

async def url_frontier_scan_task(url_obj: URLFrontierTable, ports, semaphore):
    async with semaphore, aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=config.crawler.req_timeout)) as session:
        host = _resolve_domain(url_obj.url)
        try:
            ip = socket.gethostbyname(host)
        except socket.gaierror:
            print(f"❌ - {url_obj.url} - [DNS RESOLUTION FAILED]")
            return
        except Exception as e:
            print("CRITICAL ERROR:", e.__class__.__name__, e)
            return

        for port in ports:
            is_https = port == 443
            ip_template = "http{}://{}:{}"
            full_url = ip_template.format("s" if is_https else "", ip, port)
            headers = {
                "User-Agent": config.crawler.user_agent,
            }
            
            try:                
                async with session.get(full_url, headers=headers) as response:
                    response = await ResponseConverter.from_aiohttp(response)
                    fails = validator.validate(response)
                    if fails:
                        print(f"❌ - {response.url} ({full_url}) [{response.status_code}] - {[fail.name for fail in fails]}")
                        raise InvalidResponse("Response failed validation")


                    obj = IPTable(
                        ip=ip,
                        domain=url_obj.url,
                        port=port,
                        status=response.status_code,
                        score=url_obj.score
                    )
                    ip_service.add_ip(obj)  # might need to be upsert, but I'm sure the IP/domain is unique by the time it gets here
                    print(f"✅ - ({url_obj.url}) - ({ip}:{port}) - [{response.status_code}] - added to the session to be committed.")
                    
                    url_frontier_service.delete_url(url_obj.url)
                    print(f"✅ - ({url_obj.url}) - removed from the URL frontier.")
                    
            # TODO handle exceptions
            except (
                SQLAlchemyError,
                aiohttp.ClientConnectorError,
                aiohttp.ClientOSError,
                asyncio.TimeoutError,
                aiohttp.ServerDisconnectedError,
                aiohttp.ClientResponseError,
                InvalidResponse,
                ParserError,
                ValueError,
                ):
                pass
            except KeyboardInterrupt:
                raise KeyboardInterrupt
            except Exception as e:
                print("CRITICAL ERROR:", e.__class__.__name__, e)


async def url_frontier_task_generator(semaphore, limit=100):
    tasks = []
    ports = config.crawler.ports
    urls = url_frontier_service.db_adapter.get_session() \
            .query(URLFrontierTable) \
            .order_by(URLFrontierTable.score.desc()) \
            .limit(limit) \
            .all()
    for url_obj in urls:
        tasks.append(url_frontier_scan_task(url_obj, ports, semaphore))
    # tasks.append(ip_scan_task(ip, ports=ports, semaphore=semaphore))
    await asyncio.gather(*tasks)

with open("config.json") as f:
        config = Config(**json.load(f))

validator = ResponseValidator()
db_adapter = load_db_adapter()

ip_service = IPService(db_adapter)
url_frontier_service = URLFrontierService(db_adapter)

print("Initial URL Frontier size:", len(url_frontier_service.get_urls()))

print("Starting URL Frontier scan...")

stop_event = threading.Event()

def main():
    try:
        if stop_event.is_set():
            raise KeyboardInterrupt
        semaphore = asyncio.Semaphore(config.crawler.max_workers)
        asyncio.run(url_frontier_task_generator(semaphore))
        print("URL Frontier scan complete")
    except Exception as e:
        print("CRITICAL ERROR:", e.__class__.__name__, e)
        raise e
    except KeyboardInterrupt:
        print("Interrupted by user")
    finally:
        print("Committing changes...")
        ip_service.commit(verbose=False)
        url_frontier_service.commit(verbose=False)
        print("Total URLs in URL Frontier:", len(url_frontier_service.get_urls()))
        time.sleep(1)

def run():    
    while True:
        main()
        print("Finished one iteration of URL Frontier scan. Waiting 30 seconds for data to be added to the URL Frontier...")
        time.sleep(30)

if __name__ == "__main__":
    run()

    # parallelism = config.crawler.parallelism
    # threads = []

    # try:
    #     for i in range(parallelism):
    #         t = threading.Thread(target=main)
    #         t.daemon = True
    #         print(f"Starting thread {t.name} ({i+1}/{parallelism})")
    #         t.start()
    #         threads.append(t)

    #     # Wait for interruption
    #     while not stop_event.is_set():
    #         time.sleep(1)

    # except (KeyboardInterrupt, SystemExit):
    #     print('Received keyboard interrupt, safely quitting threads. Wait for threads to finish...')
    #     stop_event.set()

    # for t in threads:
    #     t.join()  # Wait for all threads to finish