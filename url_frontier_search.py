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
from urllib.parse import urlparse

def _resolve_domain(domain):
    return urlparse(domain).hostname

def get_base_url(url):
    "example https://www.google.com/products/1 -> https://www.google.com"
    parsed = urlparse(url)
    return parsed.scheme + "://" + parsed.netloc

async def url_frontier_scan_task(url_obj: URLFrontierTable, semaphore):
    async with semaphore, aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=config.crawler.req_timeout)) as session:
        try:
            base_url = get_base_url(url_obj.url)
            ip = None
            try:
                _host = _resolve_domain(url_obj.url)
                ip = socket.gethostbyname(_host)
            except socket.gaierror:
                print(f"❌ - {url_obj.url} - [DNS RESOLUTION FAILED]")
            except Exception as e:
                print("❌❌❌ CRITICAL ERROR RESOLVING DNS:", e.__class__.__name__, e)
            
            port = 80 if urlparse(url_obj.url).scheme == "http" else 443
            headers = {
                "User-Agent": config.crawler.user_agent,
            }
            async with session.get(base_url, headers=headers) as response:
                response = await ResponseConverter.from_aiohttp(response)
                fails = validator.validate(response)
                if fails:
                    raise InvalidResponse("Response failed validation")

                if response.url != base_url:
                    with open("redirects.txt", "a") as f:
                        f.write(f"{base_url} -> {response.url}\n")
                    # TODO handle redirects

                obj = IPTable(
                    domain=base_url,
                    ip=ip,
                    port=port,
                    status=response.status_code,
                    score=url_obj.score
                )
                ip_service.safe_add_url(obj)
                print(f"✅ - {obj.domain} - ({ip}:{port}) - [{obj.status}] - {obj.score} - added to IP table session.")
                
                url_frontier_service.delete_url(url_obj.url)
                print(f"✅ - Cleared - {url_obj.url} removed from the URL frontier.")
                
        # TODO handle exceptions
        except (
            SQLAlchemyError,
            aiohttp.ClientConnectorError,
            aiohttp.ClientOSError,
            asyncio.TimeoutError,
            aiohttp.ServerDisconnectedError,
            aiohttp.ClientResponseError,
            ParserError,
            ValueError,
            ):
            # TODO maybe implement error counter and timeout?
            print(f"❌ - General Error - Removing {url_obj.url} from URL Frontier due to error")
            url_frontier_service.delete_url(url_obj.url)
        except InvalidResponse:
            print(f"❌ - Validation Error - {response.url} ({ip}) [{response.status_code}] - {[fail.name for fail in fails]}")
            url_frontier_service.delete_url(url_obj.url)
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except (Exception) as e:
            print("CRITICAL ERROR:", e.__class__.__name__, e)
            url_frontier_service.db_adapter.get_session().rollback()
        finally:
            print("Committing changes...")
            ip_service.commit(verbose=False)
            url_frontier_service.commit(verbose=False)


async def url_frontier_task_generator(semaphore, limit=50):
    tasks = []
    urls = url_frontier_service.db_adapter.get_session() \
            .query(URLFrontierTable) \
            .order_by(URLFrontierTable.score.desc()) \
            .limit(limit) \
            .all()
    for url_obj in urls:
        tasks.append(url_frontier_scan_task(url_obj, semaphore))
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
        # ip_service.commit(verbose=False)
        # url_frontier_service.commit(verbose=False)
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