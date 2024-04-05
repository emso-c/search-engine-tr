from datetime import datetime
import json
import socket
import sys
import os
import threading
import time

from tqdm import tqdm


from src.exceptions import InvalidResponse
from src.models import Config, IPTable, PageTable
from src.services import PageService, URLFrontierService
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import aiohttp
from src.utils import ResponseConverter, ping
from lxml.etree import ParserError
from src.database.adapter import DBAdapter, load_db_adapter
from src.services.IPService import IPService
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.expression import func
from src.models import DocumentIndexTable
from src.services.DocumentIndexService import DocumentIndexService
from src.models import LinkType
from src.modules.crawler import Crawler
from src.modules.response_validator import ResponseValidator


async def page_scan_task(obj: IPTable|PageTable, semaphore):
    if isinstance(obj, IPTable):
        page_url = obj.domain or obj.ip
    elif isinstance(obj, PageTable):
        page_url = obj.page_url
    else:
        raise ValueError("Invalid object type")

    async with semaphore, aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=config.crawler.req_timeout)) as session:
        try:
            headers = {
                "User-Agent": config.crawler.user_agent,
            }
            
            async with session.get(page_url, headers=headers) as response:
                response = await ResponseConverter.from_aiohttp(response)
                fails = validator.validate(response)
                if fails:
                    print(f"❌ - Page Crawl - {response.url} ({page_url}) [{response.status_code}] - {[fail.name for fail in fails]}")
                    raise InvalidResponse("Response failed validation")

                meta_tags = crawler.get_meta_tags(response)
                favicon = crawler.get_favicon(response)
                robots_txt = crawler.get_robots_txt(response)
                sitemap = crawler.get_sitemap(response)
                last_crawled = datetime.now()

                page_obj = PageTable(
                    page_url=response.url,
                    title=meta_tags.title,
                    status_code=response.status_code,
                    keywords=meta_tags.keywords,
                    description=meta_tags.description,
                    body=response.content_bytes,
                    favicon=favicon,
                    robotstxt=robots_txt,
                    sitemap=sitemap,
                    last_crawled=last_crawled,
                )
                
                # Update the objects last_crawled
                obj.last_crawled = last_crawled
                page_service.upsert_page(page_obj)
                print(f"✅ - Page Crawl - {response.url} ({page_url}) - added to the page session to be committed.")
                
                links = crawler.get_links(response)
                if not links or not all([link.type == LinkType.INVALID for link in links]):
                    print("Discovering links...")
                    link_score_increment_map = {}
                    for link in links: link_score_increment_map[link.full_url] = False
                    for link in links:
                        if not link.full_url:
                            continue
                        if link.type == LinkType.INTERNAL:
                            if not page_service.get_page(link.full_url):
                                page_service.add_page(PageTable(page_url=link.full_url, last_crawled=None))
                                print(f"✅ - 🔗 Internal Page Discover - ({link.full_url}) - added to the page session to be committed.")
                            else:
                                print(f"⚠️ - 🔗 Internal Page Discover - ({link.full_url}) - already exists in the database.")
                        elif link.type == LinkType.EXTERNAL:
                            target_ip = ip_service.get_ip_by_domain(link.full_url)
                            if not target_ip:
                                # add new ip to URL frontier instead of directly adding to IP table
                                # because we have not send a request to the IP to validate it yet.
                                # we could, but it should not be the responsibility of the page scan task
                                searched_url = url_frontier_service.get_url(link.full_url)
                                if not searched_url:
                                    url_frontier_service.add_url(link.full_url)
                                    print(f"✅ - 🌐 External Page Discover: URL Frontier - ({link.full_url}) - added to the URL frontier.")
                                else:
                                    if not link_score_increment_map.get(link.full_url):
                                        searched_url.score += 1
                                        link_score_increment_map[link.full_url] = True
                                        print(f"🔼 - 🌐 External Page Discover: URL Frontier Score Increment - incremented score of URL {link.full_url} ({searched_url.score}) from {page_url}.")
                                    else:
                                        print(f"⚠️ - 🌐 External Page Discover: URL Frontier Score Increment - already incremented score of URL {link.full_url}")
                            elif target_ip:
                                if not link_score_increment_map[link.full_url]:
                                    target_ip.score += 1
                                    link_score_increment_map[link.full_url] = True
                                    print(f"🔼 - 🌐 External Page Discover: IP Score Increment - ({link.full_url}) - incremented score of IP {target_ip.ip} ({target_ip.score}) from {page_url}.")
                                else:
                                    print(f"⚠️ - 🌐 External Page Discover: IP Score Increment - ({link.full_url}) - already incremented score of IP {target_ip.ip}")
                else:
                    print("No valid links found.")
        # TODO handle exceptions
        except (
            SQLAlchemyError,
            aiohttp.ClientConnectorError,
            aiohttp.ClientOSError,
            asyncio.TimeoutError,
            aiohttp.ServerDisconnectedError,
            ConnectionResetError,
            aiohttp.ClientResponseError,
            InvalidResponse, ParserError,
            ValueError):
            # ValueError Unicode strings with encoding declaration are not supported. Please use bytes input or XML fragments without declaration.
            # Might need to handle this later
            # print(f"❌ - Page Crawl - ({page_url}) - ValueError", e.__class__.__name__, e)
            pass
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except Exception as e:
            print("CRITICAL ERROR:", e.__class__.__name__, e)


async def generate_page_scan_tasks(semaphore, limit=100):
    calculate_ratio = lambda x, y: x / (x + y)

    
    # TODO randomize to prevent stale / unreachable IPs from being scanned over and over
    ips = ip_service.db_adapter.get_session().query(IPTable).filter_by(last_crawled=None) #.order_by(func.random())
    pages = page_service.db_adapter.get_session().query(PageTable).filter_by(last_crawled=None) #.order_by(func.random())
    
    if not ips.count() and not pages.count():
        print("No pages or IPs to scan.")
        return
    ip_limit = int(limit * calculate_ratio(ips.count(), pages.count()))
    page_limit = limit - ip_limit
    ips = ips.limit(ip_limit)
    pages = pages.limit(page_limit)
    print(f"Generating task with {ips.count()} ({ip_limit}) IPs and {pages.count()} ({page_limit}) pages.") 
    tasks = []
    if ip_limit:
        for ip_obj in ips:
            task_name = f"IP-Task-{ip_obj.domain or ip_obj.ip}"
            print("Generating task:", task_name)
            tasks.append(page_scan_task(ip_obj, semaphore))
    if page_limit:
        for page_obj in pages:
            task_name = f"Page-Task-{page_obj.page_url}"
            print("Generating task:", task_name)
            tasks.append(page_scan_task(page_obj, semaphore))
    # shuffle here?
    await asyncio.gather(*tasks)


with open("config.json") as f:
    config = Config(**json.load(f))

validator = ResponseValidator()
crawler = Crawler(config.crawler)

db_adapter = load_db_adapter()

ip_service = IPService(db_adapter)
page_service = PageService(db_adapter)
url_frontier_service = URLFrontierService(db_adapter)

print("Initial pages:", len(page_service.get_pages()))

print("Starting page scan...")

stop_event = threading.Event()

def run():  
    def main():
        try:
            if stop_event.is_set():
                raise KeyboardInterrupt
            semaphore = asyncio.Semaphore(config.crawler.max_workers)
            # weird fix: https://stackoverflow.com/questions/65682221/runtimeerror-exception-ignored-in-function-proactorbasepipetransport
            asyncio.get_event_loop().run_until_complete((generate_page_scan_tasks(semaphore)))
            semaphore.release()
        except Exception as e:
            print("CRITICAL ERROR:", e.__class__.__name__, e)
            raise e
        except KeyboardInterrupt:
            print("Interrupted by user")
        finally:
            print("Committing changes...")
            ip_service.commit(verbose=False)
            page_service.commit(verbose=False)
            url_frontier_service.commit(verbose=False)
            print("Total pages:", len(page_service.get_pages()))
            time.sleep(0.1)

    while True:
        main()
        print("Finished scanning pages. Waiting for 30 seconds before starting again...")
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