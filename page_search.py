from datetime import datetime
import random
import sys
import os
import threading


from src.exceptions import InvalidResponse
from src.models import BacklinkTable, IPTableBase, PageTableBase
from src.services import BacklinkService, PageService, URLFrontierService
from src.utils import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import aiohttp
from src.modules.response_converter import ResponseConverter
from lxml.etree import ParserError
from src.database.adapter import load_db_adapter
from src.services.IPService import IPService
from sqlalchemy.exc import SQLAlchemyError
from src.models import LinkType
from src.modules.crawler import Crawler
from src.modules.response_validator import ResponseValidator


async def page_scan_task(obj: IPTableBase|PageTableBase, semaphore):
    if obj.__class__.__name__.startswith(IPTableBase.__basename__):
        page_url = obj.domain or obj.ip
    elif obj.__class__.__name__.startswith(PageTableBase.__basename__):
        page_url = obj.page_url
    else:
        raise ValueError("Invalid object type")

    async with semaphore, aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=config.crawler.req_timeout)) as session:
        try:
            # TODO look for the robots.txt file in the db first
            # robotstxt = crawler.get_robots_txt(page_url)
            # if robotstxt and not crawler.can_fetch(robotstxt, page_url):
            #     print(f"❌ - 🤖 Page Crawl - {page_url} - Disallowed by robots.txt")
            #     return
            
            headers = {
                "User-Agent": config.crawler.user_agent,
            }
            
            async with session.get(page_url, headers=headers) as response:
                response = await ResponseConverter.from_aiohttp(response)
                fails = validator.validate(response)
                if fails:
                    print(f"❌ - 🕷️ Page Crawl - {response.url} ({page_url}) [{response.status_code}] - {[fail.name for fail in fails]}")
                    raise InvalidResponse("Response failed validation")

                meta_tags = crawler.get_meta_tags(response)
                favicon = crawler.get_favicon(response)
                robots_txt = crawler.get_robots_txt(page_url)
                sitemap = crawler.get_sitemap(response)
                last_crawled = datetime.now()

  
                page_obj = page_service.generate_obj(
                    "page_url",
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
                print(f"✅ - 🕷️ Page Crawl - {response.url} ({page_url}) - added to the page session to be committed.")
                
                links = crawler.get_links(response)
                if not links or not all([link.type == LinkType.INVALID for link in links]):
                    print(f"Discovering {len(links)} links...")
                    for link in links:
                        # deleting backlinks from the source to the target to
                        # prevent duplicates. It will be recreated later.
                        backlink_service.delete_backlinks_by_source_to_target_url(page_url, link.full_url)
                    for link in links:
                        if not link.full_url:
                            continue
                        if link.type == LinkType.INTERNAL:
                            if not page_service.get_page(link.full_url):
                                page_service.add_page(
                                    page_service.generate_obj(
                                        "page_url",
                                        page_url=link.full_url,
                                        title=None,
                                        status_code=None,
                                        keywords=None,
                                        description=None,
                                        body=None,
                                        favicon=None,
                                        robotstxt=None,
                                        sitemap=None,
                                        last_crawled=None,
                                    )
                                )
                                print(f"✅ - ↩️ Internal Page Discover - ({link.full_url}) - added to the page session to be committed.")
                            else:
                                print(f"⚠️ - ↩️ Internal Page Discover - ({link.full_url}) - already exists in the database.")
                        elif link.type == LinkType.EXTERNAL:
                            target_ip = ip_service.get_ip_by_domain(link.full_url)
                            if not target_ip:
                                # add new ip to URL frontier instead of directly adding to IP table
                                # because we have not send a request to the IP to validate it yet.
                                # we could, but it should not be the responsibility of the page scan task
                                if not url_frontier_service.get_url(link.full_url):
                                    url_frontier_service.add_url(link.full_url)
                                    print(f"✅ - 🌐 External Page Discover - ({link.full_url}) - added to the URL frontier.")
                                else :
                                    print(f"⚠️ - 🌐 External Page Discover - ({link.full_url}) - already exists in the URL frontier.")
                            backlink_service.add_backlink(BacklinkTable(source_url=page_url, target_url=link.full_url, anchor_text=link.anchor_text))
                            print(f"✅ - 🔗 External Page Discover: Backlink - ({page_url}) -> ({link.full_url}) - added to the backlink session to be committed.")
                else:
                    print("No valid links found.")
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
        # except Exception as e:
        #     print("❌ - 🕷️ CRITICAL ERROR:", e.__class__.__name__, e)


async def generate_page_scan_tasks(semaphore, limit=10):
    calculate_ratio = lambda x, y: x / (x + y)

    ips = ip_service.get_unscanned_ips()
    pages = page_service.get_unscanned_pages()
    
    # randomize to prevent stale / unreachable IPs from being scanned over and over
    driver = db_adapter.engine.url.get_dialect().driver
    if driver == "pysqlite":
        random.shuffle(ips)
        random.shuffle(pages)
    elif driver == "pymssql":
        random.shuffle(ips)
        random.shuffle(pages)
    else:
        raise NotImplementedError(f"Driver {driver} not supported")

    if not len(ips) and not len(pages):
        print("No pages or IPs to scan.")
        return

    # distribute limit proportionally
    ip_limit = int(limit * calculate_ratio(len(ips), len(pages)))
    page_limit = limit - ip_limit
    
    # prevent total dominance
    if ip_limit == 0 and len(ips) > 0:
        ip_limit = 1
        page_limit -= 1
    if page_limit == 0 and len(pages) > 0:
        page_limit = 1
        ip_limit -= 1

    ips = ips[:ip_limit]
    pages = pages[:page_limit]

    print(f"Generating task with {ip_limit} IPs and {page_limit} pages.")
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
    await asyncio.gather(*tasks)

validator = ResponseValidator()
crawler = Crawler(config.crawler)

db_adapter = load_db_adapter()

ip_service = IPService(db_adapter)
page_service = PageService(db_adapter)
url_frontier_service = URLFrontierService(db_adapter)
backlink_service = BacklinkService(db_adapter)


print("Initial pages:", page_service.count())

print("Starting page scan...")

stop_event = threading.Event()

async def main():
    try:
        if stop_event.is_set():
            raise KeyboardInterrupt
        semaphore = asyncio.Semaphore(config.crawler.max_workers.page_search)
        await generate_page_scan_tasks(semaphore)
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
        backlink_service.commit(verbose=False)
        print("Total pages:", len(page_service.get_pages()))

async def run():
    while True:
        try:
            await main()
            print("Finished scanning pages...")
            if page_service.count_unscanned_pages() == 0:
                await asyncio.sleep(30)
            else:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            print("Interrupted by user")
            break
        except:
            pass
if __name__ == "__main__":
    asyncio.run(run())