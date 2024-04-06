import re

from bs4 import BeautifulSoup
from bs4.element import Comment
from src.utils import UniformResponse
from collections import Counter
from typing import List, Optional
from lxml import html
import tldextract
from urllib.parse import urlparse
from urllib import robotparser
import requests

from src.models import (
    CrawlerConfig,
    LinkType,
    Link,
    MetaTags,
)

invalid_file_extensions = [".pdf", ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx", ".csv", ".zip", ".rar", ".tar", ".gz", ".7z", ".mp3", ".mp4", ".avi", ".mkv", ".mov", ".flv", ".wmv", ".wav", ".ogg", ".jpg", ".jpeg", ".png", ".gif", ".svg", ".bmp", ".webp"]

class Crawler:
    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.parser = robotparser.RobotFileParser()
    
    def _get_base_url(self, url: str, lib:str='urllib') -> str:
        if lib == 'urllib':
            parsed_uri = urlparse(url)
            result = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_uri).strip()
            return result
        elif lib == 'tldextract':
            return tldextract.extract(url).registered_domain

    def can_fetch(self, robotstxt:bytes, url:str) -> bool:
        self.parser.parse(robotstxt.decode("utf-8").splitlines())
        return self.parser.can_fetch(self.config.user_agent, url)

    def _get_link_type(self, page_url: str, link: str) -> LinkType:
        base_url = self._get_base_url(page_url)
        
        # Check invalid file extensions
        if any([link.endswith(ext) for ext in invalid_file_extensions]):
            return LinkType.INVALID

        # Check if the link is in the same domain
        if base_url in link:
            return LinkType.INTERNAL

        # Check if the link is in the same subdomain
        if self._get_base_url(link) == base_url:
            return LinkType.INTERNAL

        # Check if the link starts with /
        if link.startswith("/"):
            return LinkType.INTERNAL

        if link.startswith("http"):
            return LinkType.EXTERNAL

        # TODO check other cases like mailto, tel, #
        return LinkType.INVALID

    def get_links(self, response: UniformResponse) -> List[Link]:
        result = []
        try:
            content = response.body
            tree = html.fromstring(content)
            tags = tree.xpath("//a")
            links_and_anchor_texts = [(tag.get("href"), tag.text) for tag in tags]
        except Exception as e:
            print("There was an error parsing the html:", e)
            return result
        
        # check if link is external or internal
        for link, anchor in links_and_anchor_texts:
            link = str(link)
            # TODO sanitize more here
            link_type = self._get_link_type(response.url, link)
            base_url = self._get_base_url(response.url)
            result.append(Link(type=link_type, base_url=base_url, href=link, anchor_text=anchor))

        return result

    def get_meta_tags(self, response: UniformResponse) -> MetaTags:
        try:
            content = response.body
            # Extract metadata
            tree = html.fromstring(content)
            title = tree.findtext(".//title")
            title = title.strip() if title else None
            description = tree.xpath('//meta[@name="description"]/@content')
            description = description[0].strip() if description else None
            keywords = tree.xpath('//meta[@name="keywords"]/@content')
            keywords = keywords[0].split(",") if keywords else None
            keywords = [keyword.strip() for keyword in keywords] if keywords else None
            keywords = ','.join(keywords) if keywords else None
        except Exception as e:
            print("There was an error getting meta tags:", e)
            # TODO log critical error
            return MetaTags(title=None, description=None, keywords=None)

        return MetaTags(
            title=title,
            description=description,
            keywords=keywords,
        )

    def _preprocess_document(self, content: str) -> str:
        """Parse the document and return only the text content.
        - Remove scripts, styles, etc.
        - Remove comments
        - Remove special characters & punctuation
        - Remove stopwords
        - Truncate long documents
        - Remove empty lines & unnecessary whitespaces
        """
        # Remove scripts, styles, comments, etc. using BeautifulSoup
        soup = BeautifulSoup(content, 'html.parser')
        for script in soup(["script", "style"]):
            script.extract()
        comments = soup.find_all(text=lambda text: isinstance(text, Comment))
        for comment in comments:
            comment.extract()

        # Get text content
        text_content = soup.get_text(separator=' ', strip=True)

        # Remove special characters & punctuation
        text_content = re.sub(r'[^\w\s]', ' ', text_content)

        # Remove extra whitespaces
        text_content = re.sub(r'\s+', ' ', text_content).strip()

        # Truncate long documents
        max_length = 100000
        if len(text_content) > max_length:
            text_content = text_content[:max_length]

        return text_content

    def get_favicon(self, response: UniformResponse) -> Optional[bytes]:
        try:
            base_url = self._get_base_url(response.url)
            with requests.get(base_url + "/favicon.ico") as r:
                if r.status_code == 200:
                    return r.content
            
            soup = BeautifulSoup(response.body, 'html.parser')
            link = soup.find("link", rel="shortcut icon")
            if link:
                with requests.get(base_url + link["href"]) as r:
                    if r.status_code == 200:
                        return r.content
            
            link = soup.find("link", rel="icon")
            if link:
                with requests.get(base_url + link["href"]) as r:
                    if r.status_code == 200:
                        return r.content
            
        except Exception as e:
            print("Could not get favicon with the following url:", base_url)
        return None
        
    def get_robots_txt(self, response: UniformResponse|str) -> Optional[bytes]:
        if isinstance(response, str):
            base_url = self._get_base_url(response)
        else:
            base_url = self._get_base_url(response.url)
        try:
            with requests.get(base_url + "/robots.txt") as r:
                if r.status_code == 200 and r.headers.get("Content-Type") == "text/plain":
                    return r.content
        except Exception as e:
            print("Could not get robots.txt with the following url:", base_url)
        return None

    def get_sitemap(self, response: UniformResponse) -> Optional[bytes]:
        base_url = self._get_base_url(response.url)
        try:
            with requests.get(base_url + "/sitemap.xml") as r:
                if r.status_code == 200 and r.headers.get("Content-Type") == "application/xml":
                    return r.content
        except Exception as e:
            print("Could not get sitemap with the following url:", base_url)
        return None

    def get_document_frequency(self, content: str) -> Optional[Counter]:
        if not content:
            return None
        try:
            text_content = self._preprocess_document(content)
        except Exception as e:
            print("There was an error parsing the html:", e)
            return None

        document_frequency = Counter()
        unique_words = text_content.split()
        # TODO do stemming and lemmatization
        document_frequency.update(unique_words)

        if not document_frequency:
            return None
        return document_frequency

    # def send_request(self, url: str) -> aiohttp.ClientResponse:
    #     # TODO send initial request with HEAD method to get headers
    #     # and determine whether to send a GET request or not

    #     # check if protocol allowed
    #     if not url.startswith(tuple(self.config.allowed_protocols)):
    #         raise Exception("Protocol not allowed")

    #     with aiohttp.ClientSession() as session:
    #         with session.get(
    #             url,
    #             headers={"User-Agent": self.config.user_agent},
    #             timeout=self.config.req_timeout,
    #             allow_redirects=True,  # TODO decide whether to allow redirects
    #         ) as response:
    #             return response
