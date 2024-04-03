import re
from src.utils import UniformResponse
from collections import Counter
from typing import List, Optional
from lxml import html
import tldextract

from utils.config import default_config
from src.models import (
    CrawlerConfig,
    LinkType,
    Link,
    MetaTags,
)

class Crawler:
    # depends on LinkType, Link, CrawlerConfig, MetaTags
    # lxml, aiohttp, tldextract, collections.Counter

    def __init__(self, config: CrawlerConfig):
        self.config = config

    def _get_link_type(self, page_url: str, link: str) -> LinkType:
        base_url = tldextract.extract(page_url).registered_domain

        # Check if the link is in the same domain
        if base_url in link:
            return LinkType.INTERNAL

        # Check if the link is in the same subdomain
        if tldextract.extract(link).registered_domain == base_url:
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
            links = tree.xpath("//a/@href")
        except Exception as e:
            print("There was an error parsing the html:", e)
            return result

        # TODO: remove links that are not html

        # check if link is external or internal
        for link in links:
            link = str(link)
            # TODO sanitize more here
            link_type = self._get_link_type(response.url, link)
            base_url = tldextract.extract(response.url).registered_domain
            result.append(Link(type=link_type, base_url=base_url, href=link))

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
        
        tree = html.fromstring(content)
        text_content = tree.xpath("//body//text()")
        processed_text = " ".join(text_content)
        
        # Remove scripts, styles, etc.
        scripts = tree.xpath("//script")
        for script in scripts:
            script.getparent().remove(script)
        
        styles = tree.xpath("//style")
        for style in styles:
            style.getparent().remove(style)
        
        # Remove comments
        comments = tree.xpath("//comment()")
        for comment in comments:
            comment.getparent().remove(comment)
        
        # Remove special characters & punctuation
        processed_text = re.sub(r"[^\w\s]", "", processed_text)
        
        # Remove stopwords
        stopwords = [
            "=",
            "?>",
            "(",
            ")",
            "{",
            "}",
            "[",
            "]",
            "!",
            "?",
            ".",
            ",",
            ":",
            ";",
            '"',
            "'",
            "’",
            "‘",
            "“",
            "”",
            "-",
            "_",
            "*",
            "#",
            "@",
            "$",
            "ve",
            "veya",
            "ile",
            "/",
            "else",
        ]
        stopwords = set(stopwords) 
        processed_text = " ".join(word for word in processed_text.split() if word.lower() not in stopwords)
        
        # Remove empty lines & unnecessary whitespaces
        processed_text = re.sub(r"\s+", " ", processed_text)
        processed_text = processed_text.strip()
        
        # Truncate long documents  # TODO: get from config.json
        max_length = 10000
        if len(processed_text) > max_length:
            processed_text = processed_text[:max_length]
        
        return processed_text

    def get_document_frequency(self, response: UniformResponse) -> Optional[Counter]:
        try:
            text_content = self._preprocess_document(response.body)
        except Exception as e:
            print("There was an error parsing the html:", e)
            return None

        # TODO improve exclusion logic

        document_frequency = Counter()
        for text in text_content:
            words = text.split()
            unique_words = set(words)
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
