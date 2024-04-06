
import platform
import subprocess
from typing import Optional
import aiohttp
from pydantic import BaseModel
import requests
import ipaddress
import pickle

class UniformResponse(BaseModel):
    """
    A single source of truth for all kinds of responses from different libraries
    """
    url: str
    status_code: int
    headers: dict
    body: Optional[str]
    content_bytes: Optional[bytes]

class ResponseConverter:
    @staticmethod
    async def from_aiohttp(response: aiohttp.ClientResponse) -> UniformResponse:
        if isinstance(response, aiohttp.client._RequestContextManager):
            raise ValueError("""
            aiohttp.ClientSession.get() is an async context manager and not a response object.
            Please use `async with session.get(url) as response:` and pass the response object to this method.
            """)
        try:
            body_bytes = await response.read()
            body = body_bytes.decode('utf-8')
        except UnicodeDecodeError:
            body = body_bytes.decode('iso-8859-9')  # Turkish encoding

        return UniformResponse(
            url=str(response.url),
            body=body,
            headers=response.headers,
            status_code=response.status,
            content_bytes=body_bytes
        )

    @staticmethod
    def from_requests(response: requests.Response) -> UniformResponse:
        return UniformResponse(
            url=response.url,
            body=response.text,
            headers=response.headers,
            status_code=response.status_code,
            body_bytes=response.content
        )

def ping(host):
    """
    Returns True if host (str) responds to a ping request.
    Remember that a host may not respond to a ping (ICMP) request even if the host name is valid.
    """

    # Option for the number of packets as a function of
    param = '-n' if platform.system().lower()=='windows' else '-c'

    # Building the command. Ex: "ping -c 1 google.com"
    command = ['ping', param, '1', host]
    return subprocess.call(command, stdout=subprocess.DEVNULL) == 0


def get_reserved_ips() -> set[ipaddress.IPv4Address]:
    _reserved_blocks = [
        (ipaddress.IPv4Network('0.0.0.0/8'),),
        (ipaddress.IPv4Network('10.0.0.0/8'),),
        # (ipaddress.IPv4Network('100.64.0.0/10'),),
        # (ipaddress.IPv4Network('127.0.0.0/8'),),
        # (ipaddress.IPv4Network('169.254.0.0/16'),),
        # (ipaddress.IPv4Network('172.16.0.0/12'),),
        # (ipaddress.IPv4Network('192.0.0.0/24'),),
        # (ipaddress.IPv4Network('192.0.2.0/24'),),
        # (ipaddress.IPv4Network('192.88.99.0/24'),),
        # (ipaddress.IPv4Network('192.168.0.0/16'),),
        # (ipaddress.IPv4Network('198.18.0.0/15'),),
        # (ipaddress.IPv4Network('198.51.100.0/24'),),
        # (ipaddress.IPv4Network('203.0.113.0/24'),),
        # (ipaddress.IPv4Network('224.0.0.0/4'),),
        # (ipaddress.IPv4Network('233.252.0.0/24'),),
        # (ipaddress.IPv4Network('240.0.0.0/4'),),
        # (ipaddress.IPv4Network('255.255.255.255/32'),)
    ]
    reserved_ips = set()
    try:
        print("Trying to load reserved IPs from file...")
        with open("data/reserved_ips.pkl", "rb") as f:
            reserved_ips = pickle.load(f)
            print("Loaded reserved IPs from file")
            return reserved_ips
    except FileNotFoundError:
        print("No reserved IP file found, generating reserved IPs...")
        for block in _reserved_blocks:
            print("Processing block:", block)
            if len(block) != 1 or not isinstance(block[0], ipaddress.IPv4Network):
                continue
            reserved_ips.update(set(block[0]))
        with open("data/reserved_ips.pkl", "wb") as f:
            pickle.dump(reserved_ips, f)
        return reserved_ips