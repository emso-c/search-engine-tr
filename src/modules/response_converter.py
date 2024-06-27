
from src.models import UniformResponse
import requests
import aiohttp

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