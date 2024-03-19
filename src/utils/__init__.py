
from typing import Optional
import aiohttp
from pydantic import BaseModel
import requests

class UniformResponse(BaseModel):
    """
    A single source of truth for all kinds of responses from different libraries
    """
    url: str
    status_code: int
    headers: dict
    body: Optional[str]

class ResponseConverter:
    @staticmethod
    async def from_aiohttp(response: aiohttp.ClientResponse) -> UniformResponse:
        try:
            body_bytes = await response.read()
            body = body_bytes.decode('utf-8')
        except UnicodeDecodeError:
            body = body_bytes.decode('iso-8859-9')  # Turkish encoding

        return UniformResponse(
            url=str(response.url),
            body=body,
            headers=response.headers,
            status_code=response.status
        )

    @staticmethod
    def from_requests(response: requests.Response) -> UniformResponse:
        return UniformResponse(
            url=response.url,
            body=response.text,
            headers=response.headers,
            status_code=response.status_code
        )