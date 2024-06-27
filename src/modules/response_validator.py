from http import HTTPStatus
from typing import List, Tuple, Union
from lxml import html

from src.models import FailEnum
from src.models import UniformResponse

accepted_status_codes = [
    HTTPStatus.OK,
]

class ResponseValidator:
    # depends on FailEnum, lxml, UniformResponse

    def __init__(self, exclude: Tuple[callable] = None):
        self.exclude = exclude or tuple()
    
    def _check_content_exists(
        self, response: UniformResponse
    ) -> Union[None, FailEnum]:
        if response.body:
            return None
        return FailEnum.NO_CONTENT

    def _check_status_code(
        self, response: UniformResponse
    ) -> Union[None, FailEnum]:
        if response.status_code not in accepted_status_codes:
            return FailEnum.INVALID_STATUS_CODE
        return None


    def _check_content_language(
        self, response: UniformResponse
    ) -> Union[None, FailEnum]:
        # check response headers
        if response.headers.get("Content-Language") in ["tr", "tr-TR", "tr_TR"]:
            return None

        content = response.body
        
        if not content:
            return FailEnum.NO_CONTENT

        tree = html.fromstring(content)

        # check http-equiv meta tag
        if tree.xpath('//meta[@http-equiv="Content-Language" and @content="tr"]'):
            return None

        # check og:locale meta tag
        if tree.xpath('//meta[@property="og:locale" and @content="tr_TR"]'):
            return None

        # check html lang attribute
        if tree.xpath("//html/@lang") in [["tr"], ["tr-TR"], ["tr_TR"]]:
            return None

        return FailEnum.NOT_TURKISH

    def _check_content_type(
        self, response: UniformResponse
    ) -> Union[None, FailEnum]:
        # if response.headers.get("Content-Type", '').lower() in [
        #     "text/html",
        #     "text/html; charset=utf-8",
        #     "text/html; charset=iso-8859-9"
        # ]:
        if 'text/html' in response.headers.get("Content-Type", ''):
            return None
        return FailEnum.INVALID_CONTENT_TYPE

    def validate(
        self, response: UniformResponse
    ) -> Union[None, List[FailEnum]]:
        fails = []
        for func in dir(self):
            if func.startswith("__") or func == "validate":
                continue

            check_func = getattr(self, func)
            if callable(check_func):  # and check_func not in self.exclude:
                fail = check_func(response)
                if fail is not None:
                    fails.append(fail)

        return fails or None
