import json
import string
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from urllib3 import Retry

from src.utils.typing import AfluxResponse
from src.utils.constants import (
    HTTP_PROTOCOLS,
    HTTP_STATUS_FORCELIST,
    AFLOW_API,
    AFLOW_SERVER,
    AFLOW_DEFAULT_PAGING,
    AFLOW_KEYWORDS,
    AFLOW_OPERATORS,
)


class AflowAPI:
    SERVER = AFLOW_SERVER
    API = AFLOW_API
    PROTOCOLS = HTTP_PROTOCOLS
    STATUS_FORCELIST = HTTP_STATUS_FORCELIST
    API_KEYWORDS = AFLOW_KEYWORDS
    API_OPERATORS = AFLOW_OPERATORS
    DEFAULT_PAGING = AFLOW_DEFAULT_PAGING

    def __init__(
        self,
        max_retries: Optional[int] = None,
    ) -> None:
        self.max_retries = max_retries
        self.session = self._create_session()
        
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.session.close()

    def _create_session(self):
        session = requests.Session()

        if self.max_retries is not None:
            retry = Retry(
                total=self.max_retries,
                read=self.max_retries,
                connect=self.max_retries,
                respect_retry_after_header=True,
                status_forcelist=self.STATUS_FORCELIST,
            )

            adapter = HTTPAdapter(max_retries=retry)
            for protocol in self.PROTOCOLS:
                session.mount(protocol, adapter)

        return session

    def _make_request(self, url: str) -> requests.Response:
        response = self.session.get(url)
        try:
            response.raise_for_status()
        except HTTPError as e:
            raise RuntimeError(f"Failed to download AFLUX data.\n\t{e}")

        return response
    
    #TODO: Check the query values
    def _is_query_valid(self, query: str) -> bool:
        check_spaces =  any(c.isspace() for c in query)
        
        query_operators = [c for c in query if c in string.punctuation]
        check_operators = all(c in self.API_OPERATORS for c in query_operators)
        
        query_keywords = ''.join([c for c in query if c.isalpha()])
        for key in self.API_KEYWORDS:
            if key in query_keywords:
                query_keywords = query_keywords.replace(key, '')
        check_keywords = len(query_keywords) == 0
        
        return check_spaces and check_operators and check_keywords
    
    @property
    def base_url(self) -> str:
        return self.SERVER + self.API

    def request(
        self,
        matchbook: str,
        paging: Optional[int] = None,
        chunk_size: Optional[int] = None,
        no_directives: bool = False,
    ) -> AfluxResponse:
        """
        Sends a request to AFLUX API and retrieves the response.

        Args:
            matchbook (str): The matchbook to query. See `https://aflow.org/documentation/` for more information.
            paging (Optional[int]): The page number for the request. By default, the query will be done on all pages at once.
            chunk_size (Optional[int]): The number of entries per page. This number must be tuned if HttpError 500 happens.

        Returns:
            AfluxResponse: The response from AFLUX API in a JSON-like object.
        """
        if chunk_size is not None and chunk_size < 1:
            raise ValueError("chunk_size must be greater than 0")

        if paging is not None and paging < 0:
            raise ValueError("paging must be greater than or equal to 0")
        
        if not self._is_query_valid(matchbook):
            raise ValueError("Invalid query: contains invalid characters or keywords")

        paging = paging or self.DEFAULT_PAGING
        if chunk_size is not None:
            paging_str = f"$paging({paging},{chunk_size})"
        else:
            paging_str = f"$paging({paging})"

        request_url = self.base_url + matchbook
        if not no_directives:
            request_url = request_url + paging_str + "format(json)"

        response = self._make_request(request_url)

        try:
            json_response = response.json()
        except json.JSONDecodeError:
            raise RuntimeError(f"Failed to decode AFLUX response as JSON")

        return json_response

    def help(self, keyword: Optional[str] = None) -> None:
        """
        Display help information for the AFLOW API.

        Args:
            keyword (str, optional): The specific keyword to get help for. None will display general help. Defaults to None.
        """
        # General help (https://aflow.org/API/aflux/?)
        if keyword is None:
            help_data = self.aflux_request("", no_directives=True)
            help_str = "\n".join(help_data)

        # Help regarding a specific keyword (https://aflow.org/API/aflux/?help(keyword))
        else:
            if not self._is_query_valid(keyword):
                raise ValueError("Invalid query: contains invalid keywords")
            
            try:
                help_data = self.aflux_request(f"help({keyword})", no_directives=True)
            except RuntimeError:
                print(f"No help information found for keyword: {keyword}")
                return

            entry = help_data[keyword]
            help_str = f"{keyword}:\n"
            help_str += f"  description: {entry['description']}\n"
            help_str += f"  units: {entry['units']}\n"
            help_str += f"  status: {entry['status']}\n"

            comment = "\n    ".join(entry["__comment__"]).strip()
            if comment:
                help_str += f"  comment:\n    {comment}"
                
        print(help_str)

    def get_contcar(self, entry: dict[str, str]) -> str:
        if "aurl" not in entry.keys():
            raise ValueError("Invalid entry: missing 'aurl' key.")

        aurl = entry["aurl"].replace(":", "/")
        request_url = f"http://{aurl}/CONTCAR.relax"

        response = self._make_request(request_url)

        # Fix POSTCAR if in VASP4 format
        poscar_lines = response.text.split("\n")

        # Add species names if missing
        if poscar_lines[5].strip()[0].isnumeric():
            poscar_lines.insert(5, " ".join(entry["species"]))

        poscar = "\n".join(poscar_lines)
        return poscar

    def get_property(self, entry: dict[str, str], property: str) -> list[str]:
        if "aurl" not in entry.keys():
            raise ValueError("Invalid entry: missing 'aurl' key.")

        aurl = entry["aurl"].replace(":", "/")
        request_url = f"http://{aurl}/?{property}"

        response = self._make_request(request_url)

        property_value = response.text.strip().split(";")
        for value in property_value:
            value = value.strip().split(",")

        return property_value
