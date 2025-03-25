from abc import ABC, abstractmethod
import hashlib
import hmac
import logging
import os
from sys import stdout
import time
from typing import Any, Dict, List, Optional, Tuple

import requests


logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [%(filename)s:%(lineno)d]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=stdout,
)
logger = logging.getLogger(__name__)

class BaseSource(ABC):
    """Base class for API sources."""

    def __init__(self, connection_string: str, interval: str = None) -> None:
        credentials = self._parse_connection_string(connection_string)
        self._api_key = credentials["API_KEY"]
        self._secret_key = credentials["SECRET_KEY"]
        self.interval = interval
        self._session: Optional[requests.Session] = None
        self._headers: Dict[str, str] = {}

    def _parse_connection_string(self, connection_string: str) -> Dict[str, str]:
        """Parse connection string into credentials dictionary."""
        return dict(kv.split("=") for kv in connection_string.split(" "))

    def connect(self) -> None:
        """Connect to the API."""
        self._session = requests.Session()
        self._setup_headers()
        self._session.headers.update(self._headers)
        self.ping()

    @abstractmethod
    def _setup_headers(self) -> None:
        """Set up headers for API requests."""
        pass

    @abstractmethod
    def ping(self) -> None:
        """Test connection to the API."""
        pass

    def _make_request(
        self, 
        method: str, 
        endpoint: str, 
        params: Optional[Dict[str, Any]] = None, 
        data: Optional[Dict[str, Any]] = None,
        sign: bool = False
    ) -> Optional[Dict[str, Any]]:
        """Make a request to the API."""
        if self._session is None:
            logger.error("Session not initialized. Call connect() first.")
            return None

        url = self._build_url(endpoint)
        
        if sign:
            params = self._sign_request(params or {})
            
        try:
            if method.upper() == "GET":
                response = self._session.get(url, params=params)
            elif method.upper() == "POST":
                response = self._session.post(url, params=params, json=data)
            else:
                logger.error(f"Unsupported HTTP method: {method}")
                return None
                
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"Request failed with status code {response.status_code}: {response.text}")
                return None
        except Exception as e:
            logger.error(f"Error making request: {e}")
            return None

    @abstractmethod
    def _build_url(self, endpoint: str) -> str:
        """Build the URL for the API request."""
        pass

    @abstractmethod
    def _sign_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Sign the request if required by the API."""
        pass