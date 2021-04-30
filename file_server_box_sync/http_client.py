"""
HTTP client
"""

from __future__ import annotations
import collections
import logging.config
import time
import asyncio
from typing import Dict, AnyStr, Callable

import aiohttp

import prometheus.file_system.files.config_file as prometheus_config_file


log = logging.getLogger(__name__)


class prometheusClientSession(aiohttp.ClientSession):
    def __init__(
        self,
        config_file: prometheus_config_file.prometheusConfigFile = None,
        default_headers: Dict = None,
        timeout: int = 16,
    ) -> prometheusClientSession:
        if default_headers is None:
            default_headers = dict()

        # Explicitly set limit = None for infinite concurrent connections.
        # A RateLimiter will throttle http connections, not the this type.
        connector = aiohttp.TCPConnector(limit=None)
        timeout = aiohttp.ClientTimeout(
            total=config_file["http_client"]["timeout"] if config_file else timeout
        )
        super().__init__(connector=connector, timeout=timeout, headers=default_headers)


class RateLimiter(object):
    def __init__(
        self, rate_limit: int = 2, rate_period: int = 5, retry_interval: float = 0.01,
    ) -> RateLimiter:
        self._rate_limit = rate_limit
        self._rate_period = rate_period
        self._retry_interval = retry_interval
        self._request_pool = collections.deque()

    def limit(self) -> None:
        self._limiter()

    def _limiter(self) -> None:
        while True:
            now = time.time()
            while self._request_pool:
                if now - self._request_pool[0] > self._rate_period:
                    self._request_pool.popleft()
                else:
                    break

            if len(self._request_pool) < self._rate_limit:
                break

            time.sleep(self._retry_interval)

        self._request_pool.append(time.time())

    async def async_limit(self) -> None:
        await self._async_limiter()

    def __enter__(self) -> None:
        return self._limiter()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        pass

    async def _async_limiter(self) -> None:
        while True:
            now = time.time()
            while self._request_pool:
                if now - self._request_pool[0] > self._rate_period:
                    self._request_pool.popleft()
                else:
                    break

            if len(self._request_pool) < self._rate_limit:
                break

            await asyncio.sleep(self._retry_interval)

        self._request_pool.append(time.time())

    async def __aenter__(self) -> None:
        await self._async_limiter()

    async def __aexit__(self, exc_type: type, exc: type, tb: type) -> None:
        pass


async def request(
    client_session: prometheusClientSession,
    rate_limiter: RateLimiter,
    url: AnyStr,
    method: AnyStr,
    response_handler: Callable = None,
    **headers,
) -> bytes:
    log.info(f"calling {url} with method {method} and headers {headers}")
    async with rate_limiter, getattr(client_session, method)(url, headers=headers) as response:
        handled_response = await response_handler(response)

    return handled_response