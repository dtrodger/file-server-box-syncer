"""
Box Python SDK boxsdk.Client subclass
"""

from __future__ import annotations
from typing import Dict, AnyStr, Optional, List, Tuple
import logging
import json

import boxsdk
import aioredis
import boxsdk.exception as boxsdk_exception

import file_server_box_sync.files.config_file as sentinel_config_file
import file_server_box_sync.http_client as sentinel_http_client
import file_server_box_sync.redis_client as sentinel_redis_client


log = logging.getLogger(__name__)


class SentinelBoxClient(boxsdk.Client):
    def __init__(
        self,
        box_jwt_auth: dict,
        oauth: boxsdk.JWTAuth = None,
        rate_limited: bool = True,
        rate_limit: int = 15,
        rate_period: int = 1,
        **kwargs,
    ) -> SentinelBoxClient:

        if not oauth:
            oauth = self.configure_standard_box_auth(box_jwt_auth)

        super().__init__(oauth, **kwargs)
        self.auth_enterprise_id = self.auth._enterprise_id
        self.auth_client_id = self.auth._client_id
        self.rate_limiter = (
            sentinel_http_client.RateLimiter(rate_limit, rate_period)
            if rate_limited
            else None
        )

    def __repr__(self) -> AnyStr:
        return f"<SentinelBoxClient-EID-{self.auth_enterprise_id}-ClientID-{self.auth_client_id}>"

    @staticmethod
    def configure_standard_box_auth(
        box_jwt_auth: dict,
    ) -> boxsdk.JWTAuth:
        oauth = boxsdk.JWTAuth.from_settings_dictionary(
            box_jwt_auth
        )
        oauth.authenticate_instance()

        return oauth

    def make_request(
        self, method: AnyStr, url: AnyStr, **kwargs
    ) -> boxsdk.network.default_network.DefaultNetworkResponse:
        """
        Base class override to rate limit requests
        """
        if self.rate_limiter:
            with self.rate_limiter:
                resp = super().make_request(method, url, **kwargs)
        else:
            resp = super().make_request(method, url, **kwargs)

        return resp
