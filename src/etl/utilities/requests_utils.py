from __future__ import annotations

import random
import time
from typing import Any, Dict, Optional

import requests


class RequestsUtils:
    def __init__(
        self,
        default_headers: Optional[Dict[str, str]] = None,
        session: Optional[requests.Session] = None,
    ):
        self.default_headers = default_headers or {}
        self.session = session or requests.Session()

    def _merge_headers(self, headers: Optional[Dict[str, str]]) -> Dict[str, str]:
        merged = dict(self.default_headers)
        if headers:
            merged.update(headers)
        return merged

    def _sleep_seconds(self, base: float, attempt: int) -> float:
        return base * (2 ** attempt) * (0.5 + random.random())

    def _should_retry_status(self, status_code: int) -> bool:
        # Only certain codes are retried
        return status_code in {429, 500, 502, 503, 504}

    def get(
        self,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        retry: int = 3,
        retry_sleep: float = 1.0,
        timeout_s: float = 30.0,
        raise_for_status: bool = True,
    ) -> requests.Response:
        resp: Optional[requests.Response] = None
        last_exc: Optional[Exception] = None

        for attempt in range(max(1, retry)):
            try:
                resp = self.session.get(
                    url,
                    params=params,
                    headers=self._merge_headers(headers),
                    timeout=timeout_s,
                )

                if self._should_retry_status(resp.status_code):
                    ra = resp.headers.get("Retry-After")
                    if ra and ra.isdigit():
                        time.sleep(int(ra))
                    else:
                        time.sleep(self._sleep_seconds(retry_sleep, attempt))
                    continue

                if raise_for_status:
                    resp.raise_for_status()
                return resp

            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                last_exc = e
                time.sleep(self._sleep_seconds(retry_sleep, attempt))
                continue

            except requests.exceptions.RequestException as e:
                raise e

        if resp is not None:
            if raise_for_status:
                resp.raise_for_status()
            return resp

        if last_exc is not None:
            raise last_exc

        raise RuntimeError("Request failed")

    def post(
        self,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Any = None,
        data: Any = None,
        headers: Optional[Dict[str, str]] = None,
        retry: int = 3,
        retry_sleep: float = 1.0,
        timeout_s: float = 30.0,
        raise_for_status: bool = True,
        retry_on_status: Optional[set[int]] = None,
    ) -> requests.Response:
        resp: Optional[requests.Response] = None
        last_exc: Optional[Exception] = None
        status_set = retry_on_status if retry_on_status is not None else {429, 500, 502, 503, 504}

        for attempt in range(max(1, retry)):
            try:
                resp = self.session.post(
                    url,
                    params=params,
                    json=json_body,
                    data=data,
                    headers=self._merge_headers(headers),
                    timeout=timeout_s,
                )

                if resp.status_code in status_set:
                    ra = resp.headers.get("Retry-After")
                    if ra and ra.isdigit():
                        time.sleep(int(ra))
                    else:
                        time.sleep(self._sleep_seconds(retry_sleep, attempt))
                    continue

                if raise_for_status:
                    resp.raise_for_status()
                return resp

            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                last_exc = e
                time.sleep(self._sleep_seconds(retry_sleep, attempt))
                continue

            except requests.exceptions.RequestException as e:
                raise e

        if resp is not None:
            if raise_for_status:
                resp.raise_for_status()
            return resp

        if last_exc is not None:
            raise last_exc

        raise RuntimeError("Request failed")

    def put(
        self,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Any = None,
        data: Any = None,
        headers: Optional[Dict[str, str]] = None,
        retry: int = 3,
        retry_sleep: float = 1.0,
        timeout_s: float = 30.0,
        raise_for_status: bool = True,
        retry_on_status: Optional[set[int]] = None,
    ) -> requests.Response:
        resp: Optional[requests.Response] = None
        last_exc: Optional[Exception] = None
        status_set = retry_on_status if retry_on_status is not None else {429, 500, 502, 503, 504}

        for attempt in range(max(1, retry)):
            try:
                resp = self.session.put(
                    url,
                    params=params,
                    json=json_body,
                    data=data,
                    headers=self._merge_headers(headers),
                    timeout=timeout_s,
                )

                if resp.status_code in status_set:
                    ra = resp.headers.get("Retry-After")
                    if ra and ra.isdigit():
                        time.sleep(int(ra))
                    else:
                        time.sleep(self._sleep_seconds(retry_sleep, attempt))
                    continue

                if raise_for_status:
                    resp.raise_for_status()
                return resp

            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                last_exc = e
                time.sleep(self._sleep_seconds(retry_sleep, attempt))
                continue

            except requests.exceptions.RequestException as e:
                raise e

        if resp is not None:
            if raise_for_status:
                resp.raise_for_status()
            return resp

        if last_exc is not None:
            raise last_exc

        raise RuntimeError("Request failed")