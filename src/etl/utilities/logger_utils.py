from __future__ import annotations

import logging
from typing import Optional


class LoggerUtils:
    @staticmethod
    def get_logger(
        name: str,
        *,
        level: int = logging.INFO,
        fmt: str = "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt: str = "%Y-%m-%d %H:%M:%S",
    ) -> logging.Logger:
        logger = logging.getLogger(name)
        logger.setLevel(level)

        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setLevel(level)

            formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
            handler.setFormatter(formatter)

            logger.addHandler(handler)

        logger.propagate = False
        return logger

    @staticmethod
    def info(logger: logging.Logger, message: str, *, extra: Optional[dict] = None) -> None:
        logger.info(message, extra=extra)

    @staticmethod
    def warning(logger: logging.Logger, message: str, *, extra: Optional[dict] = None) -> None:
        logger.warning(message, extra=extra)

    @staticmethod
    def error(logger: logging.Logger, message: str, *, exc: Optional[BaseException] = None, extra: Optional[dict] = None) -> None:
        if exc is not None:
            logger.error(message, exc_info=exc, extra=extra)
        else:
            logger.error(message, extra=extra)

    @staticmethod
    def exception(logger: logging.Logger, message: str, *, extra: Optional[dict] = None) -> None:
        logger.exception(message, extra=extra)