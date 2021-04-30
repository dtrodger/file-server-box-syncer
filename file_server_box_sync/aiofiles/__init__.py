"""Utilities for asyncio-friendly file handling."""
from file_server_box_sync.aiofiles.threadpool import open
from file_server_box_sync.aiofiles import os

__version__ = "0.5.0.dev0"

__all__ = ['open', 'os']
