"""
XML file
"""

from __future__ import annotations
import logging
from typing import AnyStr

import lxml.etree as lxml_etree

from file_server_box_sync.files import sentinel_file


log = logging.getLogger(__name__)


class XMLFile(sentinel_file.SentinelFile):

    __slots__ = [
        "attempted_upload_to_box_on",
        "box_file",
        "box_upload_folder_id",
        "box_upload_folder_path",
        "box_upload_user",
        "box_upload_user_email",
        "box_upload_user_id",
        "current_directory",
        "deleted",
        "deleted_on",
        "directory_entry_on",
        "file_directory_path",
        "file_name",
        "file_path",
        "id",
        "initial_monitor_on",
        "input_complete",
        "min_elapsed_for_box_upload_fail",
        "min_elapsed_for_delete",
        "min_elapsed_for_input_complete",
        "ready_for_delete",
        "ready_for_upload",
        "self_redis_key",
        "st_check_on",
        "st_creation_time",
        "st_last_accessed_time",
        "st_last_modified_time",
        "st_size",
        "st_size_diff_from_cache_on",
        "uploaded_to_box",
        "uploaded_to_box_on",
        "parsed_xml",
    ]

    def __init__(self, file_path: AnyStr, **kwargs):
        super().__init__(file_path=file_path, **kwargs)
        self.parsed_xml = lxml_etree.parse(file_path)
