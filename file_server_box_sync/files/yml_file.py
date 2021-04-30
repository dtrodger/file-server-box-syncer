"""
YML file
"""

from __future__ import annotations
import logging
from typing import Dict

import yaml
from file_server_box_sync.files import sentinel_file


log = logging.getLogger(__name__)


class YMLFile(sentinel_file.SentinelFile):

    __slots__ = [
        "attempted_upload_to_box_on",
        "box_file",
        "box_upload_folder_id",
        "box_upload_folder_path",
        "box_upload_user",
        "box_upload_user_email",
        "box_upload_user_id",
        "cached_disk_self",
        "current_directory",
        "deleted",
        "deleted_on",
        "dict_self",
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
    ]

    def __init__(
        self,
        cached_disk_self: Dict = None,
        cached_disk_self_on_init: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cached_disk_self = cached_disk_self
        if cached_disk_self_on_init:
            self.cached_disk_self = self.disk_self

    @property
    def disk_self(self):
        loaded_dict = None
        if self.exists:
            with open(self.file_path) as fh:
                loaded_dict = yaml.load(fh, Loader=yaml.FullLoader)

        return loaded_dict

    def set_disk_self_from_cache(self):
        set_self = False
        if self.exists:
            with open(self.file_path, "w") as fh:
                yaml.dump(self.cached_disk_self, fh, default_flow_style=False)
            set_self = True

        return set_self

    def __getitem__(self, key, recache_disk_self: bool = False):
        if recache_disk_self:
            self.cached_disk_self = self.disk_self

        return self.cached_disk_self[key]

    def __setitem__(self, key, value, set_disk_self: bool = True):
        self.dict_self[key] = value
        if set_disk_self:
            self.set_disk_self_from_cache()

        return True
