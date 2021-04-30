"""
Base file
"""

from __future__ import annotations
from typing import Optional, Tuple, List, AnyStr, Dict, Union
import os
import datetime
import logging
import shutil

import boxsdk
import aioredis

from file_server_box_sync import (
    box_client,
    redis_client,
    aiofiles
)
from file_server_box_sync import exception as sentinel_exception


log = logging.getLogger(__name__)


class SentinelFile(sentinel_abc._SentinelABC):

    __slots__ = [
        "attempted_upload_to_box_on",
        "box_file",
        "box_file_id",
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
        "min_elapsed_for_box_upload_attempt",
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
        file_path: AnyStr,
        attempted_upload_to_box_on: Optional[datetime.datetime] = None,
        box_file: Optional[boxsdk.object.file.File] = None,
        box_file_id: Optional[AnyStr] = None,
        box_upload_folder_id: Optional[AnyStr] = None,
        box_upload_folder_path: Optional[AnyStr] = None,
        box_upload_user: Optional[boxsdk.object.user.User] = None,
        box_upload_user_email: Optional[AnyStr] = None,
        box_upload_user_id: Optional[AnyStr] = None,
        current_directory: Optional[AnyStr] = None,
        deleted: bool = False,
        deleted_on: bool = False,
        directory_entry_on: Optional[datetime.datetime] = None,
        file_directory_path: Optional[AnyStr] = None,
        file_name: Optional[AnyStr] = None,
        initial_monitor_on: Optional[datetime.datetime] = None,
        input_complete: bool = False,
        min_elapsed_for_box_upload_attempt: int = 1,
        min_elapsed_for_box_upload_fail: int = 3,
        min_elapsed_for_delete: int = 0,
        min_elapsed_for_input_complete: int = 1,
        ready_for_delete: bool = False,
        ready_for_upload: bool = False,
        st_check_on: Optional[datetime.datetime] = None,
        st_creation_time: Optional[int] = None,
        st_last_accessed_time: Optional[int] = None,
        st_last_modified_time: Optional[int] = None,
        st_size: Optional[int] = None,
        st_size_diff_from_cache_on: Optional[datetime.datetime] = None,
        uploaded_to_box: bool = False,
        uploaded_to_box_on: datetime = None,
        **kwargs,
    ) -> SentinelFile:
        super().__init__()
        if not file_directory_path:
            file_directory_path = os.path.dirname(file_path)

        if not file_name:
            file_name = os.path.basename(file_path)

        self.attempted_upload_to_box_on = attempted_upload_to_box_on
        self.box_file = box_file
        self.box_file_id = box_file_id
        self.box_upload_user = box_upload_user
        self.box_upload_user_email = box_upload_user_email
        self.box_upload_folder_id = box_upload_folder_id
        self.box_upload_folder_path = box_upload_folder_path
        self.box_upload_user_id = box_upload_user_id
        self.current_directory = current_directory
        self.file_directory_path = file_directory_path
        self.file_name = file_name
        self.file_path = file_path
        self.input_complete = input_complete
        self.initial_monitor_on = initial_monitor_on or datetime.datetime.utcnow()
        self.min_elapsed_for_box_upload_attempt = min_elapsed_for_box_upload_attempt
        self.min_elapsed_for_box_upload_fail = min_elapsed_for_box_upload_fail
        self.min_elapsed_for_delete = min_elapsed_for_delete
        self.min_elapsed_for_input_complete = min_elapsed_for_input_complete
        self.deleted = deleted
        self.deleted_on = deleted_on
        self.directory_entry_on = directory_entry_on or datetime.datetime.utcnow()
        self.ready_for_delete = ready_for_delete
        self.ready_for_upload = ready_for_upload
        self.st_check_on = st_check_on
        self.st_creation_time = st_creation_time
        self.st_last_accessed_time = st_last_accessed_time
        self.st_last_modified_time = st_last_modified_time
        self.st_size = st_size
        self.st_size_diff_from_cache_on = st_size_diff_from_cache_on
        self.uploaded_to_box = uploaded_to_box
        self.uploaded_to_box_on = uploaded_to_box_on

    def __repr__(self) -> AnyStr:
        return f"<{type(self).__name__}-{self.file_name}>"

    def associate_box_folder_path_to_id(
        self,
        box_client: box_client.SentinelBoxClient,
        as_box_upload_user: Optional[bool] = False,
        box_user: Optional[boxsdk.object.user.User] = None
    ) -> Optional[str]:
        if as_box_upload_user:
            if not self.box_upload_user and self.box_upload_user_id:
                box_user = box_client.user(self.box_upload_user_id).get()
                self.box_upload_user = box_user
            else:
                box_user = self.box_upload_user

        response = box_client.get_folder_by_path(self.box_upload_folder_path, box_user)
        try:
            folder_id = response.json()["entries"][0]["id"]
            self.box_upload_folder_id = folder_id
        except Exception as e:
            log.debug(f"{self} failed to get box folder for user {box_user} with path {self.box_upload_folder_path}")

        return folder_id

    @property
    def cache_box_upload_folder_path_id_key(self) -> Optional[AnyStr]:
        redis_key = None
        if self.box_upload_folder_path is not None and self.box_upload_user_id:
            redis_key = f"box-folder-path-id:box-user-{self.box_upload_user_id}:{self.box_upload_folder_path}"

        return redis_key

    @property
    def cache_box_upload_user_email_id_key(self) -> Optional[AnyStr]:
        redis_key = None
        if self.box_upload_user_email:
            redis_key = f"box-user-email-id:{self.box_upload_user_email}"

        return redis_key

    def check_input_complete(self) -> bool:
        input_complete = False
        if self.input_complete:
            input_complete = True
        elif (
            self.elapsed_since_st_size_diff_from_cache
            < self.min_elapsed_for_input_complete
        ):
            input_complete = True
            self.input_complete = input_complete

        return input_complete

    def check_min_elapsed_for_box_upload(self) -> bool:
        can_upload = False
        if (
            not self.attempted_upload_to_box_on
            or self.elapsed_since_box_upload_attempt
            > self.min_elapsed_for_box_upload_attempt
        ):
            can_upload = True

        return can_upload

    def check_ready_for_delete(self) -> bool:
        ready_for_delete = False
        elapsed_since_directory_entry_on = self.elapsed_since_directory_entry_on
        if (
            not self.deleted
            and elapsed_since_directory_entry_on
            and elapsed_since_directory_entry_on > self.min_elapsed_for_delete
        ):
            ready_for_delete = True

        self.ready_for_delete = ready_for_delete
        log.debug(f"{self} ready for delete {ready_for_delete}")

        return ready_for_delete

    async def create_on_disk(self, override_existing: bool = True) -> bool:
        async def create():
            async with sentinel_aiofiles.open(self.file_path, mode="w") as fh:
                pass

        created = False
        try:
            if os.path.exists(self.file_path):
                if override_existing:
                    await create()
                    created = True
            else:
                await create()
                created = True
        except Exception as e:
            log.error(f"{self} failed create file with {str(e)}")

        if created:
            log.debug(f"created file {self} on disk")

        return created

    async def current_stats(self) -> Tuple[Optional[Dict], datetime.datetime]:
        st_check_on = datetime.datetime.utcnow()
        stats = None
        if self.file_path and os.path.exists(self.file_path):
            try:
                stats = await file_system.aiofiles.os.stat(self.file_path)
                log.debug(f"{self} found stats {stats} on {st_check_on}")
            except Exception as e:
                log.error(f"{self} stats check failed with {str(e)}")

        return stats, st_check_on

    async def current_st_creation_time_diff_from_cache(
        self, stats: Dict = None
    ) -> Dict:
        return await self.current_st_diff_from_cache("creation_time", stats)

    async def current_st_diff_from_cache(
        self, stat: Union[AnyStr, bool], stats: Dict = None
    ) -> Dict:
        def build_st_diff_dict(previous, current):
            return {"previous": previous, "current": current}

        if not stats:
            stats, stats_checked_on = await self.current_stats()

        st_diff_from_cache = {}

        if stats:
            if stat == "creation_time" or stat is True:
                new_st_creation_time = stats[9]
                if self.st_creation_time != new_st_creation_time:
                    st_diff_from_cache["creation_time"] = build_st_diff_dict(
                        self.st_creation_time, new_st_creation_time
                    )
                    self.st_creation_time = new_st_creation_time

            elif stat == "last_accessed_time" or stat is True:
                new_st_last_accessed_time = stats[7]
                if self.st_last_accessed_time != new_st_last_accessed_time:
                    st_diff_from_cache["last_accessed_time"] = build_st_diff_dict(
                        self.st_last_accessed_time, new_st_last_accessed_time
                    )
                    self.st_last_accessed_time = new_st_last_accessed_time

            elif stat == "last_modified_time" or stat is True:
                new_st_last_modified_time = stats[8]
                if self.st_last_modified_time != new_st_last_modified_time:
                    st_diff_from_cache["last_modified_time"] = build_st_diff_dict(
                        self.st_last_modified_time, new_st_last_modified_time
                    )
                    self.st_size = new_st_last_modified_time

            elif stat == "size" or stat is True:
                new_st_size = stats[6]
                if self.st_size != new_st_size:
                    st_diff_from_cache["size"] = build_st_diff_dict(
                        self.st_size, new_st_size
                    )
                    self.st_size = new_st_size

        self.st_size_diff_from_cache_on = datetime.datetime.utcnow()
        log.debug(f"{self} st_{stat}_diff_from_cache {st_diff_from_cache}")

        return st_diff_from_cache

    async def current_st_last_accessed_time(self, stats: Dict = None) -> Dict:
        return await self.current_st_diff_from_cache("last_accessed_time", stats)

    async def current_st_last_modified_time_diff_from_cache(
        self, stats: Dict = None
    ) -> Dict:

        return await self.current_st_diff_from_cache("last_modified_time", stats)

    async def current_st_size_diff_from_cache(self, stats: Dict = None) -> Dict:
        return await self.current_st_diff_from_cache("size", stats)

    async def delete(self) -> bool:
        deleted = False
        try:
            await file_system.aiofiles.os.remove(self.file_path)
            deleted = True
            log.debug(f"{self} deleted {deleted}")
            self.deleted = deleted
            self.deleted_on = datetime.datetime.utcnow()
            self.file_path = None
            self.current_directory = None
        except Exception as e:
            log.error(f"failed {self} delete with error {str(e)}")

        return deleted

    @property
    def elapsed_since_box_upload_attempt(self) -> Optional[float]:
        elapsed_secs = None
        if self.attempted_upload_to_box_on:
            elapsed_secs = (
                datetime.datetime.utcnow() - self.attempted_upload_to_box_on
            ).total_seconds()

        return elapsed_secs

    @property
    def elapsed_since_initial_monitor(self) -> Optional[float]:
        elapsed_secs = None
        if self.initial_monitor_on:
            elapsed_secs = (
                datetime.datetime.utcnow() - self.initial_monitor_on
            ).total_seconds()

        return elapsed_secs

    @property
    def elapsed_since_st_check(self) -> Optional[float]:
        elapsed_secs = None
        if self.st_check_on:
            elapsed_secs = (
                datetime.datetime.utcnow() - self.st_check_on
            ).total_seconds()

        return elapsed_secs

    @property
    def elapsed_since_st_size_diff_from_cache(self) -> Optional[float]:
        elapsed_secs = None
        if self.st_size_diff_from_cache_on:
            elapsed_secs = (
                datetime.datetime.utcnow() - self.st_size_diff_from_cache_on
            ).total_seconds()

        return elapsed_secs

    @property
    def elapsed_since_directory_entry_on(self) -> Optional[float]:
        elapsed_secs = None
        if self.directory_entry_on:
            elapsed_secs = (
                datetime.datetime.utcnow() - self.directory_entry_on
            ).total_seconds()

        return elapsed_secs

    @property
    def exists(self) -> bool:
        return os.path.exists(self.file_path)

    @property
    def file_name_prefix(self):
        return self.file_name.split(".")[0]

    async def get_box_upload_folder_path_id_from_cache(
        self, redis_client: aioredis.Redis
    ) -> AnyStr:

        redis_value = None
        if self.cache_box_upload_folder_path_id_key:
            redis_value = await redis_client.get(
                self.cache_box_upload_folder_path_id_key
            )

        if redis_value:
            log.debug(
                f"got redis box upload folder path key {self.cache_box_upload_folder_path_id_key}"
            )
        else:
            log.debug(
                f"failed to get box upload folder path key {self.cache_box_upload_folder_path_id_key} in redis"
            )

        return redis_value

    async def get_box_user_email_id_from_cache(self, redis_client: aioredis.Redis):
        redis_value = None
        if self.cache_box_upload_user_email_id_key:
            redis_value = await redis_client.get(
                redis_client, self.cache_box_upload_user_email_id_key
            )

        if redis_value:
            log.debug(
                f"got redis box user email association from key {self.cache_box_upload_user_email_id_key}"
            )
        else:
            log.debug(
                f"failed to box user email association from key {self.cache_box_upload_user_email_id_key} in redis"
            )

        return redis_value

    async def is_ready_for_box_upload(self) -> bool:
        stats, st_check_on = await self.current_stats()

        ready_for_upload = False
        if (
            not self.deleted
            and not self.current_st_size_diff_from_cache(stats)
            and self.check_input_complete()
            and self.check_min_elapsed_for_box_upload()
        ):
            ready_for_upload = True
            self.ready_for_upload = ready_for_upload

        log.debug(f"{self} is ready for upload {ready_for_upload}")
        return ready_for_upload

    def move_directories(
        self, new_directory_path: AnyStr, rename_dup=True
    ) -> bool:
        moved = False
        try:
            if os.path.exists(os.path.join(new_directory_path, self.file_name)):
                if rename_dup:
                    new_file_name = f"{int(datetime.datetime.utcnow().timestamp())}-{self.file_name}"

                    renamed = self.rename_self(new_file_name)

                    if not renamed:
                        raise sentinel_exception.SentinelFileError(
                            f"{self} failed to remain {self.file_path} to {new_file_name}"
                        )
                else:
                    raise sentinel_exception.SentinelFileError(f"{self} duplicate name")

            shutil.move(self.file_path, new_directory_path)
            log.debug(f"moved {self} from {self.file_path} to {new_directory_path}")
            self.file_path = new_directory_path
            self.directory_entry_on = datetime.datetime.utcnow()
            self.current_directory = os.path.split(new_directory_path)[-1]
            moved = True
        except Exception as e:
            log.error(
                f"failed move {self} from {self.file_path} to {new_directory_path} with {str(e)}"
            )

        return moved

    async def read_line(self):
        try:
            async with sentinel_aiofiles.open(self.file_path, "r") as fh:
                line = await fh.readline()
                line = line.strip("\n")
            log.debug(f"{self} read line {line}")
        except Exception as e:
            log.error(f"{self} failed to read line with {e}")

        return line

    async def read_lines(self):
        lines = list()
        try:
            async with sentinel_aiofiles.open(self.file_path, "r") as fh:
                async for line in fh:
                    lines.append(line.replace("\r", "").replace("\n", ""))

            log.debug(f"{self} read lines {lines}")
        except Exception as e:
            log.error(f"{self} failed to read lines with {e}")

        return lines

    def rename_self(self, new_file_name: AnyStr) -> bool:
        renamed = False
        new_file_path = os.path.join(
            os.path.dirname(__file__), self.file_directory_path, new_file_name
        )
        try:
            old_file_path = self.file_path
            os.rename(self.file_path, new_file_path)
            self.file_path = new_file_path
            self.file_name = new_file_name
            renamed = True
            log.debug(f"{self} renamed form {old_file_path} to {new_file_path}")

        except Exception as e:
            log.error(f"failed rename {self} to {new_file_path} with {str(e)}")

        return renamed

    async def set_box_upload_user_from_cache(
        self,
        box_client: box_client.SentinelBoxClient,
        redis_client: aioredis.Redis,
    ):
        set_box_upload_user = False
        if self.box_upload_user_email:
            redis_value = await self.get_box_user_email_id_from_cache(redis_client)
            if redis_value:
                try:
                    box_user = box_client.user(redis_value)
                    if not box_user.response_object:
                        box_user = box_client.search_user_by_primary_email(
                            self.box_upload_user_email
                        )
                except Exception as e:
                    box_user = None
                    log.error(
                        f"{box_client} failed to get user {self.box_upload_user_id}"
                    )

                if box_user.response_object:
                    self.set_box_upload_user_info_from_user(box_user)
                    set_box_upload_user = True
                    log.debug(
                        f"set box upload user id from cache {self.cache_box_upload_user_email_id_key}"
                    )
                else:
                    log.info(
                        f"{self} failed to associate {self.box_upload_user_email} to Box user"
                    )
            else:
                set_box_upload_user = await self.set_box_upload_user_to_cache(
                    box_client, redis_client
                )

        return set_box_upload_user

    def set_box_upload_user_info_from_user(self, box_user: boxsdk.object.user.User):
        self.box_upload_user = box_user
        self.box_upload_user_email = box_user.response_object["login"]
        self.box_upload_user_id = box_user.response_object["id"]
        log.debug(f"{self} set box upload user info from {box_user}")

    async def set_box_upload_user_to_cache(
        self,
        box_client: box_client.SentinelBoxClient,
        redis_client: aioredis.Redis,
    ) -> bool:
        set_association = False
        box_user = box_client.search_user_by_primary_email(self.box_upload_user_email)
        if box_user:
            self.set_box_upload_user_info_from_user(box_user)

            set_association = await redis_client.set_box_user_email_id_association(
                redis_client, self.box_upload_user_email, self.box_upload_user_id
            )
            log.debug(
                f"{self} associated {self.box_upload_user_email} to Box user ID {self.box_upload_user_id} and set to redis"
            )
        else:
            log.debug(
                f"{self} failed to associate {self.box_upload_user_email} to Box user ID"
            )

        return set_association

    async def update_stats(
        self,
        stats: Optional[Dict] = None,
        st_check_on: Optional[datetime.datetime] = None,
    ) -> None:
        if not stats:
            stats, st_check_on = await self.current_stats()

        if stats:
            self.st_check_on = st_check_on
            self.st_size = stats[6]
            self.st_last_accessed_time = stats[7]
            self.st_last_modified_time = stats[8]
            self.st_creation_time = stats[9]

    async def upload_to_box(
        self,
        box_client: box_client.SentinelBoxClient,
        box_upload_folder_id: Optional[AnyStr] = None,
        as_box_upload_user: Optional[bool] = False,
        box_user: Optional[boxsdk.object.user.User] = None,
        to_box_upload_folder: Optional[bool] = True,
    ) -> Union[boxsdk.object.file.File, AnyStr]:
        try:
            self.attempted_upload_to_box_on = datetime.datetime.utcnow()
            if not self.st_size:
                await self.update_stats()

            if as_box_upload_user:
                box_user = self.box_upload_user

            if to_box_upload_folder:
                box_upload_folder_id = self.box_upload_folder_id

            if self.st_size < 50000000:
                log.debug(f"{self} attempting simple Box upload")

                if box_user:
                    box_file = (
                        box_client.as_user(box_user)
                        .folder(box_upload_folder_id)
                        .upload(self.file_path)
                    )
                else:
                    box_file = box_client.folder(box_upload_folder_id).upload(
                        self.file_path
                    )
            else:
                log.debug(f"{self} attempting chunked Box upload")

                if box_user:
                    chunked_uploader = (
                        box_client.as_user(box_user)
                        .folder(box_upload_folder_id)
                        .get_chunked_uploader(self.file_path)
                    )
                else:
                    chunked_uploader = box_client.folder(
                        box_upload_folder_id
                    ).get_chunked_uploader(self.file_path)

                box_file = chunked_uploader.start()

            log.debug(
                f"{self} uploaded to Box with {box_client} to folder {box_upload_folder_id} and created a Box file with ID {box_file.id}"
            )
            self.box_file = box_file
            self.box_file_id = box_file.id
            self.uploaded_to_box = True
            self.uploaded_to_box_on = datetime.datetime.now()
            uploaded = box_file
        except Exception as e:
            uploaded = str(e)
            log.error(
                f"{self} failed upload to box with client {box_client} to folder {box_upload_folder_id} failed with {uploaded}"
            )

        return uploaded

    def upload_to_box_failed(self) -> bool:
        upload_to_box_failed = False
        if (
            datetime.datetime.utcnow() - self.initial_monitor_on
        ).seconds > self.min_elapsed_for_box_upload_fail and not self.deleted:
            upload_to_box_failed = True

        return upload_to_box_failed

    async def write_line(self, line: AnyStr):
        wrote = False
        try:
            async with sentinel_aiofiles.open(self.file_path, "a+") as fh:
                await fh.write(line)

            wrote = True
            log.debug(f"{self} wrote line {line}")
        except Exception as e:
            log.error(f"{self} failed to write line with {e}")

        return wrote

    async def write_new_line(self, line: AnyStr):
        return await self.write_line(f"\n{line}")

    async def write_lines(self, lines: List):
        wrote = False
        lines = [f"{line}\n" for line in lines]
        try:
            async with sentinel_aiofiles.open(self.file_path, "a+") as fh:
                await fh.writelines(lines)

            wrote = True
            log.debug(f"{self} wrote lines {lines}")
        except Exception as e:
            log.error(f"{self} failed to write lines with {e}")

        return wrote
