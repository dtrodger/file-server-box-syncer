"""
Sentinel type
"""

from __future__ import annotations
from abc import ABC
import datetime
from typing import Dict
import uuid
import json

import aioredis

from file_server_box_sync import redis_client


class _SentinelABC(ABC):

    __slots__ = ["id", "self_redis_key"]

    def __init__(self) -> _SentinelABC:
        self.id = uuid.uuid4()
        self.self_redis_key = f"{type(self).__name__}:{str(self.id)}"

    def __repr__(self):
        return f"<{type(self).__name__}-{str(self.id)}>"

    def __dict__(self) -> Dict:
        return {slot: self.serialize_slot_value(slot) for slot in self.__slots__}

    def __iter__(self):
        yield from self.__dict__().items()

    def serialize_slot_value(self, slot):
        serialized_slot_value = None
        slot_value = getattr(self, slot)

        if slot in [
            "document_nodes",
            "xml_element_tree",
            "csv_writer",
            "report_sheet",
            "xlsx_workbook",
            "box_upload_user",
        ]:
            pass

        elif isinstance(slot_value, uuid.UUID):
            serialized_slot_value = str(slot_value)

        elif isinstance(slot_value, _SentinelABC):
            serialized_slot_value = dict(slot)

        elif isinstance(slot_value, datetime.datetime):
            serialized_slot_value = slot_value.isoformat()

        elif isinstance(slot_value, datetime.date):
            serialized_slot_value = datetime.datetime(
                slot_value.year, slot_value.month, slot_value.day, 0, 0, 0, 0
            )

        elif isinstance(slot_value, bytes):
            serialized_slot_value = str(slot_value, "utf8")

        elif isinstance(slot_value, dict):
            serialized_slot_value = dict(slot_value)

        elif isinstance(slot_value, list):
            list_items = list()
            for list_item in slot_value:
                list_items.append(self.serialize_slot_value(list_item))

            serialized_slot_value = list_items
        else:
            serialized_slot_value = str(slot_value)

        return serialized_slot_value

    async def set_self_to_redis(self, redis_client: aioredis.Redis = None) -> bool:
        self_redis_record_value = json.dumps(dict(self), default=str)

        return await redis_client.set(
            redis_client, self.self_redis_key, self_redis_record_value
        )

    async def get_self_from_redis(self, redis_client: aioredis.Redis = None) -> Dict:
        redis_record_value = await redis_client.get(
            redis_client, self.self_redis_key
        )

        loaded_redis_record = json.loads(redis_record_value.decode("utf-8"))

        return loaded_redis_record
