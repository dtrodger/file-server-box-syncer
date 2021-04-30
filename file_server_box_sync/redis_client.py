"""
Redis client
"""

from __future__ import annotations
import logging
from typing import AnyStr, Optional, Any, List
import json

import aioredis

import file_server_box_sync.files.config_file as sentinel_config_file


log = logging.getLogger(__name__)


async def configure_redis_client(
    host: str, port: str, db: str, password: str,
) -> aioredis.Redis:
    return await aioredis.create_redis_pool(
        f'redis://{host}:{port}',
        db=db,
        password=password,
    )


async def close_connection(redis_client: aioredis.Redis):
    if redis_client is not None:
        redis_client.close()
        await redis_client.wait_closed()


async def delete(redis_client: aioredis.Redis, key: AnyStr) -> bool:
    record_delete = False
    try:
        record_delete = await redis_client.delete(key)
    except Exception as e:
        log.error(f"set failed to delete key {key} with {str(e)}")

    return record_delete


async def get(redis_client: aioredis.Redis, key: AnyStr) -> Optional[AnyStr]:
    record_get = None
    try:
        record_get = await redis_client.get(key)
        if record_get:
            record_get = record_get.decode("utf-8")
    except Exception as e:
        log.error(f"set failed {str(e)} key {key}")

    return record_get


async def scan_keys(redis_client: aioredis.Redis, pattern: AnyStr) -> List:
    matched_keys = await redis_client.scan(match=pattern)
    scanned_keys = [key.decode("utf-8") for key in matched_keys[1]]
    log.debug(f"pattern {pattern} scanned keys {scanned_keys}")

    return scanned_keys


async def set(redis_client: aioredis.Redis, key: AnyStr, value: Any):
    record_set = False
    try:
        record_set = await redis_client.set(key, json.dumps(value, default=AnyStr))
        log.debug(f"set key {key} value {value}")
    except Exception as e:
        log.error(f"failed to set key {key} to value {value} with {str(e)}")

    return record_set


async def empty_queue(redis_client: aioredis.Redis, q: AnyStr,) -> list:
    values = None
    try:
        values = [
            json.loads(value.decode()) for value in await redis_client.lrange(q, 0, -1)
        ]
        await delete(redis_client, q)
    except Exception as e:
        log.info(f"failed empty q {q} with {str(e)}")

    return values


async def dequeue(redis_client: aioredis.Redis, q: AnyStr) -> Any:
    loaded_value = None
    try:
        value = await redis_client.brpop(q)
        loaded_value = json.dumps(value[1].decode(), default=AnyStr)
        log.debug(f"dequeue key {q} value {loaded_value}")
    except Exception as e:
        log.error(f"dequeue from q {q} failed with {str(e)}")

    return loaded_value


async def enqueue(redis_client: aioredis.Redis, q: AnyStr, record: Any) -> bool:
    enqueued = False
    try:
        await redis_client.lpush(q, json.dumps(record, default=AnyStr))
        enqueued = True
        log.debug(f"enqueue key {q} and value {record}")
    except Exception as e:
        log.error(f"failed enqueue to q {q} with {record}")

    return enqueued


async def set_box_user_email_id_association(
    redis_client: aioredis.Redis,
    box_user_email: AnyStr,
    box_user_id: AnyStr
) -> bool:
    redis_key = f"box-user-email-id:{box_user_email}"
    return await set(redis_client, redis_key, box_user_id)
