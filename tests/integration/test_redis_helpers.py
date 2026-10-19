import os

import pytest

from rf.db.redisdb import RedisDB


@pytest.mark.parametrize("decode_responses", [True, False])
def test_url_connection_preserves_pool_limit_and_decoding(namespace, decode_responses):
    db = RedisDB(
        url=os.environ["RF_TEST_REDIS_URL"],
        decode_responses=decode_responses,
        max_connections=3,
    )
    client = db.get_redis_obj()
    try:
        key = namespace + ":connection"
        client.set(key, "payload")
        assert client.get(key) == ("payload" if decode_responses else b"payload")
        assert client.connection_pool.max_connections == 3
    finally:
        client.close()
