import time

from gsheets_sql.cache import Cache


def test_set_and_get():
    c = Cache(ttl=60)
    c.set("key", "value")
    assert c.get("key") == "value"


def test_miss_returns_none():
    c = Cache(ttl=60)
    assert c.get("missing") is None


def test_expiry():
    c = Cache(ttl=1)
    c.set("key", "value")
    time.sleep(1.1)
    assert c.get("key") is None


def test_invalidate():
    c = Cache(ttl=60)
    c.set("key", "value")
    c.invalidate("key")
    assert c.get("key") is None


def test_invalidate_prefix():
    c = Cache(ttl=60)
    c.set("table:a", 1)
    c.set("table:b", 2)
    c.set("other:c", 3)
    c.invalidate_prefix("table:")
    assert c.get("table:a") is None
    assert c.get("table:b") is None
    assert c.get("other:c") == 3


def test_ttl_zero_never_expires():
    c = Cache(ttl=0)
    c.set("key", "value")
    assert c.get("key") == "value"


def test_stores_arbitrary_types():
    c = Cache(ttl=60)
    c.set("list", [1, 2, 3])
    c.set("dict", {"a": 1})
    assert c.get("list") == [1, 2, 3]
    assert c.get("dict") == {"a": 1}
