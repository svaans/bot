import pytest

from core.data.external_feeds import ExternalFeeds


class DummyResp:
    def __init__(self, data):
        self._data = data

    async def json(self):
        return self._data

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeSession:
    def __init__(self, data):
        self._data = data

    def get(self, url):
        return DummyResp(self._data)


@pytest.mark.asyncio
async def test_funding_rate_rest_empty_list():
    feeds = ExternalFeeds()
    feeds.session = FakeSession([])
    result = await feeds.funding_rate_rest("BTCEUR")
    assert result is None


@pytest.mark.asyncio
async def test_open_interest_rest_empty_list():
    feeds = ExternalFeeds()
    feeds.session = FakeSession([])
    result = await feeds.open_interest_rest("BTCEUR")
    assert result is None