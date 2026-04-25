import requests

from etl.extract import manga_api as api


class FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, response):
        self.response = response

    def get(self, *_args, **_kwargs):
        return self.response


def test_request_page_from_returns_data_key():
    session = FakeSession(FakeResponse(payload={"data": [{"id": 1}]}))

    result = api._request_page_from("https://example.com", 10, 0, session)

    assert result == [{"id": 1}]


def test_request_page_from_returns_list_payload_as_is():
    session = FakeSession(FakeResponse(payload=[{"id": "m1"}]))

    result = api._request_page_from("https://example.com", 10, 0, session)

    assert result == [{"id": "m1"}]


def test_request_page_from_tolerates_400_as_end_of_data():
    session = FakeSession(FakeResponse(status_code=400, payload={"error": "bad offset"}))

    result = api._request_page_from("https://example.com", 10, 1000, session, tolerate_400=True)

    assert result == []
