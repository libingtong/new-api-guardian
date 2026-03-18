import json
import unittest
from unittest.mock import patch

import app.breaker as breaker


class _FakeResponse:
    def __init__(self, status_code=200, body='{"success":true}'):
        self._status_code = status_code
        self._body = body.encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def getcode(self):
        return self._status_code

    def read(self):
        return self._body


class BreakerRefreshTests(unittest.TestCase):
    def test_skip_refresh_when_not_configured(self):
        with (
            patch.object(breaker, "NEW_API_BASE_URL", ""),
            patch("app.breaker.urllib_request.urlopen") as mock_urlopen,
        ):
            refreshed = breaker.refresh_new_api_channel_cache(38)

        self.assertFalse(refreshed)
        mock_urlopen.assert_not_called()

    def test_resolve_refresh_auth_from_database_when_env_missing(self):
        fake_row = {"id": 5, "access_token": "db-token"}

        class _FakeCursor:
            def execute(self, query, params):
                self.query = query
                self.params = params

            def fetchone(self):
                return fake_row

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class _FakeConn:
            def cursor(self):
                return _FakeCursor()

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        with (
            patch.object(breaker, "NEW_API_ACCESS_TOKEN", ""),
            patch.object(breaker, "NEW_API_USER_ID", ""),
            patch("app.breaker.get_conn", return_value=_FakeConn()),
        ):
            access_token, user_id = breaker.resolve_new_api_auth()

        self.assertEqual(access_token, "db-token")
        self.assertEqual(user_id, "5")

    def test_refresh_channel_cache_via_new_api(self):
        with (
            patch.object(breaker, "NEW_API_BASE_URL", "http://new-api.local"),
            patch.object(breaker, "NEW_API_ACCESS_TOKEN", "token-123"),
            patch.object(breaker, "NEW_API_USER_ID", "7"),
            patch.object(breaker, "NEW_API_REFRESH_TIMEOUT_SECONDS", 5.0),
            patch("app.breaker.urllib_request.urlopen", return_value=_FakeResponse()) as mock_urlopen,
        ):
            refreshed = breaker.refresh_new_api_channel_cache(38)

        self.assertTrue(refreshed)
        request = mock_urlopen.call_args.args[0]
        timeout = mock_urlopen.call_args.kwargs["timeout"]
        self.assertEqual(timeout, 5.0)
        self.assertEqual(request.full_url, "http://new-api.local/api/channel/")
        self.assertEqual(request.get_method(), "PUT")
        self.assertEqual(request.get_header("Authorization"), "token-123")
        self.assertEqual(request.get_header("New-api-user"), "7")
        self.assertEqual(json.loads(request.data.decode("utf-8")), {"id": 38})

    def test_refresh_channel_cache_uses_database_auth_when_env_missing(self):
        with (
            patch.object(breaker, "NEW_API_BASE_URL", "http://new-api.local"),
            patch.object(breaker, "NEW_API_ACCESS_TOKEN", ""),
            patch.object(breaker, "NEW_API_USER_ID", ""),
            patch.object(breaker, "NEW_API_REFRESH_TIMEOUT_SECONDS", 5.0),
            patch("app.breaker.resolve_new_api_auth", return_value=("db-token", "9")),
            patch("app.breaker.urllib_request.urlopen", return_value=_FakeResponse()) as mock_urlopen,
        ):
            refreshed = breaker.refresh_new_api_channel_cache(88)

        self.assertTrue(refreshed)
        request = mock_urlopen.call_args.args[0]
        self.assertEqual(request.get_header("Authorization"), "db-token")
        self.assertEqual(request.get_header("New-api-user"), "9")
        self.assertEqual(json.loads(request.data.decode("utf-8")), {"id": 88})

    def test_new_api_refresh_status_warns_when_no_admin_access_token(self):
        with (
            patch.object(breaker, "NEW_API_BASE_URL", "http://new-api.local"),
            patch.object(breaker, "NEW_API_ACCESS_TOKEN", ""),
            patch.object(breaker, "NEW_API_USER_ID", ""),
            patch("app.breaker.resolve_new_api_auth", return_value=("", "")),
        ):
            status = breaker.get_new_api_refresh_status()

        self.assertTrue(status["configured"])
        self.assertFalse(status["ready"])
        self.assertIn("access_token", status["warning"])


if __name__ == "__main__":
    unittest.main()
