import unittest
from unittest.mock import patch

import app.breaker as breaker
from app.breaker import should_execute_disable_action


class BreakerProbeGateTests(unittest.TestCase):
    def test_skip_disable_when_active_probe_succeeds(self):
        with patch("app.breaker.perform_channel_probe", return_value=(True, "ok", {"success": True})):
            should_disable, detail, payload = should_execute_disable_action(38, "glm-5")

        self.assertFalse(should_disable)
        self.assertEqual(detail, "probe succeeded; skip disable")
        self.assertEqual(payload, {"success": True})

    def test_allow_disable_when_active_probe_fails(self):
        with patch(
            "app.breaker.perform_channel_probe",
            return_value=(False, "upstream did not return", {"success": False, "detail": "upstream did not return"}),
        ):
            should_disable, detail, payload = should_execute_disable_action(38, "glm-5")

        self.assertTrue(should_disable)
        self.assertEqual(detail, "upstream did not return")
        self.assertFalse(payload["success"])

    def test_channel_probe_appends_stream_flag_when_enabled(self):
        captured = {}

        class _FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def getcode(self):
                return 200

            def read(self):
                return b'{"success": true}'

        def fake_urlopen(req, timeout):
            captured["url"] = req.full_url
            captured["timeout"] = timeout
            return _FakeResponse()

        with (
            patch.object(breaker, "PROBE_URL_TEMPLATE", "https://probe.local/api/channel/test/{channel_id}?model={model}"),
            patch.object(breaker, "PROBE_STREAM_ENABLED", True),
            patch("app.breaker.urllib_request.urlopen", side_effect=fake_urlopen),
        ):
            success, detail, payload = breaker.perform_channel_probe(38, "glm-5")

        self.assertTrue(success)
        self.assertEqual(detail, "ok")
        self.assertEqual(captured["url"], "https://probe.local/api/channel/test/38?model=glm-5&stream=true")
        self.assertTrue(payload["success"])


if __name__ == "__main__":
    unittest.main()
