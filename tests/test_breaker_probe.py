import unittest
from unittest.mock import patch

import app.breaker as breaker
from app.breaker import build_unstable_channel_notification_payload, should_execute_disable_action


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

    def test_channel_probe_uses_unified_new_api_auth_headers(self):
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
            captured["authorization"] = req.get_header("Authorization")
            captured["user"] = req.get_header("New-api-user")
            captured["timeout"] = timeout
            return _FakeResponse()

        with (
            patch.object(breaker, "PROBE_URL_TEMPLATE", "https://probe.local/api/channel/test/{channel_id}?model={model}"),
            patch.object(breaker, "PROBE_STREAM_ENABLED", False),
            patch.object(breaker, "NEW_API_ACCESS_TOKEN", "token-123"),
            patch.object(breaker, "NEW_API_USER_ID", "7"),
            patch("app.breaker.urllib_request.urlopen", side_effect=fake_urlopen),
        ):
            success, detail, payload = breaker.perform_channel_probe(38, "glm-5")

        self.assertTrue(success)
        self.assertEqual(detail, "ok")
        self.assertEqual(captured["authorization"], "token-123")
        self.assertEqual(captured["user"], "7")
        self.assertTrue(payload["success"])

    def test_build_unstable_channel_notification_payload_contains_manual_restore_context(self):
        payload = build_unstable_channel_notification_payload(
            event={
                "channel_id": 38,
                "channel_name": "PackyAli",
                "model_name": "glm-5",
                "error_code": "channel:no_response",
                "status_code": 500,
                "content": "upstream did not return",
            },
            rule={"name": "渠道反复抖动", "window_seconds": 3600, "threshold_count": 3},
            source_log_ids=[101, 102, 103],
        )

        self.assertEqual(payload["title"], "New API 渠道稳定性告警")
        self.assertIn("人工恢复", payload["body"])
        self.assertIn("PackyAli", payload["body"])
        self.assertIn("channel:no_response", payload["body"])

    def test_send_wecom_markdown_posts_markdown_payload(self):
        captured = {}

        class _FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def getcode(self):
                return 200

            def read(self):
                return b'{"errcode":0,"errmsg":"ok"}'

        def fake_urlopen(req, timeout):
            captured["url"] = req.full_url
            captured["timeout"] = timeout
            captured["body"] = req.data.decode("utf-8")
            return _FakeResponse()

        with (
            patch.object(breaker, "WECOM_ROBOT_WEBHOOK", "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=test"),
            patch.object(breaker, "WECOM_NOTIFY_TIMEOUT_SECONDS", 4),
            patch("app.breaker.urllib_request.urlopen", side_effect=fake_urlopen),
        ):
            breaker._send_wecom_markdown("告警标题", "告警正文")

        self.assertEqual(captured["url"], "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=test")
        self.assertEqual(captured["timeout"], 4)
        self.assertIn('"msgtype":"markdown"', captured["body"])
        self.assertIn("告警标题", captured["body"])
        self.assertIn("告警正文", captured["body"])


if __name__ == "__main__":
    unittest.main()
