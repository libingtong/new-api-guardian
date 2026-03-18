import unittest

from app.breaker import RulePayload, RuleValidationError, payload_to_record
from app.breaker_logic import (
    event_matches_rule,
    interpret_probe_response,
    parse_csv_items,
    remove_model_from_list,
    restore_model_to_list,
)
from app.sql_utils import split_sql_statements


class BreakerLogicTests(unittest.TestCase):
    def test_event_matches_rule_by_text_and_scope(self):
        event = {
            "channel_id": 31,
            "group": "coding",
            "model_name": "glm-5",
            "content": "渠道上游没有返回消息，请稍后重试",
            "error_code": "channel:no_response",
            "status_code": 503,
            "request_path": "/v1/chat/completions",
            "channel_name": "PackyAli",
            "other": {"foo": "bar"},
        }
        rule = {
            "id": 1,
            "match_channel_ids": [31],
            "match_groups": ["coding"],
            "match_models": ["glm-5"],
            "match_error_text": ["上游没有返回消息"],
            "match_error_codes": ["channel:no_response"],
            "match_status_codes": [503],
            "match_request_paths": ["/v1/chat/completions"],
        }
        self.assertTrue(event_matches_rule(event, rule))

    def test_event_does_not_match_when_scope_misses(self):
        event = {
            "channel_id": 39,
            "group": "default",
            "model_name": "gemini-3-pro-preview",
            "content": "status_code=408, 响应时间超时",
            "error_code": "channel:response_time_exceeded",
            "status_code": 408,
            "request_path": "/v1/chat/completions",
            "channel_name": "Gemini",
            "other": {},
        }
        rule = {
            "id": 2,
            "match_channel_ids": [31],
            "match_groups": ["coding"],
            "match_models": [],
            "match_error_text": ["超时"],
            "match_error_codes": [],
            "match_status_codes": [],
            "match_request_paths": [],
        }
        self.assertFalse(event_matches_rule(event, rule))

    def test_interpret_probe_response_handles_false_payload(self):
        payload = interpret_probe_response(200, '{"success": false, "message": "channel down"}')
        self.assertFalse(payload["success"])
        self.assertEqual(payload["detail"], "channel down")

    def test_interpret_probe_response_accepts_success_payload(self):
        payload = interpret_probe_response(200, '{"success": true, "data": {"latency": 120}}')
        self.assertTrue(payload["success"])

    def test_parse_csv_items_deduplicates_and_trims(self):
        self.assertEqual(parse_csv_items(" glm-5, qwen3-max ,glm-5 "), ["glm-5", "qwen3-max"])

    def test_remove_model_from_list_only_removes_target(self):
        items = ["glm-5", "qwen3-max", "qwen3.5-plus"]
        self.assertEqual(remove_model_from_list(items, "qwen3-max"), ["glm-5", "qwen3.5-plus"])

    def test_restore_model_to_list_respects_original_order(self):
        current = ["glm-5", "qwen3.5-plus"]
        original = ["glm-5", "qwen3-max", "qwen3.5-plus"]
        self.assertEqual(
            restore_model_to_list(current, original, "qwen3-max"),
            ["glm-5", "qwen3-max", "qwen3.5-plus"],
        )

    def test_empty_match_rule_is_rejected(self):
        payload = RulePayload(name="unsafe")
        with self.assertRaises(RuleValidationError):
            payload_to_record(payload)

    def test_unstable_channel_action_is_accepted(self):
        payload = RulePayload(name="unstable", match_channel_ids=[38], action_type="disable_unstable_channel")
        record = payload_to_record(payload)
        self.assertEqual(record["action_type"], "disable_unstable_channel")

    def test_split_sql_statements_keeps_insert_payload_intact(self):
        script = """
        DROP TABLE IF EXISTS `demo`;
        CREATE TABLE `demo` (`id` bigint(20), `payload` longtext);
        INSERT INTO `demo` VALUES (1, '{"message":"a;still inside","nested":{"ok":true}}');
        """
        statements = split_sql_statements(script)
        self.assertEqual(len(statements), 3)
        self.assertIn("a;still inside", statements[2])


if __name__ == "__main__":
    unittest.main()
