import unittest
from unittest.mock import Mock, patch
from ipaddress import ip_network

from fastapi.testclient import TestClient

import app.main as main_module

app = main_module.app


class MainRouteTests(unittest.TestCase):
    def test_home_page_renders_leaderboard(self):
        worker = Mock()
        worker.snapshot.return_value = {"log_scanner": {"running": False}}
        worker_cls = Mock(return_value=worker)

        with patch("app.main.bootstrap_database"), patch("app.main.WorkerManager", worker_cls):
            with TestClient(app) as client:
                response = client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertIn("智通AI Token 使用排行榜", response.text)

    def test_hidden_admin_dashboard_renders(self):
        worker = Mock()
        worker.snapshot.return_value = {"log_scanner": {"running": False}}
        worker_cls = Mock(return_value=worker)

        with patch("app.main.bootstrap_database"), patch("app.main.WorkerManager", worker_cls):
            with TestClient(app) as client:
                response = client.get(main_module.ADMIN_PATH)

        self.assertEqual(response.status_code, 200)
        self.assertIn("渠道熔断与自动恢复控制台", response.text)

    def test_admin_dashboard_shows_login_when_password_enabled(self):
        worker = Mock()
        worker.snapshot.return_value = {}
        worker_cls = Mock(return_value=worker)

        with patch("app.main.ADMIN_PASSWORD", "secret"), patch("app.main.bootstrap_database"), patch(
            "app.main.WorkerManager",
            worker_cls,
        ):
            with TestClient(app) as client:
                response = client.get(main_module.ADMIN_PATH)

        self.assertEqual(response.status_code, 200)
        self.assertIn("管理页登录", response.text)

    def test_admin_login_sets_cookie(self):
        worker = Mock()
        worker.snapshot.return_value = {}
        worker_cls = Mock(return_value=worker)

        with patch("app.main.ADMIN_PASSWORD", "secret"), patch("app.main.bootstrap_database"), patch(
            "app.main.WorkerManager",
            worker_cls,
        ):
            with TestClient(app) as client:
                response = client.post("/api/admin-auth/login", json={"password": "secret"})

        self.assertEqual(response.status_code, 200)
        self.assertIn("admin_auth=", response.headers["set-cookie"])

    def test_admin_api_requires_auth_when_password_enabled(self):
        worker = Mock()
        worker.snapshot.return_value = {}
        worker_cls = Mock(return_value=worker)

        with patch("app.main.ADMIN_PASSWORD", "secret"), patch("app.main.bootstrap_database"), patch(
            "app.main.WorkerManager",
            worker_cls,
        ):
            with TestClient(app) as client:
                response = client.get("/api/rules")

        self.assertEqual(response.status_code, 401)

    def test_health_endpoint_returns_worker_snapshot(self):
        worker = Mock()
        worker.snapshot.return_value = {"log_scanner": {"running": True}}
        worker_cls = Mock(return_value=worker)

        with patch("app.main.bootstrap_database"), patch("app.main.WorkerManager", worker_cls):
            with TestClient(app) as client:
                response = client.get("/api/health")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        self.assertIn("log_scanner", response.json()["workers"])

    def test_rules_endpoint_uses_backend_service(self):
        worker = Mock()
        worker.snapshot.return_value = {}
        worker_cls = Mock(return_value=worker)
        fake_rules = [{"id": 1, "name": "demo", "enabled": True}]

        with patch("app.main.bootstrap_database"), patch("app.main.WorkerManager", worker_cls), patch(
            "app.main.list_rules",
            return_value=fake_rules,
        ):
            with TestClient(app) as client:
                response = client.get("/api/rules")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["items"], fake_rules)

    def test_recovery_state_endpoint_returns_channel_and_model_items(self):
        worker = Mock()
        worker.snapshot.return_value = {}
        worker_cls = Mock(return_value=worker)
        fake_state = {"channel_items": [{"id": 1}], "model_items": [{"channel_id": 2, "model_name": "glm-5"}]}

        with patch("app.main.bootstrap_database"), patch("app.main.WorkerManager", worker_cls), patch(
            "app.main.list_recovery_states",
            return_value=fake_state,
        ):
            with TestClient(app) as client:
                response = client.get("/api/channels/recovery-state")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), fake_state)

    def test_delete_rule_endpoint_calls_backend_service(self):
        worker = Mock()
        worker.snapshot.return_value = {}
        worker_cls = Mock(return_value=worker)

        with patch("app.main.ADMIN_PASSWORD", ""), patch("app.main.bootstrap_database"), patch(
            "app.main.WorkerManager",
            worker_cls,
        ), patch("app.main.delete_rule") as delete_rule_mock:
            with TestClient(app) as client:
                response = client.delete("/api/rules/12")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"ok": True})
        delete_rule_mock.assert_called_once_with(12)

    def test_create_rule_endpoint_returns_400_for_empty_match_rule(self):
        worker = Mock()
        worker.snapshot.return_value = {}
        worker_cls = Mock(return_value=worker)

        with patch("app.main.ADMIN_PASSWORD", ""), patch("app.main.bootstrap_database"), patch(
            "app.main.WorkerManager",
            worker_cls,
        ):
            with TestClient(app) as client:
                response = client.post("/api/rules", json={"name": "unsafe", "action_type": "disable_channel"})

        self.assertEqual(response.status_code, 400)
        self.assertIn("at least one match condition is required", response.text)

    def test_ip_allowlist_blocks_non_whitelisted_clients(self):
        worker = Mock()
        worker.snapshot.return_value = {}
        worker_cls = Mock(return_value=worker)

        with patch("app.main.ALLOWED_IP_NETWORKS", [ip_network("127.0.0.1/32")]), patch(
            "app.main.bootstrap_database"
        ), patch("app.main.WorkerManager", worker_cls), patch("app.main.LOGGER.warning") as warning_mock:
            with TestClient(app) as client:
                response = client.get("/", headers={"x-forwarded-for": "8.8.8.8"})

        self.assertEqual(response.status_code, 403)
        warning_mock.assert_called_once()
        self.assertIn("ip access denied", warning_mock.call_args.args[0])

    def test_ip_allowlist_allows_whitelisted_clients(self):
        worker = Mock()
        worker.snapshot.return_value = {}
        worker_cls = Mock(return_value=worker)

        with patch("app.main.ALLOWED_IP_NETWORKS", [ip_network("10.0.0.0/8")]), patch(
            "app.main.bootstrap_database"
        ), patch("app.main.WorkerManager", worker_cls):
            with TestClient(app) as client:
                response = client.get(main_module.ADMIN_PATH, headers={"x-forwarded-for": "10.2.3.4"})

        self.assertEqual(response.status_code, 200)
        self.assertIn("渠道熔断与自动恢复控制台", response.text)


if __name__ == "__main__":
    unittest.main()
