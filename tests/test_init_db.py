import unittest
from unittest.mock import patch

from app.init_db import main, run_init


class InitDbTests(unittest.TestCase):
    def test_run_init_includes_compile_result(self):
        with patch("app.init_db.bootstrap_database", return_value={"imported_tables": [], "missing_tables": [], "added_columns": [], "internal_tables_checked": [], "default_rule_seeded": False}), patch(
            "app.init_db.compileall.compile_dir",
            return_value=True,
        ) as compile_mock:
            report = run_init(skip_compile=False, attached_only=True)

        self.assertTrue(report["compile_ok"])
        compile_mock.assert_called()

    def test_main_returns_failure_when_compile_fails(self):
        with patch("app.init_db.bootstrap_database", return_value={"imported_tables": [], "missing_tables": [], "added_columns": [], "internal_tables_checked": [], "default_rule_seeded": False}), patch(
            "app.init_db.compileall.compile_dir",
            return_value=False,
        ):
            exit_code = main(["--json"])

        self.assertEqual(exit_code, 1)

    def test_run_init_disables_dump_import_in_attached_mode(self):
        with patch(
            "app.init_db.bootstrap_database",
            return_value={"imported_tables": [], "missing_tables": [], "added_columns": [], "internal_tables_checked": [], "default_rule_seeded": False},
        ) as bootstrap_mock, patch("app.init_db.compileall.compile_dir", return_value=True):
            run_init(skip_compile=True, attached_only=True)

        bootstrap_mock.assert_called_once_with(import_missing_business_tables=False)


if __name__ == "__main__":
    unittest.main()
