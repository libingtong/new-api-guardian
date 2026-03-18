import argparse
import compileall
import json
import sys
from pathlib import Path

from app.schema import bootstrap_database


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Initialize the breaker schema for a target OneAPI database and validate Python compilation."
    )
    parser.add_argument(
        "--skip-compile",
        action="store_true",
        help="Initialize schema only and skip compileall validation.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the initialization report as JSON.",
    )
    parser.add_argument(
        "--attached-only",
        action="store_true",
        help="Do not import missing business tables from the SQL dump; only validate the existing database and create internal tables.",
    )
    return parser


def run_init(skip_compile: bool = False, attached_only: bool = False) -> dict:
    report = bootstrap_database(import_missing_business_tables=not attached_only)
    compile_ok = True
    if not skip_compile:
        root = Path(__file__).resolve().parent.parent
        compile_ok = bool(
            compileall.compile_dir(str(root / "app"), quiet=1)
            and compileall.compile_dir(str(root / "tests"), quiet=1)
        )
    report["compile_ok"] = compile_ok
    return report


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    report = run_init(skip_compile=args.skip_compile, attached_only=args.attached_only)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        added_columns_text = ", ".join(
            f"{item['table']}.{item['column']}" for item in report["added_columns"]
        ) or "none"
        print("Initialization complete.")
        print(f"Imported tables: {', '.join(report['imported_tables']) or 'none'}")
        print(f"Missing tables: {', '.join(report['missing_tables']) or 'none'}")
        print(f"Added columns: {added_columns_text}")
        print(f"Internal tables checked: {', '.join(report['internal_tables_checked'])}")
        print(f"Default rule seeded: {'yes' if report['default_rule_seeded'] else 'no'}")
        print(f"Compile validation: {'ok' if report['compile_ok'] else 'failed'}")

    return 0 if report["compile_ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
