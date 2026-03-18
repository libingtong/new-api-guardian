from typing import List


def split_sql_statements(script: str) -> List[str]:
    statements: List[str] = []
    buf: List[str] = []
    in_single = False
    in_double = False
    in_backtick = False
    in_line_comment = False
    in_block_comment = False
    escape = False
    idx = 0

    while idx < len(script):
        ch = script[idx]
        nxt = script[idx + 1] if idx + 1 < len(script) else ""

        if in_line_comment:
            buf.append(ch)
            if ch == "\n":
                in_line_comment = False
            idx += 1
            continue

        if in_block_comment:
            buf.append(ch)
            if ch == "*" and nxt == "/":
                buf.append(nxt)
                idx += 2
                in_block_comment = False
                continue
            idx += 1
            continue

        if not in_single and not in_double and not in_backtick:
            if ch == "-" and nxt == "-":
                in_line_comment = True
                buf.append(ch)
                buf.append(nxt)
                idx += 2
                continue
            if ch == "/" and nxt == "*":
                in_block_comment = True
                buf.append(ch)
                buf.append(nxt)
                idx += 2
                continue

        buf.append(ch)

        if in_single:
            if ch == "'" and not escape:
                in_single = False
            escape = ch == "\\" and not escape
            idx += 1
            continue

        if in_double:
            if ch == '"' and not escape:
                in_double = False
            escape = ch == "\\" and not escape
            idx += 1
            continue

        if in_backtick:
            if ch == "`":
                in_backtick = False
            idx += 1
            continue

        if ch == "'":
            in_single = True
        elif ch == '"':
            in_double = True
        elif ch == "`":
            in_backtick = True
        elif ch == ";":
            statement = "".join(buf).strip()
            if statement:
                statements.append(statement)
            buf = []

        idx += 1

    trailing = "".join(buf).strip()
    if trailing:
        statements.append(trailing)
    return statements
