from __future__ import annotations

import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Iterable


def normalize_partition_value(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def sql_literal(value: object) -> str:
    normalized = normalize_partition_value(value).replace("'", "''")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", normalized):
        return f"DATE '{normalized}'"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", normalized):
        return f"TIMESTAMP '{normalized}'"
    return f"'{normalized}'"


def build_replace_where(column: str, values: Iterable[object]) -> str:
    unique_values = []
    seen = set()
    for value in values:
        normalized = normalize_partition_value(value)
        if normalized in seen:
            continue
        seen.add(normalized)
        unique_values.append(value)

    if not unique_values:
        raise ValueError(f"Cannot build replaceWhere clause for column '{column}' without values.")

    literals = ", ".join(sql_literal(value) for value in unique_values)
    return f"{column} IN ({literals})"


def read_impacted_months_manifest(path: str | Path) -> list[str]:
    resolved_path = Path(path)
    if not resolved_path.exists():
        return []

    payload = json.loads(resolved_path.read_text(encoding="utf-8"))
    values = payload.get("claim_months", [])
    return sorted({normalize_partition_value(value) for value in values})


def write_impacted_months_manifest(path: str | Path, claim_months: Iterable[object]) -> None:
    resolved_path = Path(path)
    existing_values = set(read_impacted_months_manifest(resolved_path))
    new_values = {normalize_partition_value(value) for value in claim_months}
    merged_values = sorted(existing_values | new_values)

    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_path.write_text(
        json.dumps({"claim_months": merged_values}, indent=2),
        encoding="utf-8",
    )


def clear_impacted_months_manifest(path: str | Path) -> None:
    resolved_path = Path(path)
    if resolved_path.exists():
        resolved_path.unlink()
