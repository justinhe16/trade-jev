"""Builders for a panel command's output (PER-280). Stdlib only: copy this file next to your
script, like ``emit.py``; extensions run as ``python -I -S`` and can't import the SDK.

A panel command prints one JSON object (the contract is ``panel_schema.py`` and
``docs/extensions.md`` 6a). These helpers build the pieces as plain dicts, leave out what is
None, and turn ``datetime`` values into ISO-8601 text:

    from panel import kv, panel, print_panel, series, stat, table, view

    print_panel([
        view("overview", "Overview", [
            stat("Equity", 10432.5, format="money"),
            series("line", [t0, t1, t2], {"Equity": [10000, 10200, 10432.5]}, format="money"),
        ]),
    ])
"""

from __future__ import annotations

import json
import sys
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

SCHEMA = 1


def _clean(**fields: Any) -> dict[str, Any]:
    return {k: _jsonable(v) for k, v in fields.items() if v is not None}


def _jsonable(v: Any) -> Any:
    if isinstance(v, datetime):
        if v.tzinfo is None:
            raise ValueError("a panel time needs a timezone offset")
        return v.isoformat()
    if isinstance(v, Mapping):
        return {k: _jsonable(x) for k, x in v.items()}  # pyright: ignore[reportUnknownVariableType]
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]  # pyright: ignore[reportUnknownVariableType]
    return v


def stat(label: str, value: Any, **fields: Any) -> dict[str, Any]:
    """A big number or short text. Fields: unit, format, currency, delta, delta_format,
    health, hint, id, title."""
    return _clean(type="stat", label=label, value=value, **fields) | {"value": _jsonable(value)}


def series(
    kind: str,
    x: Sequence[Any],
    lines: Mapping[str, Sequence[Any]] | Sequence[tuple[str, Sequence[Any]]],
    *,
    x_type: str = "time",
    **fields: Any,
) -> dict[str, Any]:
    """A line, area or bar chart: ``x`` and one list of values per line (None is a gap).
    Fields: unit, format, currency, stacked, id, title."""
    pairs = list(lines.items()) if isinstance(lines, Mapping) else list(lines)
    return _clean(
        type="series",
        kind=kind,
        x={"type": x_type, "values": list(x)},
        series=[{"name": n, "values": list(v)} for n, v in pairs],
        **fields,
    )


def column(key: str, label: str, type: str = "text", **fields: Any) -> dict[str, Any]:
    """A table column. ``type``: text, number, money, time, pill or link."""
    return _clean(key=key, label=label, type=type, **fields)


def pill(label: str, tone: str = "neutral") -> dict[str, str]:
    return {"label": label, "tone": tone}


def link(label: str, tool: str) -> dict[str, str]:
    return {"label": label, "tool": tool}


def table(
    columns: Sequence[Mapping[str, Any]], rows: Sequence[Mapping[str, Any]], **fields: Any
) -> dict[str, Any]:
    """Rows are ``{column key: cell}``. Fields: id, title."""
    return _clean(type="table", columns=list(columns), rows=list(rows), **fields)


def status(items: Sequence[Mapping[str, Any]], **fields: Any) -> dict[str, Any]:
    """Items are ``{name, state (up|down|warn|unknown), detail?, since?}``."""
    return _clean(type="status", items=list(items), **fields)


def kv(items: Mapping[str, Any] | Sequence[Mapping[str, Any]], **fields: Any) -> dict[str, Any]:
    """Key/value pairs: a dict, or a list of ``{key, value, format?, unit?}``."""
    rows = (
        [{"key": k, "value": v} for k, v in items.items()]
        if isinstance(items, Mapping)
        else list(items)
    )
    return _clean(type="kv", items=rows, **fields)


def text(body: str, **fields: Any) -> dict[str, Any]:
    """Plain text (never markup)."""
    return _clean(type="text", text=body, **fields)


def details(data: Any, *, collapsed: bool = True, **fields: Any) -> dict[str, Any]:
    """Any JSON value, shown as an expandable tree."""
    return {"type": "details", "data": _jsonable(data), "collapsed": collapsed} | _clean(**fields)


def view(id: str, title: str, widgets: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {"id": id, "title": title, "widgets": list(widgets)}


def panel(
    views: Sequence[Mapping[str, Any]],
    *,
    updated_at: datetime | None = None,
    health: str | None = None,
    local_url: str | None = None,
) -> dict[str, Any]:
    """The whole output object. ``updated_at`` is the data's time (default: when it ran)."""
    return {"schema": SCHEMA, "views": list(views)} | _clean(
        updated_at=updated_at, health=health, local_url=local_url
    )


def print_panel(views: Sequence[Mapping[str, Any]], **fields: Any) -> None:
    """Print the panel as the one JSON object on stdout the command must end with."""
    sys.stdout.write(json.dumps(panel(views, **fields), separators=(",", ":")) + "\n")
