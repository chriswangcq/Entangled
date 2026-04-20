"""Process-wide zero-dependency Prometheus-exposition metrics for Entangled.

PR-32 (2026-04-21) — Entangled is a foundation package with no
dependency on ``novaic-common``, so the shared metrics module in
``common.utils.metrics`` is not reachable. This module deliberately
mirrors that one's public surface (``metric_inc`` / ``metric_observe``
/ ``metric_set`` / ``metric_timer`` / ``render_metrics``) so call sites
read the same regardless of which service they live in.

If you ever change the semantics in one file, change both. A cross-repo
test (``tests/test_metrics_surface_parity``, TODO) can lock the two
helpers together; until then, diff the two module headers whenever
touching either side.
"""

from __future__ import annotations

import threading
import time
from typing import Dict, Tuple


_METRIC_LOCK = threading.Lock()
_COUNTERS: Dict[str, Dict[Tuple[Tuple[str, str], ...], float]] = {}
_HISTOGRAMS: Dict[str, Dict[Tuple[Tuple[str, str], ...], Tuple[float, float]]] = {}
_GAUGES: Dict[str, Dict[Tuple[Tuple[str, str], ...], float]] = {}


def _label_key(labels: Dict[str, str]) -> Tuple[Tuple[str, str], ...]:
    return tuple(sorted((str(k), str(v)) for k, v in labels.items()))


def metric_inc(name: str, value: float = 1.0, **labels: str) -> None:
    key = _label_key(labels)
    with _METRIC_LOCK:
        bucket = _COUNTERS.setdefault(name, {})
        bucket[key] = bucket.get(key, 0.0) + float(value)


def metric_observe(name: str, value: float, **labels: str) -> None:
    key = _label_key(labels)
    with _METRIC_LOCK:
        bucket = _HISTOGRAMS.setdefault(name, {})
        s, c = bucket.get(key, (0.0, 0.0))
        bucket[key] = (s + float(value), c + 1.0)


def metric_set(name: str, value: float, **labels: str) -> None:
    key = _label_key(labels)
    with _METRIC_LOCK:
        bucket = _GAUGES.setdefault(name, {})
        bucket[key] = float(value)


def _format_labels(labels: Tuple[Tuple[str, str], ...]) -> str:
    if not labels:
        return ""
    inner = ",".join(f'{k}="{v}"' for k, v in labels)
    return "{" + inner + "}"


def render_metrics() -> str:
    lines: list[str] = []
    with _METRIC_LOCK:
        for name, bucket in sorted(_COUNTERS.items()):
            lines.append(f"# TYPE {name} counter")
            for labels, val in bucket.items():
                lines.append(f"{name}{_format_labels(labels)} {val}")
        for name, bucket in sorted(_GAUGES.items()):
            lines.append(f"# TYPE {name} gauge")
            for labels, val in bucket.items():
                lines.append(f"{name}{_format_labels(labels)} {val}")
        for name, bucket in sorted(_HISTOGRAMS.items()):
            lines.append(f"# TYPE {name} summary")
            for labels, (s, c) in bucket.items():
                lab = _format_labels(labels)
                lines.append(f"{name}_sum{lab} {s}")
                lines.append(f"{name}_count{lab} {c}")
    lines.append("")
    return "\n".join(lines)


class _MetricTimer:
    __slots__ = ("_name", "_labels", "_start")

    def __init__(self, name: str, **labels: str) -> None:
        self._name = name
        self._labels = labels
        self._start = 0.0

    def __enter__(self) -> "_MetricTimer":
        self._start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - self._start
        metric_observe(self._name, elapsed, **self._labels)


def metric_timer(name: str, **labels: str) -> _MetricTimer:
    return _MetricTimer(name, **labels)


def reset_all_metrics_for_tests() -> None:
    with _METRIC_LOCK:
        _COUNTERS.clear()
        _HISTOGRAMS.clear()
        _GAUGES.clear()


__all__ = [
    "metric_inc",
    "metric_observe",
    "metric_set",
    "metric_timer",
    "render_metrics",
    "reset_all_metrics_for_tests",
]
