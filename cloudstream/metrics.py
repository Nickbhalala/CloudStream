"""CloudWatch metrics via Embedded Metric Format: one JSON log line, no PutMetricData calls."""

from __future__ import annotations

import json
import time
from collections.abc import Callable

NAMESPACE = "CloudStream"


class Metrics:
    def __init__(
        self, dimensions: dict[str, str] | None = None, sink: Callable[[str], None] = print
    ) -> None:
        self.dimensions = dimensions or {}
        self.values: dict[str, float] = {}
        self.units: dict[str, str] = {}
        self.sink = sink

    def add(self, name: str, value: float, unit: str = "Count") -> None:
        self.values[name] = self.values.get(name, 0) + value
        self.units[name] = unit

    def put(self, name: str, value: float, unit: str) -> None:
        self.values[name] = value
        self.units[name] = unit

    def emf(self) -> dict:
        return {
            "_aws": {
                "Timestamp": int(time.time() * 1000),
                "CloudWatchMetrics": [
                    {
                        "Namespace": NAMESPACE,
                        "Dimensions": [list(self.dimensions)],
                        "Metrics": [{"Name": n, "Unit": self.units[n]} for n in self.values],
                    }
                ],
            },
            **self.dimensions,
            **self.values,
        }

    def flush(self) -> None:
        if self.values:
            self.sink(json.dumps(self.emf()))
            self.values.clear()
