"""Serializer edge cases: rejected records, timestamps in odd forms, required values."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from enum import Enum
from typing import Any

import numpy as np
import pytest

from influxkit import ValidationError
from influxkit._lineprotocol import Dialect
from influxkit._serializer import Serializer
from influxkit.config import MeasurementSchema, TagsConfig, ValidationConfig
from influxkit.tags import TagInjector
from influxkit.types import FieldType

T0 = 1_700_000_000_000_000_000


def make(version: int = 3, *, schemas: dict[str, Any] | None = None, enrichers: Any = (), **validation: Any):
    return Serializer(
        dialect=Dialect.for_version(version),
        validation=ValidationConfig(**validation),
        schemas={name: MeasurementSchema.model_validate(s) for name, s in (schemas or {}).items()},
        injector=TagInjector(TagsConfig(), enrichers=enrichers),
        auto_timestamp=False,
    )


def serialize(serializer: Serializer, *records: Any, precision: str = "ns") -> list[str]:
    return serializer.serialize(list(records), database="db", precision=precision).lines


def rejected(serializer: Serializer, record: Any, **kwargs: Any) -> ValidationError:
    with pytest.raises(ValidationError) as info:
        serialize(serializer, record, **kwargs)
    return info.value


class TestRejectedRecordsLeaveNoTrace:
    def test_types_are_not_locked_by_a_rejected_record(self) -> None:
        serializer = make()
        assert rejected(serializer, {"measurement": "m", "fields": {"v": 1}, "time": "garbage"}).code == (
            "invalid_time"
        )
        assert serializer.locked_types("db", "m") == {}
        assert serialize(serializer, {"measurement": "m", "fields": {"v": 1.5}, "time": T0}) == [
            f"m v=1.5 {T0}"
        ]

    def test_a_later_invalid_field_unlocks_earlier_fields_of_the_record(self) -> None:
        strict = make(non_finite="error")
        rejected(strict, {"measurement": "m", "fields": {"a": 1, "b": float("inf")}})
        assert strict.locked_types("db", "m") == {}

    def test_tag_keys_of_a_rejected_record_are_not_registered(self) -> None:
        serializer = make(3)
        rejected(serializer, {"measurement": "m", "tags": {"x": "a"}, "fields": {"v": 1.0}, "time": "bad"})
        # "x" never reached the server as a tag, so it may be a field.
        assert serialize(serializer, {"measurement": "m", "fields": {"x": 1.0}, "time": T0}) == [
            f"m x=1.0 {T0}"
        ]

    def test_dropped_records_are_reported_with_their_index(self) -> None:
        serializer = make(on_invalid="drop")
        chunk = serializer.serialize(
            [
                {"measurement": "m", "fields": {"v": 1.0}, "time": T0},
                {"measurement": "m", "fields": {}, "time": T0},
                {"measurement": "m", "fields": {"v": "x"}, "time": T0},
            ],
            database="db",
            precision="ns",
        )
        assert chunk.dropped == 2
        assert [(e.index, e.code) for e in chunk.rejected] == [(1, "no_fields"), (2, "type_conflict")]


class TestRequiredValues:
    def test_nan_tag_does_not_satisfy_a_required_tag(self) -> None:
        serializer = make(schemas={"cpu": {"required_tags": ["host"]}})
        error = rejected(
            serializer, {"measurement": "cpu", "tags": {"host": float("nan")}, "fields": {"v": 1.0}}
        )
        assert error.code == "missing_tag"

    def test_nan_field_does_not_satisfy_a_required_field(self) -> None:
        serializer = make(schemas={"cpu": {"required_fields": ["usage"]}})
        record = {"measurement": "cpu", "fields": {"usage": float("nan"), "other": 1.0}}
        assert rejected(serializer, record).code == "missing_field"

    def test_nan_tag_is_not_an_unexpected_tag(self) -> None:
        serializer = make(schemas={"cpu": {"tags": ["host"]}})
        record = {"measurement": "cpu", "tags": {"host": "a", "zone": float("nan")}, "fields": {"v": 1.0}}
        assert serialize(serializer, record) == ["cpu,host=a v=1.0"]


class TestTimestamps:
    @pytest.mark.parametrize(
        "value",
        ["2300-01-01T00:00:00Z", 1.7e12, np.datetime64("2300-01-01"), datetime(2300, 1, 1, tzinfo=UTC)],
        ids=["iso", "float-ms", "datetime64", "datetime"],
    )
    def test_out_of_range_is_rejected_client_side(self, value: Any) -> None:
        error = rejected(make(), {"measurement": "m", "fields": {"v": 1.0}, "time": value})
        assert error.code == "invalid_time"

    def test_out_of_range_integer_for_the_precision(self) -> None:
        error = rejected(make(), {"measurement": "m", "fields": {"v": 1.0}, "time": 10**13}, precision="s")
        assert error.code == "invalid_time"
        assert "precision 's'" in str(error)

    def test_numpy_integers(self) -> None:
        assert serialize(make(), {"measurement": "m", "fields": {"v": 1.0}, "time": np.int64(T0)}) == [
            f"m v=1.0 {T0}"
        ]

    def test_seconds_with_nanosecond_precision_warn(self, caplog: Any) -> None:
        caplog.set_level(logging.WARNING, logger="influxkit.validation")
        lines = serialize(make(), {"measurement": "m", "fields": {"v": 1.0}, "time": 1_700_000_000})
        assert lines == ["m v=1.0 1700000000"]  # written as given
        assert "precision='s'" in caplog.text
        assert "1970-01-01T00:00:01.7" in caplog.text


class TestFloats:
    def test_int_as_float_rejects_what_a_float_cannot_hold(self) -> None:
        serializer = make(int_as_float=True)
        assert rejected(serializer, {"measurement": "m", "fields": {"v": 2**53 + 1}}).code == "out_of_range"
        assert rejected(serializer, {"measurement": "m", "fields": {"v": 10**400}}).code == "out_of_range"
        assert serialize(serializer, {"measurement": "m", "fields": {"v": 2**53}}) == [
            "m v=9007199254740992.0"
        ]


class TestMisc:
    def test_str_enum_mixin_measurement_uses_its_value(self) -> None:
        class Measurement(str, Enum):  # noqa: UP042 - the (str, Enum) form is what is tested
            CPU = "cpu"

        assert serialize(make(), {"measurement": Measurement.CPU, "fields": {"v": 1.0}}) == ["cpu v=1.0"]

    def test_key_error_in_an_enricher_is_not_reported_as_invalid_data(self) -> None:
        def enricher(measurement: str, tags: Any, fields: Any) -> dict[str, str]:
            return {"unit": fields["missing"]}

        serializer = make(enrichers=[enricher], on_invalid="drop")
        with pytest.raises(KeyError):
            serialize(serializer, {"measurement": "m", "fields": {"v": 1.0}})

    def test_untimed_points_differing_in_skipped_fields_do_not_warn(self, caplog: Any) -> None:
        caplog.set_level(logging.WARNING, logger="influxkit.validation")
        serializer = Serializer(
            dialect=Dialect.for_version(3),
            validation=ValidationConfig(),
            schemas={},
            injector=TagInjector(TagsConfig()),
            auto_timestamp=True,
        )
        serializer.serialize(
            [
                {"measurement": "m", "fields": {"a": 1.0, "b": None}},
                {"measurement": "m", "fields": {"a": None, "b": float("nan"), "c": 2.0}},
            ],
            database="db",
            precision="ns",
        )
        assert "overwrite" not in caplog.text

    def test_relock_follows_the_server_but_not_over_a_declaration(self) -> None:
        serializer = make(schemas={"m": {"fields": {"d": "integer"}}})
        serialize(serializer, {"measurement": "m", "fields": {"v": "x", "d": 1}})
        assert serializer.relock("db", "m", "v", FieldType.FLOAT) is FieldType.STRING
        assert serializer.relock("db", "m", "d", FieldType.FLOAT) is None
        assert serializer.locked_types("db", "m") == {"v": FieldType.FLOAT, "d": FieldType.INTEGER}
        assert serialize(serializer, {"measurement": "m", "fields": {"v": 2.5}}) == ["m v=2.5"]

    def test_high_tag_cardinality_warns_once(self, caplog: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        import influxkit._serializer as module

        monkeypatch.setattr(module, "_TAGSET_CACHE_LIMIT", 10)
        caplog.set_level(logging.WARNING, logger="influxkit.validation")
        serializer = make()
        records = [
            {"measurement": "req", "tags": {"request_id": str(i)}, "fields": {"v": 1.0}} for i in range(100)
        ]
        serialize(serializer, *records)
        warnings = [r for r in caplog.records if "distinct tag sets" in r.getMessage()]
        assert len(warnings) == 1
        assert "'req'" in warnings[0].getMessage()
