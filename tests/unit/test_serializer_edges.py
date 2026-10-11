"""Serializer edge cases: rejected records, timestamps in odd forms, required values."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from enum import Enum
from typing import Any

import numpy as np
import pytest

from sluicebox import ValidationError
from sluicebox._lineprotocol import Dialect
from sluicebox._serializer import Serializer
from sluicebox.config import MeasurementSchema, TagsConfig, ValidationConfig
from sluicebox.tags import TagInjector
from sluicebox.types import FieldType

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

    @pytest.mark.parametrize(
        ("version", "value", "stored"),
        [
            (2, 2**63 - 1, False),  # InfluxDB 2 fails the whole request for it
            (2, 2**63 - 2, True),
            (2, -(2**63) + 1, False),
            (2, -(2**63) + 2, True),
            (3, 2**63 - 1, True),
            (3, -(2**63), True),
        ],
    )
    def test_integer_times_at_the_servers_limits(self, version: int, value: int, stored: bool) -> None:
        record = {"measurement": "m", "fields": {"v": 1.0}, "time": value}
        if stored:
            assert serialize(make(version), record) == [f"m v=1.0 {value}"]
        else:
            assert rejected(make(version), record).code == "invalid_time"

    @pytest.mark.parametrize("version", [2, 3])
    def test_extreme_pandas_and_numpy_times_follow_the_server(self, version: int) -> None:
        import pandas as pd

        for value in (pd.Timestamp.max.tz_localize("UTC"), np.datetime64(2**63 - 1, "ns")):
            record = {"measurement": "m", "fields": {"v": 1.0}, "time": value}
            if version == 3:
                assert serialize(make(version), record) == [f"m v=1.0 {2**63 - 1}"]
            else:
                assert rejected(make(version), record).code == "invalid_time"

    def test_coarse_precisions_cannot_overflow_nanoseconds(self) -> None:
        # -9223372036854776 us is below -2**63 ns: the server could not store it.
        record = {"measurement": "m", "fields": {"v": 1.0}, "time": -9223372036854776}
        assert rejected(make(), record, precision="us").code == "invalid_time"
        record["time"] = -9223372036854775
        assert serialize(make(), record, precision="us") == ["m v=1.0 -9223372036854775"]

    def test_numpy_integers(self) -> None:
        assert serialize(make(), {"measurement": "m", "fields": {"v": 1.0}, "time": np.int64(T0)}) == [
            f"m v=1.0 {T0}"
        ]

    def test_seconds_with_nanosecond_precision_warn(self, caplog: Any) -> None:
        caplog.set_level(logging.WARNING, logger="sluicebox.validation")
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


UNPAIRED = "x\ud800y"
UNENCODABLE = [
    ({"measurement": UNPAIRED, "fields": {"v": 1.0}}, "invalid_name"),
    ({"measurement": "m", "tags": {UNPAIRED: "a"}, "fields": {"v": 1.0}}, "invalid_name"),
    ({"measurement": "m", "tags": {"t": UNPAIRED}, "fields": {"v": 1.0}}, "invalid_tag_value"),
    ({"measurement": "m", "fields": {UNPAIRED: 1.0}}, "invalid_name"),
    ({"measurement": "m", "fields": {"v": 1.0, "s": UNPAIRED}}, "invalid_encoding"),
    (f'm s="{UNPAIRED}"', "invalid_encoding"),
]


class TestUnencodableText:
    """A Python str can hold unpaired surrogates (surrogateescape, JSON, Windows APIs), which
    UTF-8 cannot encode: such records are invalid like any other, not an encoding crash."""

    @pytest.mark.parametrize(("record", "code"), UNENCODABLE)
    def test_rejected_with_a_validation_error(self, record: Any, code: str) -> None:
        assert rejected(make(), record).code == code

    @pytest.mark.parametrize(("record", "code"), UNENCODABLE)
    def test_dropped_in_drop_mode_and_lock_nothing(self, record: Any, code: str) -> None:
        serializer = make(on_invalid="drop")
        good = {"measurement": "m", "fields": {"w": 2.0}, "time": T0}
        chunk = serializer.serialize([record, good], database="db", precision="ns")
        assert (chunk.lines, chunk.dropped) == ([f"m w=2.0 {T0}"], 1)
        assert chunk.nbytes == len(chunk.lines[0]) + 1
        assert [e.code for e in chunk.rejected] == [code]
        assert serializer.locked_types("db", "m") == {"w": FieldType.FLOAT}

    def test_the_rejected_string_field_is_named(self) -> None:
        error = rejected(make(), {"measurement": "m", "fields": {"a": "ok", "b": UNPAIRED}})
        assert (error.key, error.measurement) == ("b", "m")

    def test_valid_non_ascii_text_is_counted_in_bytes(self) -> None:
        chunk = make().serialize(
            [{"measurement": "m", "tags": {"t": "é"}, "fields": {"s": "😀"}, "time": T0}],
            database="db",
            precision="ns",
        )
        assert chunk.nbytes == len(chunk.lines[0].encode()) + 1


class TestIntAsFloat:
    @pytest.mark.parametrize("coerce", [True, False])
    def test_integer_fields_stay_integer(self, coerce: bool) -> None:
        """int_as_float writes ints as floats unless the field is an integer field, declared
        or locked (e.g. by sync_schema()): there they are integers, without coercion."""
        declared = make(int_as_float=True, coerce=coerce, schemas={"m": {"fields": {"n": "integer"}}})
        assert serialize(declared, {"measurement": "m", "fields": {"n": 5, "x": 7}}) == ["m n=5i,x=7.0"]
        locked = make(int_as_float=True, coerce=coerce)
        locked.seed_types("db", "m", {"n": FieldType.INTEGER})
        assert serialize(locked, {"measurement": "m", "fields": {"n": np.int64(5)}}) == ["m n=5i"]


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
        caplog.set_level(logging.WARNING, logger="sluicebox.validation")
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
        import sluicebox._serializer as module

        monkeypatch.setattr(module, "_TAGSET_CACHE_LIMIT", 10)
        caplog.set_level(logging.WARNING, logger="sluicebox.validation")
        serializer = make()
        records = [
            {"measurement": "req", "tags": {"request_id": str(i)}, "fields": {"v": 1.0}} for i in range(100)
        ]
        serialize(serializer, *records)
        warnings = [r for r in caplog.records if "distinct tag sets" in r.getMessage()]
        assert len(warnings) == 1
        assert "'req'" in warnings[0].getMessage()

    def test_cardinality_warning_comes_after_the_documented_count(
        self, caplog: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import sluicebox._serializer as module

        monkeypatch.setattr(module, "_TAGSET_CACHE_LIMIT", 10)  # the README's 150,000 becomes 30
        caplog.set_level(logging.WARNING, logger="sluicebox.validation")

        def warned_after(count: int) -> bool:
            caplog.clear()
            records = [
                {"measurement": "req", "tags": {"id": str(i)}, "fields": {"v": 1.0}} for i in range(count)
            ]
            serialize(make(), *records)
            return any("over 30 distinct tag sets" in r.getMessage() for r in caplog.records)

        assert warned_after(31)
