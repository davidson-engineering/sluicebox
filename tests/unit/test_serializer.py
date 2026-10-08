from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum, IntEnum, StrEnum
from typing import Annotated, Any, ClassVar

import numpy as np
import pandas as pd
import pytest
from pydantic import BaseModel

from influxkit import Point, Tag, Timestamp, UInt, ValidationError, measurement
from influxkit._lineprotocol import Dialect
from influxkit._serializer import Serializer
from influxkit.config import MeasurementSchema, TagRule, TagsConfig, ValidationConfig
from influxkit.tags import TagInjector, tag_context
from influxkit.types import FieldType

T0 = 1_700_000_000_000_000_000


def make(
    version: int = 3,
    *,
    schemas: dict[str, Any] | None = None,
    tags: TagsConfig | None = None,
    **validation: Any,
):
    drops: list[tuple[ValidationError, int]] = []
    serializer = Serializer(
        dialect=Dialect.for_version(version),
        validation=ValidationConfig(**validation),
        schemas={name: MeasurementSchema.model_validate(s) for name, s in (schemas or {}).items()},
        injector=TagInjector(tags or TagsConfig()),
        auto_timestamp=False,
        on_drop=lambda error, count: drops.append((error, count)),
    )
    serializer.drops = drops  # type: ignore[attr-defined]
    return serializer


def lines(serializer: Serializer, *records: Any, precision: str = "ns", database: str = "db") -> list[str]:
    return serializer.serialize(list(records), database=database, precision=precision).lines


def one(serializer: Serializer, record: Any, **kwargs: Any) -> str:
    out = lines(serializer, record, **kwargs)
    assert len(out) == 1
    return out[0]


def error_of(serializer: Serializer, *records: Any) -> ValidationError:
    with pytest.raises(ValidationError) as info:
        lines(serializer, *records)
    return info.value


class TestFormats:
    def test_dict_point_and_raw_line(self) -> None:
        s = make()
        out = lines(
            s,
            {"measurement": "cpu", "tags": {"host": "a"}, "fields": {"v": 1.5}, "time": T0},
            Point("cpu").tag("host", "b").field("v", 2.0).time(T0 + 1),
            "cpu,host=c v=3 5",
            b"cpu,host=d v=4 6\ncpu,host=e v=5 7\r\n# comment\n\n",
        )
        assert out == [
            f"cpu,host=a v=1.5 {T0}",
            f"cpu,host=b v=2.0 {T0 + 1}",
            "cpu,host=c v=3 5",
            "cpu,host=d v=4 6",
            "cpu,host=e v=5 7",
        ]

    def test_field_types(self) -> None:
        line = one(
            make(),
            {
                "measurement": "m",
                "fields": {
                    "f": 0.1,
                    "i": -3,
                    "b": False,
                    "s": 'say "hi" \\ there',
                    "u": UInt(7),
                    "big": 1e300,
                },
                "time": 1,
            },
        )
        assert line == 'm f=0.1,i=-3i,b=false,s="say \\"hi\\" \\\\ there",u=7u,big=1e+300 1'

    def test_tags_sorted_and_empty_omitted(self) -> None:
        line = one(
            make(),
            {"measurement": "m", "tags": {"z": "1", "a": "2", "e": "", "n": None}, "fields": {"v": 1.0}},
        )
        assert line == "m,a=2,z=1 v=1.0"

    def test_non_string_tag_values(self) -> None:
        class Color(StrEnum):
            RED = "red"

        line = one(
            make(),
            {"measurement": "m", "tags": {"b": True, "c": Color.RED, "i": 42, "x": 1.5}, "fields": {"v": 1}},
        )
        assert line == "m,b=true,c=red,i=42,x=1.5 v=1i"

    def test_numpy_and_enum_fields(self) -> None:
        class Level(IntEnum):
            HIGH = 3

        line = one(
            make(),
            {
                "measurement": "m",
                "fields": {
                    "f": np.float32(1.5),
                    "i": np.int64(2),
                    "b": np.bool_(True),
                    "e": Level.HIGH,
                    "d": Decimal("2.5"),
                },
            },
        )
        assert line == "m f=1.5,i=2i,b=true,e=3i,d=2.5"

    def test_escaping_v2_vs_v3(self) -> None:
        record = {"measurement": "m x", "tags": {"path": r"c:\dir x"}, "fields": {"f k": 1.0}, "time": 1}
        assert one(make(2), record) == r"m\ x,path=c:\dir\ x f\ k=1.0 1"
        assert one(make(3), record) == r"m\ x,path=c:\\dir\ x f\ k=1.0 1"

    def test_mapping_like_records(self) -> None:
        from types import MappingProxyType

        record = MappingProxyType({"measurement": "m", "fields": MappingProxyType({"v": 1.0})})
        assert one(make(), record) == "m v=1.0"


class TestTimestamps:
    @pytest.mark.parametrize(
        ("value", "precision", "expected"),
        [
            (datetime(2024, 1, 1, tzinfo=UTC), "ns", 1704067200000000000),
            (datetime(2024, 1, 1, 0, 0, 0, 123456, tzinfo=UTC), "us", 1704067200123456),
            (datetime(2024, 1, 1, 2, tzinfo=timezone(timedelta(hours=2))), "s", 1704067200),
            ("2024-01-01T00:00:00.123456789Z", "ns", 1704067200123456789),
            ("2024-01-01T01:00:00+01:00", "ms", 1704067200000),
            (1704067200.5, "ms", 1704067200500),
            (pd.Timestamp("2024-01-01T00:00:00.000000001Z"), "ns", 1704067200000000001),
            (np.datetime64("2024-01-01T00:00:00.000000002"), "ns", 1704067200000000002),
            (123, "s", 123),
        ],
    )
    def test_conversions(self, value: Any, precision: str, expected: int) -> None:
        line = one(make(), {"measurement": "m", "fields": {"v": 1.0}, "time": value}, precision=precision)
        assert line == f"m v=1.0 {expected}"

    def test_naive_datetime_rejected_by_default(self) -> None:
        err = error_of(make(), {"measurement": "m", "fields": {"v": 1.0}, "time": datetime(2024, 1, 1)})
        assert err.code == "naive_datetime"
        err = error_of(make(), {"measurement": "m", "fields": {"v": 1.0}, "time": "2024-01-01T00:00:00"})
        assert err.code == "naive_datetime"

    def test_naive_datetime_as_utc(self) -> None:
        line = one(
            make(naive_datetime="utc"),
            {"measurement": "m", "fields": {"v": 1.0}, "time": datetime(2024, 1, 1)},
        )
        assert line.endswith(" 1704067200000000000")

    @pytest.mark.parametrize("value", [date(2024, 1, 1), "yesterday", float("nan"), 2**63, [1]])
    def test_invalid(self, value: Any) -> None:
        assert (
            error_of(make(), {"measurement": "m", "fields": {"v": 1.0}, "time": value}).code == "invalid_time"
        )

    def test_auto_timestamp_is_shared_by_one_call(self) -> None:
        s = Serializer(
            dialect=Dialect.for_version(3),
            validation=ValidationConfig(),
            schemas={},
            injector=TagInjector(TagsConfig()),
            auto_timestamp=True,
        )
        out = s.serialize(
            [{"measurement": "m", "fields": {"v": 1.0}}] * 3,
            database="db",
            precision="s",
            now_ns=5_000_000_000,
        ).lines
        assert out == ["m v=1.0 5"] * 3


class TestTypeLocking:
    def test_int_then_float_conflicts_with_hint(self) -> None:
        s = make()
        lines(s, {"measurement": "m", "fields": {"v": 1}})
        err = error_of(s, {"measurement": "m", "fields": {"v": 1.5}})
        assert err.code == "type_conflict"
        assert err.key == "v"
        assert "int_as_float" in str(err)

    def test_float_then_int_is_coerced(self) -> None:
        s = make()
        lines(s, {"measurement": "m", "fields": {"v": 1.5}})
        assert one(s, {"measurement": "m", "fields": {"v": 2}}) == "m v=2.0"

    def test_coercion_disabled(self) -> None:
        s = make(coerce=False)
        lines(s, {"measurement": "m", "fields": {"v": 1.5}})
        assert error_of(s, {"measurement": "m", "fields": {"v": 2}}).code == "type_conflict"

    def test_bool_never_becomes_number(self) -> None:
        s = make()
        lines(s, {"measurement": "m", "fields": {"v": 1.5}})
        assert error_of(s, {"measurement": "m", "fields": {"v": True}}).code == "type_conflict"

    def test_int_as_float(self) -> None:
        s = make(int_as_float=True)
        assert one(s, {"measurement": "m", "fields": {"v": 1}}) == "m v=1.0"
        assert one(s, {"measurement": "m", "fields": {"v": 2.5}}) == "m v=2.5"

    def test_locks_are_per_database_and_measurement(self) -> None:
        s = make()
        lines(s, {"measurement": "m", "fields": {"v": 1}})
        assert one(s, {"measurement": "m", "fields": {"v": "x"}}, database="other") == 'm v="x"'
        assert one(s, {"measurement": "n", "fields": {"v": "x"}}) == 'n v="x"'
        assert s.locked_types("db", "m") == {"v": FieldType.INTEGER}

    def test_lock_disabled_allows_anything(self) -> None:
        s = make(type_lock=False)
        assert lines(
            s, {"measurement": "m", "fields": {"v": 1}}, {"measurement": "m", "fields": {"v": "x"}}
        ) == [
            "m v=1i",
            'm v="x"',
        ]

    def test_seeded_types(self) -> None:
        s = make()
        s.seed_types("db", "m", {"v": FieldType.FLOAT})
        assert one(s, {"measurement": "m", "fields": {"v": 3}}) == "m v=3.0"

    def test_concurrent_first_sightings_agree(self) -> None:
        """Two threads racing to lock a new field cannot both win with different types."""
        for attempt in range(20):
            s = make(on_invalid="drop")
            barrier = threading.Barrier(2)
            results: list[list[str]] = []

            def write(
                value: Any,
                s: Serializer = s,
                barrier: threading.Barrier = barrier,
                results: list[list[str]] = results,
            ) -> None:
                barrier.wait()
                results.append(
                    s.serialize(
                        [{"measurement": "m", "fields": {"v": value}}], database="db", precision="ns"
                    ).lines
                )

            threads = [threading.Thread(target=write, args=(v,)) for v in (1, "x")]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            written = [line for chunk in results for line in chunk]
            assert len(written) == 1, (attempt, written)


class TestValidation:
    @pytest.mark.parametrize(
        ("record", "code"),
        [
            ({"measurement": "", "fields": {"v": 1}}, "invalid_name"),
            ({"measurement": "#m", "fields": {"v": 1}}, "invalid_name"),
            ({"measurement": 5, "fields": {"v": 1}}, "invalid_name"),
            ({"measurement": "m\\", "fields": {"v": 1}}, "invalid_name"),
            ({"measurement": "m", "tags": {"t": "a\\"}, "fields": {"v": 1}}, "invalid_tag_value"),
            ({"measurement": "m", "tags": {"t": "a\nb"}, "fields": {"v": 1}}, "invalid_tag_value"),
            ({"measurement": "m", "tags": {"t": [1]}, "fields": {"v": 1}}, "invalid_tag_value"),
            ({"measurement": "m", "tags": {"time": "x"}, "fields": {"v": 1}}, "reserved_name"),
            ({"measurement": "m", "tags": {1: "x"}, "fields": {"v": 1}}, "invalid_name"),
            ({"measurement": "m", "tags": {"x": "1"}, "fields": {"x": 1}}, "tag_field_conflict"),
            ({"measurement": "m", "fields": {}}, "no_fields"),
            ({"measurement": "m", "fields": {"v": None, "w": float("nan")}}, "no_fields"),
            ({"measurement": "m", "fields": {"time": 1}}, "reserved_name"),
            ({"measurement": "m", "fields": {"": 1}}, "invalid_name"),
            ({"measurement": "m", "fields": {"v": 2**63}}, "out_of_range"),
            ({"measurement": "m", "fields": {"v": [1, 2]}}, "unsupported_type"),
            ({"measurement": "m", "fields": {"v": b"bytes"}}, "unsupported_type"),
            ({"measurement": "m", "fields": [("v", 1)]}, "malformed_record"),
            ({"measurement": "m"}, "malformed_record"),
            ({"measurement": "m", "field": {"v": 1}}, "malformed_record"),
            (object(), "unsupported_record"),
            (b"\xff\xfe", "invalid_encoding"),
        ],
    )
    def test_invalid_records(self, record: Any, code: str) -> None:
        err = error_of(make(), record)
        assert err.code == code
        assert err.index == 0

    def test_v2_allows_tag_field_overlap_and_tabs(self) -> None:
        # InfluxDB 2 accepts both; InfluxDB 3 does not.
        assert one(make(2), {"measurement": "m", "tags": {"x": "1"}, "fields": {"x": 1}}) == "m,x=1 x=1i"
        assert error_of(make(3), {"measurement": "m", "tags": {"x": "a\tb"}, "fields": {"v": 1}})

    def test_overlap_across_points_on_v3(self) -> None:
        s = make(3)
        lines(s, {"measurement": "m", "tags": {"x": "1"}, "fields": {"v": 1}})
        assert error_of(s, {"measurement": "m", "fields": {"x": 1}}).code == "tag_field_conflict"
        s = make(3)
        lines(s, {"measurement": "m", "fields": {"x": 1}})
        assert (
            error_of(s, {"measurement": "m", "tags": {"x": "1"}, "fields": {"v": 1}}).code
            == "tag_field_conflict"
        )

    def test_non_finite(self) -> None:
        assert one(make(), {"measurement": "m", "fields": {"v": float("inf"), "w": 1.0}}) == "m w=1.0"
        assert (
            error_of(make(non_finite="error"), {"measurement": "m", "fields": {"v": float("nan")}}).code
            == "non_finite"
        )

    def test_string_length(self) -> None:
        s = make(max_string_bytes=10)
        assert one(s, {"measurement": "m", "fields": {"s": "x" * 10}}) == f'm s="{"x" * 10}"'
        assert error_of(s, {"measurement": "m", "fields": {"s": "é" * 6}}).code == "string_too_long"

    def test_error_index_points_at_the_record(self) -> None:
        err = error_of(
            make(),
            {"measurement": "m", "fields": {"v": 1}},
            {"measurement": "m", "fields": {"v": 2}},
            {"measurement": "m"},
        )
        assert err.index == 2

    def test_drop_mode_reports_and_continues(self) -> None:
        s = make(on_invalid="drop")
        chunk = s.serialize(
            [
                {"measurement": "m", "fields": {"v": 1}},
                {"measurement": "m", "fields": {"v": "x"}},
                {"measurement": "m"},
            ],
            database="db",
            precision="ns",
        )
        assert chunk.lines == ["m v=1i"]
        assert chunk.dropped == 2
        assert [(e.code, n) for e, n in s.drops] == [("type_conflict", 1), ("malformed_record", 1)]  # type: ignore[attr-defined]

    def test_raw_lines_validate_mode(self) -> None:
        s = make(raw_lines="validate", tags=TagsConfig(static={"env": "test"}))
        assert lines(s, "m,host=a v=1i 5") == ["m,env=test,host=a v=1i 5"]
        assert error_of(s, "m,host=a v=1.5 6").code == "type_conflict"
        assert error_of(s, "garbage").code == "invalid_line"

    def test_unknown_measurements_rejected(self) -> None:
        s = make(unknown_measurements="reject", schemas={"cpu": {"fields": {"v": "float"}}})
        assert one(s, {"measurement": "cpu", "fields": {"v": 1.0}}) == "cpu v=1.0"
        assert error_of(s, {"measurement": "mem", "fields": {"v": 1.0}}).code == "unknown_measurement"


class TestSchemas:
    SCHEMA: ClassVar[dict[str, Any]] = {
        "cpu": {
            "fields": {"usage": "float", "cores": "int", "state": "str", "up": "bool", "n": "uint"},
            "tags": ["host", "region"],
            "required_tags": ["host"],
            "required_fields": ["usage"],
            "extra_fields": "forbid",
        }
    }

    def ok(self, **fields: Any) -> dict[str, Any]:
        return {"measurement": "cpu", "tags": {"host": "a"}, "fields": {"usage": 1.0, **fields}, "time": 1}

    def test_declared_types_coerce(self) -> None:
        s = make(schemas=self.SCHEMA)
        assert one(s, self.ok(cores=4.0, n=3)) == "cpu,host=a usage=1.0,cores=4i,n=3u 1"
        assert one(s, {**self.ok(), "fields": {"usage": 2}}) == "cpu,host=a usage=2.0 1"

    @pytest.mark.parametrize(
        ("record", "code"),
        [
            ({"measurement": "cpu", "fields": {"usage": 1.0}}, "missing_tag"),
            (
                {"measurement": "cpu", "tags": {"host": "a", "zone": "z"}, "fields": {"usage": 1.0}},
                "unexpected_tag",
            ),
            (
                {"measurement": "cpu", "tags": {"host": "a"}, "fields": {"usage": 1.0, "other": 1}},
                "unexpected_field",
            ),
            ({"measurement": "cpu", "tags": {"host": "a"}, "fields": {"cores": 1}}, "missing_field"),
            (
                {"measurement": "cpu", "tags": {"host": "a"}, "fields": {"usage": 1.0, "cores": 1.5}},
                "type_conflict",
            ),
            (
                {"measurement": "cpu", "tags": {"host": "a"}, "fields": {"usage": 1.0, "n": -1}},
                "out_of_range",
            ),
            ({"measurement": "cpu", "tags": {"host": "a"}, "fields": {"usage": "high"}}, "type_conflict"),
        ],
    )
    def test_violations(self, record: dict[str, Any], code: str) -> None:
        assert error_of(make(schemas=self.SCHEMA), record).code == code

    def test_cached_tag_sets_still_enforce_required_tags(self) -> None:
        s = make(schemas=self.SCHEMA)
        for _ in range(3):
            one(s, self.ok())
        assert (
            error_of(s, {"measurement": "cpu", "tags": {"region": "eu"}, "fields": {"usage": 1.0}}).code
            == "missing_tag"
        )


class TestTagInjection:
    def test_static_tags_keep_point_values(self) -> None:
        s = make(tags=TagsConfig(static={"env": "prod", "host": "default"}))
        assert (
            one(s, {"measurement": "m", "tags": {"host": "a"}, "fields": {"v": 1}})
            == "m,env=prod,host=a v=1i"
        )
        assert (
            one(s, {"measurement": "m", "tags": {"host": ""}, "fields": {"v": 1}})
            == "m,env=prod,host=default v=1i"
        )
        assert one(s, {"measurement": "m", "fields": {"v": 1}}) == "m,env=prod,host=default v=1i"

    def test_overwrite_policy(self) -> None:
        s = make(tags=TagsConfig(static={"env": "prod"}, on_conflict="overwrite"))
        assert one(s, {"measurement": "m", "tags": {"env": "dev"}, "fields": {"v": 1}}) == "m,env=prod v=1i"

    def test_error_policy(self) -> None:
        s = make(tags=TagsConfig(static={"env": "prod"}, on_conflict="error"))
        assert one(s, {"measurement": "m", "tags": {"env": "prod"}, "fields": {"v": 1}}) == "m,env=prod v=1i"
        assert (
            error_of(s, {"measurement": "m", "tags": {"env": "dev"}, "fields": {"v": 1}}).code
            == "tag_conflict"
        )

    def test_context_tags_beat_static_and_nest(self) -> None:
        s = make(tags=TagsConfig(static={"env": "prod", "app": "x"}))
        with tag_context(env="ctx", request="r1"):
            with tag_context(request="r2"):
                assert one(s, {"measurement": "m", "fields": {"v": 1}}) == "m,app=x,env=ctx,request=r2 v=1i"
            assert one(s, {"measurement": "m", "fields": {"v": 1}}) == "m,app=x,env=ctx,request=r1 v=1i"
        assert one(s, {"measurement": "m", "fields": {"v": 1}}) == "m,app=x,env=prod v=1i"

    def test_static_tag_cache_does_not_leak_context(self) -> None:
        s = make(tags=TagsConfig(static={"env": "prod"}))
        assert one(s, {"measurement": "m", "tags": {"h": "a"}, "fields": {"v": 1}}) == "m,env=prod,h=a v=1i"
        with tag_context(env="ctx"):
            assert (
                one(s, {"measurement": "m", "tags": {"h": "a"}, "fields": {"v": 1}}) == "m,env=ctx,h=a v=1i"
            )
        assert one(s, {"measurement": "m", "tags": {"h": "a"}, "fields": {"v": 1}}) == "m,env=prod,h=a v=1i"

    def test_content_rules(self) -> None:
        rules = [
            TagRule.model_validate(
                {
                    "name": "site",
                    "when": {
                        "measurement": "^sensor",
                        "tags": {"device": r"^(?P<site>[a-z]+)-(?P<rack>\d+)$"},
                    },
                    "set": {"site": "{site}", "rack": "{rack}", "kind": "{measurement}"},
                }
            ),
            TagRule.model_validate({"when": {"fields": {"temp": {"gt": 80}}}, "set": {"alert": "hot"}}),
            TagRule.model_validate(
                {"when": {"has_tags": ["site"], "missing_tags": ["zone"]}, "set": {"zone": "z-{tags[site]}"}}
            ),
            TagRule.model_validate(
                {"when": {"fields": {"state": {"in": ["down", "error"]}}}, "set": {"healthy": "no"}}
            ),
        ]
        s = make(tags=TagsConfig(rules=rules))
        out = lines(
            s,
            {"measurement": "sensor_t", "tags": {"device": "lon-42"}, "fields": {"temp": 90.0}, "time": 1},
            {"measurement": "sensor_t", "tags": {"device": "lon-42"}, "fields": {"temp": 20.0}, "time": 2},
            {"measurement": "sensor_t", "tags": {"device": "nope"}, "fields": {"temp": 99}, "time": 3},
            {"measurement": "other", "tags": {"device": "par-1"}, "fields": {"state": "down"}, "time": 4},
        )
        assert out == [
            "sensor_t,alert=hot,device=lon-42,kind=sensor_t,rack=42,site=lon,zone=z-lon temp=90.0 1",
            "sensor_t,device=lon-42,kind=sensor_t,rack=42,site=lon,zone=z-lon temp=20.0 2",
            "sensor_t,alert=hot,device=nope temp=99.0 3",  # temp is locked as float: 99 is coerced
            'other,device=par-1,healthy=no state="down" 4',
        ]

    def test_enrichers(self) -> None:
        def unit(measurement: str, tags: Any, fields: Any) -> dict[str, str] | None:
            return {"unit": "celsius"} if "temp" in fields else None

        s = Serializer(
            dialect=Dialect.for_version(3),
            validation=ValidationConfig(),
            schemas={},
            injector=TagInjector(TagsConfig(), enrichers=[unit]),
            auto_timestamp=False,
        )
        assert lines(
            s, {"measurement": "m", "fields": {"temp": 1.0}}, {"measurement": "m", "fields": {"v": 1.0}}
        ) == [
            "m,unit=celsius temp=1.0",
            "m v=1.0",
        ]

    def test_from_env(self) -> None:
        injector = TagInjector(TagsConfig(from_env={"region": "REGION"}), environ={"REGION": "eu-west"})
        assert injector.static == {"region": "eu-west"}
        from influxkit import ConfigurationError

        with pytest.raises(ConfigurationError, match="REGION"):
            TagInjector(TagsConfig(from_env={"region": "REGION"}), environ={})


class TestModels:
    def test_dataclass_model(self) -> None:
        @measurement("cpu")
        @dataclass
        class Cpu:
            host: Annotated[str, Tag]
            usage: float
            cores: int
            time: datetime

        s = make()
        line = one(s, Cpu("web 1", 0.5, 8, datetime(2024, 1, 1, tzinfo=UTC)))
        assert line == "cpu,host=web\\ 1 usage=0.5,cores=8i 1704067200000000000"
        # Annotations declare the field types: an int for a float field is coerced.
        assert one(s, Cpu("a", 1, 2, datetime(2024, 1, 1, tzinfo=UTC))).startswith(
            "cpu,host=a usage=1.0,cores=2i"
        )
        assert s.locked_types("db", "cpu") == {"usage": FieldType.FLOAT, "cores": FieldType.INTEGER}

    def test_pydantic_model(self) -> None:
        class State(Enum):
            OK = "ok"

        @measurement(tags=("region",))
        class Reading(BaseModel):
            region: str
            sensor: Annotated[str, Tag(name="sensor_id")]
            value: float | None
            state: str
            stamp: Annotated[int, Timestamp]

        line = one(make(), Reading(region="eu", sensor="s1", value=None, state="ok", stamp=5))
        assert line == 'Reading,region=eu,sensor_id=s1 state="ok" 5'

    def test_undecorated_dataclass_hint(self) -> None:
        @dataclass
        class Plain:
            v: float

        err = error_of(make(), Plain(1.0))
        assert err.code == "unsupported_record"
        assert "@influxkit.measurement" in str(err)


class TestUntimedOverwrites:
    def serializer(self) -> Serializer:
        return Serializer(
            dialect=Dialect.for_version(3),
            validation=ValidationConfig(),
            schemas={},
            injector=TagInjector(TagsConfig()),
            auto_timestamp=True,
        )

    def test_warns_when_untimed_points_collide(self, caplog: Any) -> None:
        caplog.set_level("WARNING", logger="influxkit.validation")
        self.serializer().serialize(
            [{"measurement": "m", "fields": {"v": float(i)}} for i in range(5)], database="db", precision="ns"
        )
        assert "4 points without a timestamp" in caplog.text

    def test_no_warning_for_distinct_series_or_merged_fields(self, caplog: Any) -> None:
        caplog.set_level("WARNING", logger="influxkit.validation")
        s = self.serializer()
        s.serialize(
            [
                {"measurement": "m", "tags": {"h": "a"}, "fields": {"v": 1.0}},
                {"measurement": "m", "tags": {"h": "b"}, "fields": {"v": 1.0}},
                {"measurement": "m", "tags": {"h": "a"}, "fields": {"w": 1.0}},  # merges with the first
                {"measurement": "m", "fields": {"v": 1.0}, "time": 5},
                {"measurement": "m", "fields": {"v": 2.0}, "time": 6},
            ],
            database="db",
            precision="ns",
        )
        assert "without a timestamp" not in caplog.text


def test_rule_templates_treat_none_as_absent() -> None:
    rule = TagRule.model_validate({"when": {"has_tags": ["host"]}, "set": {"zone": "z-{tags[site]}"}})
    s = make(tags=TagsConfig(rules=[rule]))
    assert (
        one(s, {"measurement": "m", "tags": {"host": "a", "site": None}, "fields": {"v": 1}})
        == "m,host=a v=1i"
    )
    assert (
        one(s, {"measurement": "m", "tags": {"host": "a", "site": "s1"}, "fields": {"v": 1}})
        == "m,host=a,site=s1,zone=z-s1 v=1i"
    )
