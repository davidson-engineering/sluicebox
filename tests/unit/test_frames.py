from __future__ import annotations

import math
import random
from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd
import pytest

from sluicebox import Point, ValidationError
from sluicebox._lineprotocol import Dialect, parse_line
from sluicebox._serializer import Serializer
from sluicebox.config import MeasurementSchema, TagRule, TagsConfig, ValidationConfig
from sluicebox.frames import to_line_chunks
from sluicebox.tags import TagInjector, tag_context

# polars publishes no free-threaded (3.14t) wheels; CI runs that build without it.
pl = pytest.importorskip("polars")

T0 = datetime(2024, 1, 1, tzinfo=UTC)


def make(
    version: int = 3,
    *,
    tags: TagsConfig | None = None,
    schemas: dict[str, Any] | None = None,
    **validation: Any,
):
    drops: list[tuple[ValidationError, int]] = []
    serializer = Serializer(
        dialect=Dialect.for_version(version),
        validation=ValidationConfig(**validation),
        schemas={k: MeasurementSchema.model_validate(v) for k, v in (schemas or {}).items()},
        injector=TagInjector(tags or TagsConfig()),
        auto_timestamp=True,
        on_drop=lambda e, n: drops.append((e, n)),
    )
    serializer.drops = drops  # type: ignore[attr-defined]
    return serializer


def frame_lines(
    serializer: Serializer, frame: Any, chunk_size: int = 1000, **kwargs: Any
) -> tuple[list[str], int]:
    lines: list[str] = []
    dropped = 0
    options = {"measurement": "m", "tag_columns": None, "field_columns": None, "time_column": None, **kwargs}
    for chunk in to_line_chunks(
        frame, serializer=serializer, database="db", precision="ns", chunk_size=chunk_size, **options
    ):
        assert chunk.nbytes == sum(len(line.encode()) + 1 for line in chunk.lines)
        assert len(chunk.rejected) == min(chunk.dropped, 1000)
        lines += chunk.lines
        dropped += chunk.dropped
    return lines, dropped


def parsed(lines: list[str], version: int = 3) -> list[tuple[Any, ...]]:
    dialect = Dialect.for_version(version)
    out = []
    for line in lines:
        p = parse_line(dialect, line)
        fields = {k: (round(v, 12) if isinstance(v, float) else v) for k, v in p.fields.items()}
        out.append((p.measurement, p.tags, fields, p.timestamp))
    return out


def random_frame(rng: random.Random, rows: int) -> pl.DataFrame:
    def maybe(value: Any) -> Any:
        return None if rng.random() < 0.15 else value

    specials = ["a b", "c,d", "e=f", "back\\slash", "plain", "é😀", ""]
    return pl.DataFrame(
        {
            "host": [maybe(rng.choice(specials)) for _ in range(rows)],
            "region": [rng.choice(["eu", "us"]) for _ in range(rows)],
            "f": [
                maybe(rng.choice([rng.uniform(-1e6, 1e6), 1e16, 1.5e-7, 0.1, -0.0, 3.0])) for _ in range(rows)
            ],
            "i": pl.Series([maybe(rng.randint(-(2**40), 2**40)) for _ in range(rows)], dtype=pl.Int64),
            "small": pl.Series([maybe(rng.randint(-100, 100)) for _ in range(rows)], dtype=pl.Int32),
            "u": pl.Series([maybe(rng.randint(0, 2**63)) for _ in range(rows)], dtype=pl.UInt64),
            "b": [maybe(rng.random() < 0.5) for _ in range(rows)],
            "s": [maybe(rng.choice(['q"uote', "back\\", "line\nbreak", "", "x" * 20])) for _ in range(rows)],
            "time": [T0 + timedelta(microseconds=rng.randint(0, 10**9)) for _ in range(rows)],
        }
    )


@pytest.mark.parametrize("version", [2, 3])
def test_vectorized_matches_row_serializer(version: int) -> None:
    """Property: for any frame, the polars path yields the same points as the row path."""
    rng = random.Random(7)
    frame = random_frame(rng, 400)
    vectorized, dropped = frame_lines(make(version), frame, chunk_size=64, tag_columns=["host", "region"])
    assert dropped == 0
    rows = [
        Point(
            "m",
            {"host": r["host"], "region": r["region"]},
            {k: r[k] for k in ("f", "i", "small", "u", "b", "s")},
            r["time"],
        )
        for r in frame.iter_rows(named=True)
    ]
    expected = make(version).serialize(rows, database="db", precision="ns").lines
    assert parsed(vectorized, version) == parsed(expected, version)


def test_fields_types_and_escaping() -> None:
    frame = pl.DataFrame(
        {
            "host": ["a b", "x"],
            "f": [1.5, float("nan")],
            "i": [1, None],
            "flag": [True, False],
            "s": ['say "hi"', None],
            "time": [T0, T0 + timedelta(seconds=1)],
        }
    )
    lines, _ = frame_lines(make(), frame, tag_columns=["host"])
    assert lines == [
        'm,host=a\\ b f=1.5,i=1i,flag=true,s="say \\"hi\\"" 1704067200000000000',
        "m,host=x flag=false 1704067201000000000",
    ]


def test_precision_and_integer_time() -> None:
    frame = pl.DataFrame({"v": [1.0], "time": [T0]})
    serializer = make()
    chunks = list(
        to_line_chunks(
            frame,
            serializer=serializer,
            database="db",
            precision="s",
            measurement="m",
            tag_columns=None,
            field_columns=None,
            time_column=None,
            chunk_size=10,
        )
    )
    assert chunks[0].lines == ["m v=1.0 1704067200"]
    ints, _ = frame_lines(make(), pl.DataFrame({"v": [1.0], "ts": [42]}), time_column="ts")
    assert ints == ["m v=1.0 42"]


def test_naive_time_column() -> None:
    frame = pl.DataFrame({"v": [1.0], "time": [datetime(2024, 1, 1)]})
    with pytest.raises(ValidationError, match="time zone") as info:
        frame_lines(make(), frame)
    assert info.value.code == "naive_datetime"
    lines, _ = frame_lines(make(naive_datetime="utc"), frame)
    assert lines == ["m v=1.0 1704067200000000000"]


def test_auto_timestamp_without_time_column() -> None:
    lines, _ = frame_lines(make(), pl.DataFrame({"v": [1.0, 2.0]}))
    stamps = {line.rsplit(" ", 1)[1] for line in lines}
    assert len(stamps) == 1  # one timestamp for the whole write


def test_injected_tags() -> None:
    serializer = make(tags=TagsConfig(static={"env": "prod", "host": "default"}))
    frame = pl.DataFrame({"host": ["a", None], "v": [1.0, 2.0], "time": [T0, T0]})
    with tag_context(request="r1"):
        lines, _ = frame_lines(serializer, frame, tag_columns=["host"])
    assert [line.split(" ")[0] for line in lines] == [
        "m,env=prod,host=a,request=r1",
        "m,env=prod,host=default,request=r1",
    ]


def test_type_conflict_against_locked_type() -> None:
    serializer = make()
    serializer.serialize([{"measurement": "m", "fields": {"v": 1}}], database="db", precision="ns")
    with pytest.raises(ValidationError) as info:
        frame_lines(serializer, pl.DataFrame({"v": [1.0, 2.5]}))
    assert info.value.code == "type_conflict"
    assert info.value.index == 1  # 1.0 is integral and coerces; 2.5 cannot
    integral, _ = frame_lines(serializer, pl.DataFrame({"v": [1.0, 2.0], "time": [T0, T0]}))
    assert [line.split(" ")[1] for line in integral] == ["v=1i", "v=2i"]


def test_drop_mode_filters_rows() -> None:
    serializer = make(on_invalid="drop", non_finite="error", schemas={"m": {"required_tags": ["host"]}})
    frame = pl.DataFrame(
        {
            "host": ["a", None, "b\\", "c"],
            "v": [1.0, 2.0, 3.0, float("inf")],
            "time": [T0] * 4,
        }
    )
    lines, dropped = frame_lines(serializer, frame, tag_columns=["host"])
    assert lines == ["m,host=a v=1.0 1704067200000000000"]
    assert dropped == 3
    assert {e.code for e, _ in serializer.drops} == {"missing_tag", "invalid_tag_value", "non_finite"}  # type: ignore[attr-defined]


def test_raise_mode_reports_first_bad_row() -> None:
    frame = pl.DataFrame({"host": ["a", "bad\n"], "v": [1.0, 2.0]})
    with pytest.raises(ValidationError) as info:
        frame_lines(make(), frame, tag_columns=["host"])
    assert (info.value.code, info.value.index, info.value.key) == ("invalid_tag_value", 1, "host")


def test_rows_with_no_fields_are_invalid() -> None:
    serializer = make(on_invalid="drop")
    lines, dropped = frame_lines(serializer, pl.DataFrame({"v": [1.0, None], "w": [None, None]}))
    assert len(lines) == 1
    assert dropped == 1


def test_rules_fall_back_to_row_path() -> None:
    rule = TagRule.model_validate({"when": {"fields": {"v": {"gt": 1}}}, "set": {"big": "yes"}})
    serializer = make(tags=TagsConfig(rules=[rule]))
    lines, _ = frame_lines(serializer, pl.DataFrame({"v": [1.0, 2.0], "time": [T0, T0]}))
    assert [line.split(" ")[0] for line in lines] == ["m", "m,big=yes"]


def test_pandas_with_datetime_index_and_missing_values() -> None:
    frame = pd.DataFrame(
        {"host": ["a", None], "v": [1.0, None], "n": pd.array([1, 2], dtype="Int64")},
        index=pd.DatetimeIndex([T0, T0 + timedelta(seconds=1)], name="ts"),
    )
    lines, _ = frame_lines(make(), frame, tag_columns=["host"])
    assert lines == ["m,host=a v=1.0,n=1i 1704067200000000000", "m n=2i 1704067201000000000"]


def test_pandas_without_polars_uses_row_path(monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib.util

    real = importlib.util.find_spec
    monkeypatch.setattr(
        importlib.util, "find_spec", lambda name, *a: None if name == "polars" else real(name, *a)
    )
    frame = pd.DataFrame(
        {"host": ["a", None], "v": [1.0, None], "n": pd.array([1, 2], dtype="Int64"), "time": [T0, pd.NaT]}
    )
    lines, _ = frame_lines(make(), frame, tag_columns=["host"])
    assert lines[0] == "m,host=a v=1.0,n=1i 1704067200000000000"
    # Missing tag and field values are omitted; a missing time (NaT) means "now", as for records.
    second, stamp = lines[1].rsplit(" ", 1)
    assert second == "m n=2i"
    assert int(stamp) > 1704067200000000000


def test_column_errors() -> None:
    from sluicebox import ConfigurationError

    frame = pl.DataFrame({"a": [1.0], "b": ["x"]})
    with pytest.raises(ConfigurationError, match="no columns"):
        frame_lines(make(), frame, tag_columns=["missing"])
    with pytest.raises(ConfigurationError, match="both"):
        frame_lines(make(), frame, tag_columns=["a"], field_columns=["a"])
    with pytest.raises(ConfigurationError, match="measurement"):
        frame_lines(make(), frame, measurement=None)
    with pytest.raises(ValidationError, match="dtype"):
        frame_lines(make(), pl.DataFrame({"v": [[1, 2]]}))


def test_lazy_frame_and_chunking() -> None:
    frame = pl.DataFrame({"v": [float(i) for i in range(25)], "time": [T0] * 25}).lazy()
    chunks = list(
        to_line_chunks(
            frame,
            serializer=make(),
            database="db",
            precision="ns",
            measurement="m",
            tag_columns=None,
            field_columns=None,
            time_column=None,
            chunk_size=10,
        )
    )
    assert [len(c.lines) for c in chunks] == [10, 10, 5]


def test_float_text_round_trips_exactly() -> None:
    rng = random.Random(3)
    values = [rng.uniform(-1e300, 1e300) for _ in range(200)] + [5e-324, 1.7976931348623157e308, 0.1, 1 / 3]
    lines, _ = frame_lines(make(), pl.DataFrame({"v": values, "time": [T0] * len(values)}))
    assert [parse_line(Dialect.for_version(3), line).fields["v"] for line in lines] == values
    assert all(math.isfinite(v) for v in values)


def test_tag_column_dtypes_match_row_path() -> None:
    frame = pl.DataFrame(
        {
            "flag": [True, None, False],
            "ratio": [0.5, float("nan"), None],
            "empty": pl.Series([None, None, None], dtype=pl.Null),
            "v": [1.0, 2.0, 3.0],
            "time": [T0, T0, T0],
        }
    )
    vectorized, _ = frame_lines(make(), frame, tag_columns=["flag", "ratio", "empty"])
    rows = [
        Point("m", {"flag": r["flag"], "ratio": r["ratio"], "empty": None}, {"v": r["v"]}, r["time"])
        for r in frame.iter_rows(named=True)
    ]
    assert vectorized == make().serialize(rows, database="db", precision="ns").lines
    assert vectorized[1].startswith("m v=2.0")  # null flag and NaN ratio are omitted


def test_infinite_tag_value_is_invalid() -> None:
    with pytest.raises(ValidationError) as info:
        frame_lines(
            make(), pl.DataFrame({"ratio": [1.0, float("inf")], "v": [1.0, 2.0]}), tag_columns=["ratio"]
        )
    assert (info.value.code, info.value.index) == ("invalid_tag_value", 1)


def test_null_time_without_auto_timestamp_omits_it() -> None:
    serializer = Serializer(
        dialect=Dialect.for_version(3),
        validation=ValidationConfig(),
        schemas={},
        injector=TagInjector(TagsConfig()),
        auto_timestamp=False,
    )
    lines, _ = frame_lines(serializer, pl.DataFrame({"v": [1.0, 2.0], "time": [T0, None]}))
    assert lines == ["m v=1.0 1704067200000000000", "m v=2.0"]


def test_dropped_rows_are_counted_once() -> None:
    serializer = make(on_invalid="drop", non_finite="error")
    frame = pl.DataFrame({"host": ["bad\n", "ok"], "v": [float("inf"), 1.0]})
    _, dropped = frame_lines(serializer, frame, tag_columns=["host"])
    assert dropped == 1
    assert sum(n for _, n in serializer.drops) == 1  # type: ignore[attr-defined]


class TestUsability:
    def test_dropped_rows_are_reported_with_their_row_index(self) -> None:
        serializer = make(on_invalid="drop")
        frame = pl.DataFrame({"v": [1.0, None, 2.0, float("inf")], "w": [None, None, 1.0, None]})
        chunks = list(
            to_line_chunks(
                frame,
                serializer=serializer,
                database="db",
                precision="ns",
                measurement="m",
                tag_columns=None,
                field_columns=None,
                time_column=None,
                chunk_size=2,
            )
        )
        rejected = [error for chunk in chunks for error in chunk.rejected]
        assert [(e.index, e.code) for e in rejected] == [(1, "no_fields"), (3, "no_fields")]

    def test_pandas_object_column_mixing_types_reports_the_bad_row(self) -> None:
        serializer = make()
        frame = pd.DataFrame({"temperature": [21.5, "hot", 22.0]}, dtype=object)
        with pytest.raises(ValidationError) as info:
            frame_lines(serializer, frame)
        assert info.value.index == 1
        assert info.value.key == "temperature"

    def test_pandas_naive_time_hint_uses_pandas(self) -> None:
        frame = pd.DataFrame({"time": pd.to_datetime(["2024-01-01"]), "v": [1.0]})
        with pytest.raises(ValidationError, match=r"tz_localize"):
            frame_lines(make(), frame)

    def test_a_datetime_timestamp_column_is_the_time(self) -> None:
        frame = pl.DataFrame({"timestamp": [T0], "v": [1.0]})
        assert frame_lines(make(), frame)[0] == ["m v=1.0 1704067200000000000"]
        ints = pl.DataFrame({"timestamp": [5], "v": [1.0]})  # an ordinary field
        assert frame_lines(make(), ints)[0][0].startswith("m timestamp=5i,v=1.0 ")

    def test_iso_string_time_column(self) -> None:
        frame = pl.DataFrame(
            {"time": ["2024-01-01T00:00:00Z", "2024-01-01T00:00:01.5+00:00"], "v": [1.0, 2.0]}
        )
        assert frame_lines(make(), frame)[0] == ["m v=1.0 1704067200000000000", "m v=2.0 1704067201500000000"]
        naive = pl.DataFrame({"time": ["2024-01-01T00:00:00"], "v": [1.0]})
        with pytest.raises(ValidationError) as info:
            frame_lines(make(), naive)
        assert info.value.code == "naive_datetime"

    def test_integer_times_are_range_checked(self, caplog: Any) -> None:
        caplog.set_level("WARNING", logger="sluicebox.validation")
        seconds = pl.DataFrame({"time": [1_704_067_200], "v": [1.0]})
        frame_lines(make(), seconds)  # ns precision: lands in 1970, so warn
        assert "precision='s'" in caplog.text
        too_big = pl.DataFrame({"time": [10**13], "v": [1.0]})
        with pytest.raises(ValidationError, match="outside the range"):
            list(
                to_line_chunks(
                    too_big,
                    serializer=make(),
                    database="db",
                    precision="s",
                    measurement="m",
                    tag_columns=None,
                    field_columns=None,
                    time_column=None,
                    chunk_size=10,
                )
            )

    def test_single_column_names_may_be_strings(self) -> None:
        frame = pl.DataFrame({"site": ["a"], "v": [1.0], "time": [T0]})
        assert frame_lines(make(), frame, tag_columns="site")[0] == ["m,site=a v=1.0 1704067200000000000"]
