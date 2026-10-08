from __future__ import annotations

import random

import pytest

from sluicebox._lineprotocol import Dialect, LineSyntaxError, UInt, parse_line

V2 = Dialect.for_version(2)
V3 = Dialect.for_version(3)

# Characters that matter to line protocol, plus ordinary and non-ASCII ones.
ALPHABET = [*list("ab_-.:/\\,= \"'#"), "é", "😀", "\xa0"]


def random_identifier(rng: random.Random) -> str:
    while True:
        text = "".join(rng.choice(ALPHABET) for _ in range(rng.randint(1, 12)))
        if text[-1] != "\\":  # unrepresentable on both servers (validated elsewhere)
            return text


class TestEscaping:
    def test_v2_does_not_double_backslashes(self) -> None:
        assert V2.escape_key(r"c:\dir") == r"c:\dir"
        assert V2.escape_key("a b,c=d") == r"a\ b\,c\=d"
        assert V2.escape_measurement("m x,y=z") == r"m\ x\,y\=z"  # v2 unescapes "\=" in measurements

    def test_v3_doubles_backslashes(self) -> None:
        assert V3.escape_key(r"c:\dir") == r"c:\\dir"
        assert V3.escape_key("a b,c=d") == r"a\ b\,c\=d"
        assert V3.escape_measurement(r"m x\y=z") == r"m\ x\\y=z"

    @pytest.mark.parametrize(
        ("text", "problem"),
        [
            ("", "must not be empty"),
            ("ends\\", "backslash"),
            ("new\nline", "control character"),
            ("tab\there", "control character"),
            ("nul\x00", "control character"),
            ("del\x7f", "control character"),
        ],
    )
    def test_identifier_problems(self, text: str, problem: str) -> None:
        found = V3.identifier_problem(text)
        assert found is not None
        assert problem in found

    def test_measurement_must_not_start_with_hash(self) -> None:
        assert "comment" in (V2.measurement_problem("#m") or "")
        assert V2.measurement_problem("m#") is None

    def test_identifier_ok(self) -> None:
        assert V3.identifier_problem("ok \\ value, with=specials é") is None
        assert V3.identifier_problem("", allow_empty=True) is None


class TestParser:
    def test_basic(self) -> None:
        parsed = parse_line(V3, 'cpu,host=a,region=eu usage=0.5,cores=8i,ok=true,name="x y",u=3u 1700000000')
        assert parsed.measurement == "cpu"
        assert parsed.tags == {"host": "a", "region": "eu"}
        assert parsed.fields == {"usage": 0.5, "cores": 8, "ok": True, "name": "x y", "u": UInt(3)}
        assert isinstance(parsed.fields["u"], UInt)
        assert parsed.timestamp == 1700000000

    def test_no_tags_no_timestamp(self) -> None:
        parsed = parse_line(V2, "m f=1")
        assert parsed.tags == {}
        assert parsed.timestamp is None

    def test_quoted_string_with_specials(self) -> None:
        parsed = parse_line(V3, r'm s="a \"quoted\" , = \\ value",t=1i')
        assert parsed.fields == {"s": 'a "quoted" , = \\ value', "t": 1}

    @pytest.mark.parametrize(
        "line", ["", "m", "m,t=a", "m f", "m f=", 'm s="open', "m f=1 notanumber", "m f=bad"]
    )
    def test_syntax_errors(self, line: str) -> None:
        with pytest.raises(LineSyntaxError):
            parse_line(V3, line)

    def test_v2_backslash_semantics_match_server(self) -> None:
        # Verified against InfluxDB 2.9: "\\" is not an escape, "\," is.
        assert parse_line(V2, r"m,t=a\\b f=1").tags["t"] == r"a\\b"
        assert parse_line(V2, r"m,t=a\\,b f=1").tags["t"] == r"a\,b"

    def test_v3_backslash_semantics_match_server(self) -> None:
        # Verified against InfluxDB 3.12: "\\" is an escaped backslash.
        assert parse_line(V3, r"m,t=a\\b f=1").tags["t"] == r"a\b"
        assert parse_line(V3, r"m,t=a\\\,b f=1").tags["t"] == r"a\,b"


@pytest.mark.parametrize("dialect", [V2, V3], ids=["v2", "v3"])
def test_escape_round_trip(dialect: Dialect) -> None:
    """escape -> parse returns the original text for any representable identifier."""
    rng = random.Random(1234)
    for _ in range(3000):
        measurement = random_identifier(rng).lstrip("#") or "m"
        tag_key, tag_value, field_key = (random_identifier(rng) for _ in range(3))
        string_value = "".join(rng.choice([*ALPHABET, "\n"]) for _ in range(rng.randint(0, 10)))
        quoted = string_value.replace("\\", "\\\\").replace('"', '\\"')
        tag = f"{dialect.escape_key(tag_key)}={dialect.escape_key(tag_value)}"
        head = f"{dialect.escape_measurement(measurement)},{tag}"
        line = f'{head} {dialect.escape_key(field_key)}="{quoted}" 1'
        parsed = parse_line(dialect, line)
        assert parsed.measurement == measurement, line
        assert parsed.tags == {tag_key: tag_value}, line
        assert parsed.fields == {field_key: string_value}, line
