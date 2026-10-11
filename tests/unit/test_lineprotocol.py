from __future__ import annotations

import random

import pytest

from sluicebox._lineprotocol import (
    Dialect,
    LineSyntaxError,
    UInt,
    _scan_line_end,
    line_end_problem,
    parse_line,
)

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


class TestSplitLines:
    @pytest.mark.parametrize("dialect", [V2, V3], ids=["v2", "v3"])
    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("m x=1 1\nm x=2 2\r\n# comment\n\nm x=3", ["m x=1 1", "m x=2 2", "m x=3"]),
            ('m s="two\nlines" 1\nm x=1', ['m s="two\nlines" 1', "m x=1"]),
            ('m s="a\r\nb"\r\nm x=1\r\n', ['m s="a\r\nb"', "m x=1"]),  # CR LF kept inside the value
            ('m s="q\\"\n\\\\" 1\nm x=1', ['m s="q\\"\n\\\\" 1', "m x=1"]),  # escaped quote and backslash
            # Quotes in tags and field keys are literal: only one right after "=" opens a string.
            ('m,t=q"uote x=1\nm k"ey=1,s="x\n" 2', ['m,t=q"uote x=1', 'm k"ey=1,s="x\n" 2']),
            ('# say "hi\nm s="x" 1\n#"', ['m s="x" 1']),  # quotes in comments open nothing
            ('m s="unterminated\nm x=1', ['m s="unterminated', "m x=1"]),  # invalid: newline ends it
            ('m s="one line" 1', ['m s="one line" 1']),
            ("", []),
        ],
    )
    def test_newlines_in_string_values_do_not_end_lines(
        self, dialect: Dialect, text: str, expected: list[str]
    ) -> None:
        assert dialect.split_lines(text) == expected

    @pytest.mark.parametrize("dialect", [V2, V3], ids=["v2", "v3"])
    def test_random_lines_round_trip(self, dialect: Dialect) -> None:
        rng = random.Random(4321)
        for _ in range(300):
            lines = []
            for _ in range(rng.randint(1, 6)):
                measurement = random_identifier(rng).lstrip("#") or "m"
                tag = f"{dialect.escape_key(random_identifier(rng))}={dialect.escape_key(random_identifier(rng))}"
                value = "".join(rng.choice([*ALPHABET, "\n", "\r\n"]) for _ in range(rng.randint(0, 10)))
                quoted = value.replace("\\", "\\\\").replace('"', '\\"')
                field = f'{dialect.escape_key(random_identifier(rng))}="{quoted}",n=1i'
                lines.append(
                    f"{dialect.escape_measurement(measurement)},{tag} {field}" + rng.choice(["", " 5"])
                )
            noise = rng.choice(["", '# a "comment\n', "\n"])
            text = noise + "\n".join(lines) + rng.choice(["", "\n", "\r\n"])
            assert dialect.split_lines(text) == lines, text


#: Lines and whether InfluxDB 2 reads the line after them into them (pinned to the server in
#: tests/integration/test_live.py, which writes each followed by another line).
RUN_ON = [
    ("m v=1i 1", False),
    ('m s="closed" 1', False),
    ('m s="two\nlines" 1', False),
    ('m s="left open 1', True),
    ('m a="x",b="y 1', True),  # the second value is left open
    ('m s="a\\" 1', True),  # an escaped quote does not close it
    ('m s="a\\\\" 1', False),  # an escaped backslash does not escape the quote
    ('m,t="x v=1i 1', False),  # quotes in tags are literal
    ('m a=1i,"b"=2i 1', False),  # a quote opens a value only where more "=" than "," came before
    ('m k"ey="v 1', True),
    ('m v=1i "x', True),  # ... also in the time
    ('m,t=x="y v=1i 1', False),
    ('  m,t=x="y v=1i 1', True),  # a leading space starts the fields early
    ('\tm s="y 1', True),
    ("m,t=a\\", True),  # a backslash escapes the line break
    ("m v=1i 5\\", True),
    ("m v=1i 5\\\\", False),
]


@pytest.mark.parametrize(("line", "runs_on"), RUN_ON)
def test_line_end_problem(line: str, runs_on: bool) -> None:
    assert (line_end_problem(line) is not None) is runs_on
    assert misread(line) is runs_on


def scan_lines(text: str) -> list[str]:
    """The lines of a request body as the servers read them (InfluxDB 2's models.scanLine,
    ported literally; InfluxDB 3 splits the same way).

    A newline inside a string field value does not end the line; a value left open runs on to
    the next quote, or the end of the request. Lines lose their leading whitespace.
    """
    lines = []
    i, n = 0, len(text)
    while i < n:
        start, quoted, fields, equals, commas = i, False, False, 0, 0
        while i < n:
            char = text[i]
            if char == "\\" and i + 2 < n:
                i += 2
                continue
            if char == " ":
                fields = True
            if fields and not quoted and char in "=,":
                equals += char == "="
                commas += char == ","
            elif fields and char == '"' and equals > commas:
                quoted = not quoted
            elif char == "\n" and not quoted:
                break
            i += 1
        line = text[start:i].lstrip(" \t\0")
        if line and line[0] != "#":
            lines.append(line)
        i += 1
    return lines


def misread(line: str) -> bool:
    """Whether the server, given ``line`` and another line, reads anything but those two lines.

    The "x" before it changes no quote and keeps leading whitespace and comments as they are.
    """
    after = "after v=1i 1"
    return scan_lines(f"x{line}\n{after}") != [f"x{line}", after]


def random_lines(rng: random.Random, count: int) -> list[str]:
    """Lines of random characters, and lines of random well- and ill-formed parts."""
    chars = ["m", "k", "1", "=", ",", '"', " ", "\\", "\t", "\n"]
    heads = ["m", "m\\ x", 'm"q', "m,t=a", 'm,t="a b', "m,t=a\\,b", "m,t=c:\\d", "  m", "#m", "m="]
    fields = [
        "v=1i",
        "f=1.5",
        's="x y"',
        's="a=b,c"',
        's=""',
        "v=1i",
        's="x y"',
        's="a\\"b"',
        's="a\\\\"',
        's="open',
        '"k"=1',
        "k=v=1",
        's="two\nlines"',
    ]
    fields += ['p="c:\\dir"', "f=1.5", "e=", 's=""', 'k"ey="v']
    times = ["", " 1", "", " 1", ' "x', " 5\\", " 5\\\\", "\\", " 1 2", ' 1="']
    lines = []
    for _ in range(count):
        if rng.random() < 0.3:
            lines.append("".join(rng.choice(chars) for _ in range(rng.randint(0, 14))))
        else:
            parts = rng.choices(fields, k=rng.randint(1, 4))
            lines.append(f"{rng.choice(heads)} {','.join(parts)}{rng.choice(times)}")
    return lines


def test_line_end_problem_reads_lines_as_the_server_does() -> None:
    for line in random_lines(random.Random(7), 20_000):
        assert (line_end_problem(line) is not None) is misread(line), line


def test_the_common_case_is_read_without_the_scan() -> None:
    """Lines without backslashes or newlines, with a space before the first quote, are read
    with str methods: with the same result as the scan of each character."""
    common = 0
    for line in random_lines(random.Random(8), 20_000):
        assert line_end_problem(line) == _scan_line_end(line), line
        space, quote = line.find(" "), line.find('"')
        common += "\\" not in line and "\n" not in line and 0 <= space < quote
    assert common > 1_500  # an eighth of these lines
