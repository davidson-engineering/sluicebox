"""Line protocol dialects: escaping, identifier rules and a parser.

InfluxDB 2 and 3 disagree on backslashes (verified against InfluxDB 2.9 and 3.12):

* InfluxDB 2 only unescapes ``\\,``, ``\\=`` and ``\\ `` (in measurements too); every
  other backslash is literal, so backslashes must *not* be doubled. Escaping the special
  character that follows a literal backslash keeps that backslash.
* InfluxDB 3 also unescapes ``\\\\`` to ``\\``; a literal backslash before a special
  character is only representable by doubling it.
* Both reject names and tag values ending in a backslash, and line endings other than ``\\n``.
* A line starting with ``#`` is a comment: InfluxDB 2 silently drops a point whose
  measurement starts with ``#`` (escaping does not help), so such names are invalid.
* InfluxDB 2 accepts measurement names containing ``=`` but its storage engine cannot
  find their data again (tag/field keys and values with ``=`` are fine): rejected for v2.
* InfluxDB 3 rejects tabs in names and tag values; neither can store a newline there.

String field values escape ``"`` and ``\\`` identically on both.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Literal

__all__ = ["Dialect", "ParsedLine", "UInt", "parse_line"]

_CONTROL = re.compile(r"[\x00-\x1f\x7f]")
STRING_ESCAPES = str.maketrans({'"': '\\"', "\\": "\\\\"})


class UInt(int):
    """Marks an integer to be written as an unsigned (``u``) field.

    A measurement schema declaring the field ``uinteger`` has the same effect for plain ints.
    """

    __slots__ = ()

    def __repr__(self) -> str:
        return f"UInt({int(self)})"


@dataclass(frozen=True, slots=True)
class Dialect:
    """Escaping and naming rules of one server generation."""

    version: Literal[2, 3]
    measurement_escapes: dict[int, str]
    key_escapes: dict[int, str]
    reserved_tag_keys: frozenset[str]
    reserved_field_keys: frozenset[str]
    forbid_tag_field_overlap: bool
    measurement_specials: str

    @classmethod
    def for_version(cls, version: int) -> Dialect:
        return _V3 if version == 3 else _V2

    def escape_measurement(self, name: str) -> str:
        return name.translate(self.measurement_escapes)

    def escape_key(self, text: str) -> str:
        return text.translate(self.key_escapes)

    def measurement_problem(self, name: str) -> str | None:
        """Return why ``name`` cannot be a measurement name, else None."""
        if name[:1] == "#":
            return "must not start with '#' (the line would be read as a comment and dropped)"
        if self.version == 2 and "=" in name:
            return "must not contain '=' on InfluxDB 2 (it is stored but can never be queried back)"
        return self.identifier_problem(name)

    def identifier_problem(self, text: str, *, allow_empty: bool = False) -> str | None:
        """Return why ``text`` cannot be a measurement, tag key/value or field key, else None."""
        if not text:
            return None if allow_empty else "must not be empty"
        if text[-1] == "\\":
            return "must not end with a backslash (InfluxDB rejects it)"
        bad = _CONTROL.search(text)
        if bad is not None:
            return f"must not contain control character {bad.group()!r}"
        return None

    # -- parsing ---------------------------------------------------------------------------

    def _unescape(self, text: str, specials: str) -> str:
        if "\\" not in text:
            return text
        out = []
        i = 0
        n = len(text)
        while i < n:
            char = text[i]
            if char == "\\" and i + 1 < n:
                nxt = text[i + 1]
                if nxt in specials or (self.version == 3 and nxt == "\\"):
                    out.append(nxt)
                    i += 2
                    continue
            out.append(char)
            i += 1
        return "".join(out)

    def _scan(self, text: str, start: int, stops: str) -> int:
        """Index of the first unescaped character of ``stops`` at or after ``start`` (or len)."""
        i = start
        n = len(text)
        while i < n:
            char = text[i]
            if char == "\\" and i + 1 < n:
                if self.version == 3 or text[i + 1] in ",= ":
                    i += 2
                    continue
            elif char in stops:
                return i
            i += 1
        return n


_V2 = Dialect(
    version=2,
    measurement_escapes=str.maketrans({",": "\\,", " ": "\\ ", "=": "\\="}),
    key_escapes=str.maketrans({",": "\\,", "=": "\\=", " ": "\\ "}),
    reserved_tag_keys=frozenset({"_field", "_measurement", "time"}),
    reserved_field_keys=frozenset({"time"}),
    forbid_tag_field_overlap=False,
    measurement_specials=",= ",
)
_V3 = Dialect(
    version=3,
    measurement_escapes=str.maketrans({",": "\\,", " ": "\\ ", "\\": "\\\\"}),
    key_escapes=str.maketrans({",": "\\,", "=": "\\=", " ": "\\ ", "\\": "\\\\"}),
    reserved_tag_keys=frozenset({"time"}),
    reserved_field_keys=frozenset({"time"}),
    forbid_tag_field_overlap=True,
    measurement_specials=", ",
)


@dataclass(frozen=True, slots=True)
class ParsedLine:
    measurement: str
    tags: dict[str, str]
    fields: dict[str, Any]
    timestamp: int | None


class LineSyntaxError(ValueError):
    """A line of line protocol could not be parsed."""


def _parse_field_value(raw: str) -> Any:
    if raw[0] == '"':
        if len(raw) < 2 or raw[-1] != '"':
            raise LineSyntaxError(f"unterminated string field value {raw[:40]!r}")
        body = raw[1:-1]
        if "\\" not in body:
            return body
        out = []
        i = 0
        while i < len(body):
            char = body[i]
            if char == "\\" and i + 1 < len(body) and body[i + 1] in '"\\':
                out.append(body[i + 1])
                i += 2
                continue
            out.append(char)
            i += 1
        return "".join(out)
    if raw in ("t", "T", "true", "True", "TRUE"):
        return True
    if raw in ("f", "F", "false", "False", "FALSE"):
        return False
    try:
        if raw[-1] == "i":
            return int(raw[:-1])
        if raw[-1] == "u":
            return UInt(int(raw[:-1]))
        return float(raw)
    except ValueError:
        raise LineSyntaxError(f"invalid field value {raw[:40]!r}") from None


def _split_fields(dialect: Dialect, text: str) -> tuple[list[tuple[str, str]], int]:
    """Split ``k=v,k="a b",...`` up to the first unquoted, unescaped space."""
    pairs: list[tuple[str, str]] = []
    i = 0
    n = len(text)
    while True:
        eq = dialect._scan(text, i, "=")
        if eq >= n:
            raise LineSyntaxError("field without '='")
        key = text[i:eq]
        j = eq + 1
        if j < n and text[j] == '"':
            k = j + 1
            while k < n:
                if text[k] == "\\" and k + 1 < n:
                    k += 2
                    continue
                if text[k] == '"':
                    break
                k += 1
            if k >= n:
                raise LineSyntaxError("unterminated string field value")
            end = k + 1
        else:
            end = j
            while end < n and text[end] not in ", ":
                end += 1
        pairs.append((key, text[j:end]))
        if end >= n or text[end] == " ":
            return pairs, end
        i = end + 1


def parse_line(dialect: Dialect, line: str) -> ParsedLine:
    """Parse one line of line protocol (no comments or blank lines)."""
    end_meas = dialect._scan(line, 0, ", ")
    measurement = dialect._unescape(line[:end_meas], dialect.measurement_specials)
    if not measurement:
        raise LineSyntaxError("missing measurement")
    tags: dict[str, str] = {}
    i = end_meas
    if i < len(line) and line[i] == ",":
        end_tags = dialect._scan(line, i + 1, " ")
        for pair in _split_escaped(dialect, line[i + 1 : end_tags], ","):
            eq = dialect._scan(pair, 0, "=")
            if eq >= len(pair):
                raise LineSyntaxError(f"tag without '=': {pair[:40]!r}")
            tags[dialect._unescape(pair[:eq], ",= ")] = dialect._unescape(pair[eq + 1 :], ",= ")
        i = end_tags
    if i >= len(line) or line[i] != " ":
        raise LineSyntaxError("missing fields")
    raw_fields, end_fields = _split_fields(dialect, line[i + 1 :])
    fields = {dialect._unescape(key, ",= "): _parse_field_value(value) for key, value in raw_fields if value}
    if len(fields) != len(raw_fields):
        raise LineSyntaxError("empty field value")
    rest = line[i + 1 + end_fields :].strip()
    timestamp = None
    if rest:
        try:
            timestamp = int(rest)
        except ValueError:
            raise LineSyntaxError(f"invalid timestamp {rest[:40]!r}") from None
    return ParsedLine(measurement, tags, fields, timestamp)


def _split_escaped(dialect: Dialect, text: str, sep: str) -> list[str]:
    parts = []
    start = 0
    while start <= len(text):
        end = dialect._scan(text, start, sep)
        parts.append(text[start:end])
        start = end + 1
    return parts
