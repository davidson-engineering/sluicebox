"""Content-aware tag injection.

Tags can be added to points from four sources, applied in this order:

1. **Context tags** - :func:`tag_context` scopes (per thread / asyncio task), e.g. a tenant.
2. **Static tags** - ``[tags] static`` / ``from_env`` and the client's ``tags=`` argument.
3. **Rules** - ``[[tags.rules]]``: tags derived from the point's measurement, tags and field
   values, including values captured by named regex groups.
4. **Enrichers** - Python callables ``(measurement, tags, fields) -> {tag: value} | None``.

Each source fills in tags subject to the conflict policy: ``keep`` (the point's own value
wins), ``overwrite`` (the injected value wins) or ``error``. Later sources see the tags
added by earlier ones, so rules can match on static or context tags.
"""

from __future__ import annotations

import os
import re
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, TypeAlias

from .exceptions import ConfigurationError, ValidationError

if TYPE_CHECKING:
    from .config import ConflictPolicy, FieldPredicate, TagRule, TagsConfig

__all__ = ["Enricher", "TagInjector", "current_context_tags", "tag_context"]

#: ``(measurement, tags, fields) -> tags to add`` (or None for no change).
Enricher: TypeAlias = Callable[[str, Mapping[str, Any], Mapping[str, Any]], Mapping[str, Any] | None]

_CONTEXT_TAGS: ContextVar[Mapping[str, str] | None] = ContextVar("influxkit_context_tags", default=None)


@contextmanager
def tag_context(**tags: str) -> Iterator[None]:
    """Add ``tags`` to every point written in this context (thread / asyncio task).

    Scopes nest; inner values win. Tags are captured when ``write()`` is called, so they
    apply even though sending happens later on background threads.

    Use bounded values (tenant, region, endpoint): every distinct tag set is a series, so
    unbounded ones such as request ids belong in fields.

    >>> with tag_context(tenant="acme", region="eu-west-1"):
    ...     client.write(points)  # doctest: +SKIP
    """
    current = _CONTEXT_TAGS.get()
    merged = {**current, **tags} if current else dict(tags)
    token = _CONTEXT_TAGS.set(merged)
    try:
        yield
    finally:
        _CONTEXT_TAGS.reset(token)


def current_context_tags() -> Mapping[str, str]:
    """The tags :func:`tag_context` currently adds (empty if none)."""
    return _CONTEXT_TAGS.get() or {}


class _TemplateValues(dict[str, Any]):
    """``str.format_map`` namespace; a missing name means the rule cannot be rendered."""

    def __missing__(self, key: str) -> Any:
        raise KeyError(key)


def _compile_predicate(pred: FieldPredicate) -> Callable[[Any], bool]:
    checks: list[Callable[[Any], bool]] = []

    def numeric(value: Any) -> float | None:
        if isinstance(value, bool) or not isinstance(value, int | float):
            return None
        return float(value)

    for op, bound in (("gt", pred.gt), ("ge", pred.ge), ("lt", pred.lt), ("le", pred.le)):
        if bound is None:
            continue
        compare = {
            "gt": lambda a, b: a > b,
            "ge": lambda a, b: a >= b,
            "lt": lambda a, b: a < b,
            "le": lambda a, b: a <= b,
        }[op]

        def check(
            value: Any, bound: float = bound, compare: Callable[[float, float], bool] = compare
        ) -> bool:
            number = numeric(value)
            return number is not None and compare(number, bound)

        checks.append(check)
    if pred.eq is not None:
        equal_to = pred.eq
        checks.append(lambda value: bool(value == equal_to))
    if pred.ne is not None:
        not_equal_to = pred.ne
        checks.append(lambda value: bool(value != not_equal_to))
    if pred.in_ is not None:
        options = list(pred.in_)
        checks.append(lambda value: value in options)
    if pred.regex is not None:
        pattern = re.compile(pred.regex)
        checks.append(lambda value: isinstance(value, str) and pattern.search(value) is not None)
    return lambda value: all(check(value) for check in checks)


class _CompiledRule:
    __slots__ = (
        "has_fields",
        "has_tags",
        "measurement_rx",
        "missing_tags",
        "name",
        "policy",
        "predicates",
        "tag_patterns",
        "templates",
    )

    def __init__(self, rule: TagRule, index: int, default_policy: ConflictPolicy) -> None:
        when = rule.when
        self.name = rule.name or f"rules[{index}]"
        self.measurement_rx = re.compile(when.measurement) if when.measurement else None
        self.tag_patterns = tuple((key, re.compile(pattern)) for key, pattern in when.tags.items())
        self.has_tags = when.has_tags
        self.missing_tags = when.missing_tags
        self.has_fields = when.has_fields
        self.predicates = tuple((key, _compile_predicate(pred)) for key, pred in when.fields.items())
        # (tag key, template, is a plain literal)
        self.templates = tuple(
            (key, tmpl, "{" not in tmpl and "}" not in tmpl) for key, tmpl in rule.set.items()
        )
        self.policy: ConflictPolicy = rule.on_conflict or default_policy

    def match_measurement(self, measurement: str) -> dict[str, str] | None:
        """Named groups of the measurement pattern if it matches (``{}`` when unconditional)."""
        if self.measurement_rx is None:
            return {}
        match = self.measurement_rx.search(measurement)
        if match is None:
            return None
        return {k: v for k, v in match.groupdict().items() if v is not None}

    def evaluate(
        self, measurement: str, groups: dict[str, str], tags: Mapping[str, Any], fields: Mapping[str, Any]
    ) -> dict[str, str] | None:
        if self.has_tags and not self.has_tags <= tags.keys():
            return None
        if self.missing_tags and not self.missing_tags.isdisjoint(tags.keys()):
            return None
        if self.has_fields and not self.has_fields <= fields.keys():
            return None
        for key, predicate in self.predicates:
            if key not in fields or not predicate(fields[key]):
                return None
        captured = groups
        for key, pattern in self.tag_patterns:
            value = tags.get(key)
            if value is None:
                return None
            match = pattern.search(value if isinstance(value, str) else str(value))
            if match is None:
                return None
            found = match.groupdict()
            if found:
                captured = {**captured, **{k: v for k, v in found.items() if v is not None}}
        out: dict[str, str] = {}
        namespace: _TemplateValues | None = None
        for key, template, literal in self.templates:
            if literal:
                out[key] = template
                continue
            if namespace is None:
                namespace = _TemplateValues(captured)
                # Absent values (None / "") must not render as "None": the rule then does not apply.
                namespace.update(
                    measurement=measurement,
                    tags={k: v for k, v in tags.items() if v is not None and v != ""},
                    fields={k: v for k, v in fields.items() if v is not None},
                )
            try:
                out[key] = str(template.format_map(namespace))
            except (KeyError, IndexError, AttributeError):
                return None  # a referenced value is absent: the rule does not apply
        return out


class TagInjector:
    """Applies context tags, static tags, rules and enrichers to points."""

    def __init__(
        self,
        config: TagsConfig,
        *,
        static: Mapping[str, str] | None = None,
        enrichers: Sequence[Enricher] = (),
        environ: Mapping[str, str] | None = None,
    ) -> None:
        env = os.environ if environ is None else environ
        resolved: dict[str, str] = dict(config.static)
        for tag, spec in config.from_env.items():
            if isinstance(spec, str):
                value = env.get(spec)
                if value is None or value == "":
                    raise ConfigurationError(
                        f"tags.from_env: environment variable {spec!r} (for tag {tag!r}) is not set; "
                        f'use {tag} = {{ var = "{spec}", default = "..." }} to make it optional'
                    )
            else:
                value = env.get(spec.var) or spec.default
            if value:
                resolved[tag] = value
        if static:
            resolved.update(static)
        self.static: dict[str, str] = resolved
        self.policy: ConflictPolicy = config.on_conflict
        self.rules = tuple(_CompiledRule(rule, i, config.on_conflict) for i, rule in enumerate(config.rules))
        self.enrichers = tuple(enrichers)
        #: Whether static tags or enrichers apply to every point (rules and context tags are
        #: checked per measurement / per call).
        self.active = bool(self.static or self.enrichers)
        self._rule_cache: dict[str, tuple[tuple[_CompiledRule, dict[str, str]], ...]] = {}

    def rules_for(self, measurement: str) -> tuple[tuple[_CompiledRule, dict[str, str]], ...]:
        """Rules whose measurement condition matches, with the groups it captured (cached)."""
        cached = self._rule_cache.get(measurement)
        if cached is None:
            matched = []
            for rule in self.rules:
                groups = rule.match_measurement(measurement)
                if groups is not None:
                    matched.append((rule, groups))
            cached = tuple(matched)
            if len(self._rule_cache) > 10_000:
                self._rule_cache.clear()
            self._rule_cache[measurement] = cached
        return cached

    def apply(
        self,
        measurement: str,
        tags: Mapping[str, Any] | None,
        fields: Mapping[str, Any],
        rules: tuple[tuple[_CompiledRule, dict[str, str]], ...],
        context: Mapping[str, str] | None = None,
    ) -> Mapping[str, Any] | None:
        """Return the point's tags with injected tags added (the input is never mutated).

        ``context`` is the current :func:`tag_context` mapping; callers on a hot path pass the
        value they already read.
        """
        if context is None:
            context = _CONTEXT_TAGS.get()
        static = self.static
        if not (context or static or rules or self.enrichers):
            return tags
        policy = self.policy
        # Precedence: context tags are more specific than static tags, so they win over them
        # under every policy; the policy decides how both relate to the point's own tags.
        if not rules and not self.enrichers and policy != "error":
            # Fast path: plain dict merges in C.
            base = {**static, **context} if (context and static) else (context or static)
            if policy == "overwrite":
                return {**tags, **base} if tags else base
            if not tags:
                return base
            merged = {**base, **tags}
            for key, value in base.items():
                current = merged[key]
                if current is None or current == "":  # an empty point value does not block injection
                    merged[key] = value
            return merged

        merged = dict(tags) if tags else {}
        base = {**static, **context} if (context and static) else (context or static)
        if base:
            self._merge(merged, base, policy, measurement, "static/context tags")
        for rule, groups in rules:
            added = rule.evaluate(measurement, groups, merged, fields)
            if added:
                self._merge(merged, added, rule.policy, measurement, f"tag rule {rule.name!r}")
        for enricher in self.enrichers:
            enriched = enricher(measurement, merged, fields)
            if enriched:
                self._merge(
                    merged,
                    enriched,
                    policy,
                    measurement,
                    f"enricher {getattr(enricher, '__name__', enricher)!r}",
                )
        return merged

    @staticmethod
    def _merge(
        target: dict[str, Any],
        source: Mapping[str, Any],
        policy: ConflictPolicy,
        measurement: str,
        origin: str,
    ) -> None:
        if policy == "overwrite":
            target.update(source)
            return
        for key, value in source.items():
            existing = target.get(key)
            if existing is None or existing == "":
                target[key] = value
            elif policy == "error" and existing != value:
                raise ValidationError(
                    f"{origin} sets tag to {value!r} but the point already has {existing!r}",
                    code="tag_conflict",
                    measurement=measurement,
                    key=key,
                )
