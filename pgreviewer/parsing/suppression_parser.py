from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Collection

_INLINE_SQL_IGNORE_RE = re.compile(
    r"--\s*pgreviewer:ignore(?:\[(?P<rules>[^\]]+)\])?",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class InlineSuppression:
    suppress_all: bool = False
    rules: set[str] = field(default_factory=set)
    unknown_rules: set[str] = field(default_factory=set)

    def suppresses(self, rule_name: str) -> bool:
        return self.suppress_all or rule_name.lower() in self.rules


def parse_inline_suppressions(
    sql: str,
    *,
    known_rules: Collection[str],
) -> InlineSuppression:
    if not sql:
        return InlineSuppression()

    known_rules_normalized = {rule.strip().lower() for rule in known_rules}
    suppress_all = False
    parsed_rules: set[str] = set()
    unknown_rules: set[str] = set()

    for match in _INLINE_SQL_IGNORE_RE.finditer(sql):
        raw_rules = match.group("rules")
        if raw_rules is None:
            suppress_all = True
            continue
        for raw_rule in raw_rules.split(","):
            rule = raw_rule.strip().lower()
            if not rule:
                continue
            if rule in known_rules_normalized:
                parsed_rules.add(rule)
            else:
                unknown_rules.add(rule)

    return InlineSuppression(
        suppress_all=suppress_all,
        rules=parsed_rules,
        unknown_rules=unknown_rules,
    )
