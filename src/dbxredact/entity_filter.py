"""Deny/Allow list filtering for detected entities."""

import re
from dataclasses import dataclass, field
from typing import List, Dict, Any

import yaml


@dataclass
class EntityFilter:
    """Configuration for entity-level deny/allow filtering.

    deny_list: exact entity texts to always remove (case-insensitive)
    allow_list: exact entity texts to always keep / force-detect
    deny_patterns: regex patterns -- matching entities are removed
    allow_patterns: regex patterns -- matches in raw text become forced entities
    """
    deny_list: List[str] = field(default_factory=list)
    allow_list: List[str] = field(default_factory=list)
    deny_patterns: List[str] = field(default_factory=list)
    allow_patterns: List[str] = field(default_factory=list)

    def __post_init__(self):
        self._deny_set = {t.lower() for t in self.deny_list}
        self._allow_set = {t.lower() for t in self.allow_list}
        self._deny_re = [re.compile(p, re.IGNORECASE) for p in self.deny_patterns]
        self._allow_re = [re.compile(p, re.IGNORECASE) for p in self.allow_patterns]


def load_filter_from_yaml(path: str) -> EntityFilter:
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    return EntityFilter(
        deny_list=data.get("deny_list", []),
        allow_list=data.get("allow_list", []),
        deny_patterns=data.get("deny_patterns", []),
        allow_patterns=data.get("allow_patterns", []),
    )


def load_filter_from_table(spark, table_name: str, list_type: str = "deny") -> EntityFilter:
    """Load deny or allow entries from a Unity Catalog table.

    Expected columns: value (str), is_pattern (bool).
    """
    df = spark.table(table_name).collect()
    exact, patterns = [], []
    for row in df:
        if row["is_pattern"]:
            patterns.append(row["value"])
        else:
            exact.append(row["value"])
    if list_type == "deny":
        return EntityFilter(deny_list=exact, deny_patterns=patterns)
    return EntityFilter(allow_list=exact, allow_patterns=patterns)


def apply_deny_filter(
    entities: List[Dict[str, Any]], ef: EntityFilter
) -> List[Dict[str, Any]]:
    """Remove entities matching the deny list or deny patterns."""
    result = []
    for ent in entities:
        text = ent.get("entity", "")
        if text.lower() in ef._deny_set:
            continue
        if any(rx.search(text) for rx in ef._deny_re):
            continue
        result.append(ent)
    return result


def apply_allow_filter(
    text: str, ef: EntityFilter
) -> List[Dict[str, Any]]:
    """Scan raw text for allow-list terms/patterns and return forced entities."""
    forced = []
    lower_text = text.lower()
    for term in ef._allow_set:
        start = 0
        while True:
            idx = lower_text.find(term, start)
            if idx == -1:
                break
            forced.append({
                "entity": text[idx : idx + len(term)],
                "start": idx,
                "end": idx + len(term),
                "entity_type": "ALLOW_LIST",
                "score": 1.0,
                "source": "allow_list",
            })
            start = idx + 1
    for rx in ef._allow_re:
        for m in rx.finditer(text):
            forced.append({
                "entity": m.group(),
                "start": m.start(),
                "end": m.end(),
                "entity_type": "ALLOW_LIST",
                "score": 1.0,
                "source": "allow_list",
            })
    return forced
