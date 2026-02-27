"""Block/Safe list filtering for detected entities.

Block List = items that must always be flagged as PII (force detection).
Safe List  = items that should never be flagged as PII (suppress false positives).
"""

import re
from dataclasses import dataclass, field
from typing import List, Dict, Any

import yaml


@dataclass
class EntityFilter:
    """Configuration for entity-level block/safe filtering.

    safe_list: exact texts to suppress -- matching entities are removed (false-positive suppression)
    block_list: exact texts to force-detect -- always flagged as PII
    safe_patterns: regex patterns -- matching entities are removed
    block_patterns: regex patterns -- matches in raw text become forced PII entities
    """
    safe_list: List[str] = field(default_factory=list)
    block_list: List[str] = field(default_factory=list)
    safe_patterns: List[str] = field(default_factory=list)
    block_patterns: List[str] = field(default_factory=list)

    def __post_init__(self):
        self._safe_set = {t.lower() for t in self.safe_list}
        self._block_set = {t.lower() for t in self.block_list}
        self._safe_re = [re.compile(p, re.IGNORECASE) for p in self.safe_patterns]
        self._block_re = [re.compile(p, re.IGNORECASE) for p in self.block_patterns]


def load_filter_from_yaml(path: str) -> EntityFilter:
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    return EntityFilter(
        safe_list=data.get("safe_list", []),
        block_list=data.get("block_list", []),
        safe_patterns=data.get("safe_patterns", []),
        block_patterns=data.get("block_patterns", []),
    )


def load_filter_from_table(spark, table_name: str, list_type: str = "safe") -> EntityFilter:
    """Load block or safe entries from a Unity Catalog table.

    Expected columns: value (str), is_pattern (bool).
    """
    df = spark.table(table_name).collect()
    exact, patterns = [], []
    for row in df:
        if row["is_pattern"]:
            patterns.append(row["value"])
        else:
            exact.append(row["value"])
    if list_type == "safe":
        return EntityFilter(safe_list=exact, safe_patterns=patterns)
    return EntityFilter(block_list=exact, block_patterns=patterns)


def apply_safe_filter(
    entities: List[Dict[str, Any]], ef: EntityFilter
) -> List[Dict[str, Any]]:
    """Remove entities matching the safe list (suppress false positives)."""
    result = []
    for ent in entities:
        text = ent.get("entity", "")
        if text.lower() in ef._safe_set:
            continue
        if any(rx.search(text) for rx in ef._safe_re):
            continue
        result.append(ent)
    return result


def apply_block_filter(
    text: str, ef: EntityFilter
) -> List[Dict[str, Any]]:
    """Scan raw text for block-list terms/patterns and return forced PII entities."""
    forced = []
    lower_text = text.lower()
    for term in ef._block_set:
        start = 0
        while True:
            idx = lower_text.find(term, start)
            if idx == -1:
                break
            forced.append({
                "entity": text[idx : idx + len(term)],
                "start": idx,
                "end": idx + len(term),
                "entity_type": "BLOCK_LIST",
                "score": 1.0,
                "source": "block_list",
            })
            start = idx + 1
    for rx in ef._block_re:
        for m in rx.finditer(text):
            forced.append({
                "entity": m.group(),
                "start": m.start(),
                "end": m.end(),
                "entity_type": "BLOCK_LIST",
                "score": 1.0,
                "source": "block_list",
            })
    return forced
