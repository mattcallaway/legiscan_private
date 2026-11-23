"""Suggest readable civic tags from indexed bill metadata and find recurring phrases."""

from collections import Counter, defaultdict
from dataclasses import dataclass
from functools import lru_cache
from html import unescape
import json
from pathlib import Path
import re
from unicodedata import normalize

from bill_research import TOPIC_SEARCH_FIELDS

DICTIONARY = Path(__file__).with_name("civic_topics.json")
_FIELD_WEIGHTS = {"title": 3, "subjects": 3, "description": 1}
_BOUNDARY = re.compile(r"[.,;:!?()[\]{}\d\n\r/]+")
_WORD = re.compile(r"[^\W\d_]+(?:'[^\W\d_]+)?")
_MARKUP_TAG = re.compile(r"</?[a-z][a-z0-9]*\b[^>]*>", re.IGNORECASE)
_CONNECTORS = {"and", "for", "in", "of", "on", "to"}


@dataclass(frozen=True)
class CivicRules:
    topics_by_phrase: dict[str, tuple[str, ...]]
    procedural: re.Pattern
    stop_words: frozenset[str]
    anchors: frozenset[str]
    max_words: int


def _normalize(value):
    return normalize("NFKC", unescape(value)).replace("’", "'").casefold()


@lru_cache(maxsize=1)
def load_civic_rules():
    with DICTIONARY.open(encoding="utf-8") as source:
        dictionary = json.load(source)
    topics = defaultdict(list)
    for label, aliases in dictionary["topics"].items():
        for alias in aliases:
            phrase = " ".join(_WORD.findall(_normalize(alias)))
            topics[phrase].append(label)
    stop_words = frozenset(dictionary["stop_words"])
    phrases = sorted(map(_normalize, dictionary["procedural_phrases"]), key=len, reverse=True)
    procedural = re.compile(r"(?<!\w)(?:" + "|".join(
        re.escape(phrase).replace(r"\ ", r"\s+") for phrase in phrases
    ) + r")(?!\w)")
    return CivicRules(
        {phrase: tuple(labels) for phrase, labels in topics.items()},
        procedural,
        stop_words,
        frozenset(word for phrase in topics for word in phrase.split() if word not in stop_words),
        max(len(phrase.split()) for phrase in topics),
    )


def _words(value, rules):
    if not isinstance(value, str):
        return
    text = _MARKUP_TAG.sub(" ", _normalize(value[:16_000]))
    text = rules.procedural.sub(";", text)
    for section in _BOUNDARY.split(text):
        yield _WORD.findall(section)


def _windows(words, minimum, maximum):
    for count in range(minimum, maximum + 1):
        for start in range(len(words) - count + 1):
            yield words[start:start + count]


def suggest_civic_topics(bill, *, limit=5):
    """Return topic labels and exact field evidence without editing a workspace."""
    rules = load_civic_rules()
    evidence = defaultdict(list)
    scores = Counter()
    for field, field_label in TOPIC_SEARCH_FIELDS.items():
        phrases = {
            " ".join(window)
            for words in _words(bill.get(field), rules)
            for window in _windows(words, 1, rules.max_words)
        }
        for phrase in sorted(phrases & rules.topics_by_phrase.keys()):
            for topic in rules.topics_by_phrase[phrase]:
                evidence[topic].append({"phrase": phrase, "field": field_label})
                scores[topic] += _FIELD_WEIGHTS[field]

    labels = sorted(scores, key=lambda label: (-scores[label], label))
    return [{"topic": label, "evidence": evidence[label]} for label in labels[:limit]]


def recurring_phrases(bills, *, min_bills=3, max_share=0.40, limit=50):
    """Count distinct bills per candidate and down-rank broad boilerplate."""
    rules = load_civic_rules()
    document_frequency = Counter()
    title_subject_frequency = Counter()
    total = 0
    for bill in bills:
        seen = set()
        prominent = set()
        for field in TOPIC_SEARCH_FIELDS:
            phrases = {
                " ".join(window)
                for words in _words(bill.get(field), rules)
                for window in _windows(words, 2, 3)
                if window[0] not in rules.stop_words and window[-1] not in rules.stop_words
                and all(word not in rules.stop_words or word in _CONNECTORS for word in window[1:-1])
            }
            seen.update(phrases)
            if field != "description":
                prominent.update(phrases)
        document_frequency.update(seen)
        title_subject_frequency.update(prominent)
        total += 1

    candidates = []
    for phrase, count in document_frequency.items():
        if not min_bills <= count <= max_share * total:
            continue
        title_subject_count = title_subject_frequency[phrase]
        anchored = bool(rules.anchors.intersection(phrase.split()))
        if not anchored and title_subject_count < min_bills:
            continue
        candidates.append({
            "phrase": phrase,
            "bills": count,
            "title_or_subject_bills": title_subject_count,
            "dictionary_topics": list(rules.topics_by_phrase.get(phrase, ())),
            "civic_vocabulary": anchored,
        })
    candidates.sort(key=lambda row: (-row["title_or_subject_bills"], -row["bills"], row["phrase"]))
    return {"bills_scanned": total, "phrases": candidates[:limit]}
