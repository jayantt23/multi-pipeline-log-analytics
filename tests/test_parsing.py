"""Verify the master regex (PARSING_SPEC §1) against the 4 malformed-rule cases.

This uses Python `re` against the same regex string Mongo uses for $regexFind.
The pattern semantics are identical for the capture groups we care about.
"""
from __future__ import annotations

import re

from src.pipelines.mongodb.aggregations import MASTER_REGEX

PATTERN = re.compile(MASTER_REGEX)

# 5 valid + 2 with bytes="-" + 2 with status="-" + 2 truly malformed
LINES = [
    # 5 valid
    '199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245',
    'unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985',
    'pipe1.nyc.pipeline.com - - [01/Jul/1995:00:00:23 -0400] "GET /ksc.html HTTP/1.0" 304 0',
    'host1 - - [01/Jul/1995:00:00:11 -0400] "GET /images/launch-logo.gif HTTP/1.0" 200 1839',
    'host2 - - [01/Jul/1995:00:00:12 -0400] "GET /a HTTP/1.0" 200 100',
    # 2 with bytes="-"  (valid: bytes -> 0)
    'host3 - - [01/Jul/1995:00:01:00 -0400] "GET /x HTTP/1.0" 304 -',
    'host4 - - [01/Jul/1995:00:01:01 -0400] "HEAD /y HTTP/1.0" 304 -',
    # 2 with status="-"  (malformed)
    'host5 - - [01/Jul/1995:00:02:00 -0400] "GET /z HTTP/1.0" - 100',
    'host6 - - [01/Jul/1995:00:02:01 -0400] "GET /w HTTP/1.0" - -',
    # 2 truly malformed (regex fails)
    'not a log line',
    '',
]


def _is_malformed(line: str) -> bool:
    """Per PARSING_SPEC §6: malformed iff regex fails OR captured status == '-'."""
    m = PATTERN.match(line)
    if not m:
        return True
    return m.group(4) == "-"


def test_malformed_count_is_four():
    malformed = [ln for ln in LINES if _is_malformed(ln)]
    assert len(malformed) == 4, f"expected 4 malformed, got {len(malformed)}: {malformed}"


def test_bytes_dash_becomes_zero():
    """bytes='-' lines parse OK; the per-spec mapping is bytes_raw='-' → 0."""
    line = 'host3 - - [01/Jul/1995:00:01:00 -0400] "GET /x HTTP/1.0" 304 -'
    m = PATTERN.match(line)
    assert m is not None
    assert m.group(4) == "304"
    assert m.group(5) == "-"
    # Mapping rule (bytes_raw='-' → 0) applied in aggregations.parse_pipeline.


def test_valid_line_capture_groups():
    line = '199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245'
    m = PATTERN.match(line)
    assert m is not None
    assert m.group(1) == "199.72.81.55"
    assert m.group(2) == "01/Jul/1995:00:00:01 -0400"
    assert m.group(3) == "GET /history/apollo/ HTTP/1.0"
    assert m.group(4) == "200"
    assert m.group(5) == "6245"


def test_short_request_is_not_malformed():
    """Short request lines (fewer than 3 tokens) are valid; missing fields → NULL."""
    line = 'host - - [01/Jul/1995:00:03:00 -0400] "GET" 200 50'
    m = PATTERN.match(line)
    assert m is not None
    assert m.group(3) == "GET"
    assert not _is_malformed(line)
