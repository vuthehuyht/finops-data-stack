"""Tests for the analyst-report regex macros.

The dbt macro is the source of truth for the patterns. Redshift is not
available in unit tests, so the patterns are read from the macro file and
exercised with Python's `re` (the subset used is POSIX-compatible), and the
macros are rendered with plain Jinja to catch quoting mistakes.
"""

import html
import re
from pathlib import Path

import jinja2
import pytest

_MACRO_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "transform"
    / "dbt"
    / "macros"
    / "analyst_report_signals.sql"
)
_MACRO_SOURCE = _MACRO_PATH.read_text(encoding="utf-8")


def _pattern(name: str) -> str:
    match = re.search(rf"set {name} = '(.*)' -%\}}", _MACRO_SOURCE)
    assert match, f"pattern {name} not found in macro"
    return match.group(1)


_BUY = _pattern("buy_pattern")
_NEGATIVE = _pattern("negative_pattern")
_PRICE = _pattern("price_pattern")
_LAST_NUMBER = _pattern("last_number_pattern")


def _is_buy(text: str) -> bool:
    return bool(re.search(_BUY, text)) and not re.search(_NEGATIVE, text)


def _posix_longest_match(pattern: str, text: str) -> str | None:
    """Emulate POSIX leftmost-longest matching, which Redshift uses.

    Python's `re` is leftmost-first: for "từ 41,000 lên 45,200" it stops at
    the first price, while Redshift's REGEXP_SUBSTR keeps going to the last.
    """
    first = re.search(pattern, text)
    if not first:
        return None
    for end in range(len(text), first.start(), -1):
        if re.fullmatch(pattern, text[first.start() : end]):
            return text[first.start() : end]
    return first.group(0)


def _target_price(text: str) -> int | None:
    match = _posix_longest_match(_PRICE, text)
    if not match:
        return None
    last = re.search(_LAST_NUMBER, match)
    return int(re.sub(r"[^0-9]", "", last.group(0)))


@pytest.mark.parametrize(
    "text",
    [
        "BSC duy trì khuyến nghị MUA đối với cổ phiếu VRE",
        "Duy trì khuyến nghị Khả quan với giá mục tiêu 34.900 đồng/cp",
        "BVSC khuyến nghị OUTPERFORM cổ phiếu CTG",
        "Hành động: MUA với giá mục tiêu 35.000 đồng/cp",
        "Chúng tôi duy trì đánh giá KHẢ QUAN với tiềm năng tăng giá 21,9%",
    ],
)
def test_buy_pattern_matches_recommendations(text: str) -> None:
    assert _is_buy(text)


@pytest.mark.parametrize(
    "text",
    [
        "Công ty công bố kế hoạch mua lại cổ phiếu quỹ",  # buyback, not a rating
        "VPBS đưa ra khuyến nghị NẮM GIỮ đối với cổ phiếu VCB",
        "Chúng tôi khuyến nghị KÉM KHẢ QUAN đối với cổ phiếu này",
        "Tóm tắt báo cáo phân tích kết quả kinh doanh năm 2024",
    ],
)
def test_buy_pattern_rejects_non_buy_text(text: str) -> None:
    assert not _is_buy(text)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("Giá mục tiêu: 49.500", 49500),
        ("khuyến nghị MUA với giá mục tiêu 168,653 đồng (upside 53%)", 168653),
        # "12 tháng" must be skipped, not concatenated into 1256000.
        ("giá mục tiêu 12 tháng là 56.000 đồng/cổ phiếu", 56000),
        ("giá mục tiêu năm 2024 của MBB là 27.000", 27000),
        # Old -> new price: the new (last) value wins.
        ("giá mục tiêu từ 41,000 lên 45,200 VND/cp", 45200),
        ("giá mục tiêu thêm 3,8% lên 99.900đ/cp", 99900),
    ],
)
def test_target_price_extracts_last_thousands_number(text: str, expected: int) -> None:
    assert _target_price(text) == expected


@pytest.mark.parametrize(
    "text",
    [
        # Thousands-unit shorthand is intentionally skipped.
        "Giá mục tiêu 80, cutloss 65",
        "điều chỉnh giá mục tiêu so với báo cáo ngày 15/01/2016",
        "Không nhắc tới mức giá nào",
    ],
)
def test_target_price_returns_none_without_thousands_number(text: str) -> None:
    assert _target_price(text) is None


def _render(macro_call: str) -> str:
    env = jinja2.Environment()
    template = env.from_string(_MACRO_SOURCE + macro_call)
    return template.render().strip()


def test_clean_report_text_decodes_entities_and_strips_tags() -> None:
    sql = _render("{{ clean_report_text('DESCRIPTION') }}")

    # The rendered SQL must decode entities before stripping markup.
    assert "'&aacute;', 'á'" in sql
    assert "'&Aacute;', 'Á'" in sql
    assert "'&nbsp;', ' '" in sql
    assert "'<[^>]*>'" in sql
    assert sql.startswith("REGEXP_REPLACE(REGEXP_REPLACE(")
    # Balanced parentheses guard against a broken nested REPLACE chain.
    assert sql.count("(") == sql.count(")")


def test_entity_decoding_matches_python_reference() -> None:
    # Vietnamese text as it is stored: Latin-1 letters are named entities.
    stored = "<p>gi&aacute; mục ti&ecirc;u&nbsp;56.000 &amp; khuyến nghị MUA</p>"
    decoded = html.unescape(stored)
    assert "giá mục tiêu" in decoded
    assert _target_price(re.sub(r"<[^>]*>", " ", decoded)) == 56000


def test_analyst_macros_render_balanced_sql() -> None:
    buy = _render("{{ analyst_is_buy('REPORT_TEXT') }}")
    price = _render("{{ analyst_target_price('REPORT_TEXT') }}")

    assert buy.count("(") == buy.count(")")
    assert price.count("(") == price.count(")")
    assert "REGEXP_INSTR(REPORT_TEXT" in buy
    assert price.endswith("::NUMERIC(38, 4)")
