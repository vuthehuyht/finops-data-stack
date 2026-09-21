{#-
  Helpers that extract structured signals from the free-text DESCRIPTION of
  analyst reports (STG_ANALYST_REPORTS). MVP heuristics tuned on the historical
  FireAnt backfill, not a promise of high precision.

  Redshift REGEXP_* uses POSIX syntax: no lookbehind, no lazy quantifiers, and
  REGEXP_SUBSTR returns the whole match (not a capture group). The patterns
  below stay inside that subset and are mirrored by
  tests/transform/test_analyst_report_patterns.py, which reads them from this
  file. Keep every pattern on a single `set` line so the test can parse it.
-#}

{%- macro clean_report_text(column) -%}
  {#- DESCRIPTION is HTML: accented letters arrive as Latin-1 named entities
      (e.g. gi&aacute;) so accented patterns never match until decoded. -#}
  {%- set entities = {
    'agrave': 'à', 'aacute': 'á', 'acirc': 'â', 'atilde': 'ã',
    'egrave': 'è', 'eacute': 'é', 'ecirc': 'ê',
    'igrave': 'ì', 'iacute': 'í',
    'ograve': 'ò', 'oacute': 'ó', 'ocirc': 'ô', 'otilde': 'õ',
    'ugrave': 'ù', 'uacute': 'ú', 'yacute': 'ý'
  } -%}
  {%- set ns = namespace(expr=column) -%}
  {%- for name, char in entities.items() -%}
    {%- set ns.expr = "REPLACE(REPLACE(" ~ ns.expr ~ ", '&" ~ name ~ ";', '" ~ char ~ "'), '&" ~ name.capitalize() ~ ";', '" ~ char.upper() ~ "')" -%}
  {%- endfor -%}
  {%- set ns.expr = "REPLACE(REPLACE(" ~ ns.expr ~ ", '&nbsp;', ' '), '&amp;', '&')" -%}
  REGEXP_REPLACE(REGEXP_REPLACE({{ ns.expr }}, '<[^>]*>', ' '), '[[:space:]]+', ' ')
{%- endmacro -%}

{%- macro analyst_is_buy(text) -%}
  {%- set buy_pattern = '(khuyến nghị|Khuyến nghị|KHUYẾN NGHỊ|quan điểm|Quan điểm|đánh giá|Đánh giá|Hành động|xếp hạng|Xếp hạng)[^.]{0,40}(MUA|Mua|KHẢ QUAN|Khả quan|Outperform|OUTPERFORM)' -%}
  {%- set negative_pattern = '(KÉM KHẢ QUAN|Kém khả quan|Underperform|UNDERPERFORM)' -%}
  (REGEXP_INSTR({{ text }}, '{{ buy_pattern }}') > 0 AND REGEXP_INSTR({{ text }}, '{{ negative_pattern }}') = 0)
{%- endmacro -%}

{%- macro analyst_target_price(text) -%}
  {%- set price_pattern = '(giá mục tiêu|Giá mục tiêu|GIÁ MỤC TIÊU)[^0-9]{0,30}(([0-9]{1,2} (tháng|năm)|năm [0-9]{4}|[0-9]+([.,][0-9]+)?%|từ [0-9]{1,3}([.,][0-9]{3})+( đồng| VND| VNĐ)?(/cp|/cổ phiếu)?)[^0-9]{0,30}){0,2}[0-9]{1,3}([.,][0-9]{3})+' -%}
  {%- set last_number_pattern = '[0-9]{1,3}([.,][0-9]{3})+$' -%}
  NULLIF(REGEXP_REPLACE(REGEXP_SUBSTR(REGEXP_SUBSTR({{ text }}, '{{ price_pattern }}'), '{{ last_number_pattern }}'), '[^0-9]', ''), '')::NUMERIC(38, 4)
{%- endmacro -%}
