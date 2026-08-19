from lakescore.sql_utils import escape_sql_string


def test_plain_string_is_unchanged():
    assert escape_sql_string("a normal description") == "a normal description"


def test_escapes_double_quotes():
    assert escape_sql_string('a "quoted" word') == 'a \\"quoted\\" word'


def test_escapes_single_quotes():
    assert escape_sql_string("O'Brien") == "O\\'Brien"


def test_escapes_backslash_before_quotes_so_escaping_is_not_reescaped():
    # A literal backslash-quote in the input must become backslash-backslash-quote,
    # not backslash-backslash-backslash-quote.
    assert escape_sql_string('a\\"b') == 'a\\\\\\"b'


def test_sql_injection_attempt_is_neutralized_in_comment_statement():
    malicious = 'nice"; DROP TABLE dq_summary; --'
    escaped = escape_sql_string(malicious)
    statement = f'ALTER TABLE t ALTER COLUMN c COMMENT "{escaped}";'

    # Every double quote in the escaped payload must be backslash-escaped (`\"` still contains
    # `"` as a raw substring, so this checks for an *unescaped* one, not mere containment).
    unescaped_quotes = statement.replace('\\"', "").count('"')
    assert unescaped_quotes == 2  # only the statement's own opening/closing delimiters
