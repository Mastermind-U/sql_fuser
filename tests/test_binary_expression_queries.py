"""Tests for arithmetic SQL expressions."""

from typing import Iterator

import pytest

from sql_fusion import Table


@pytest.fixture(autouse=True)
def reset_table_alias_counter() -> Iterator[None]:
    """Reset Table alias counter before each test."""
    Table.reset_alias_counter()
    yield
    Table.reset_alias_counter()


def test_column_arithmetic_operators_render_sql() -> None:
    """Test that arithmetic operators render SQL and params correctly."""
    users = Table("users")

    add_sql, add_params = (users.counter + 1).to_sql()
    sub_sql, sub_params = (users.counter - 1).to_sql()
    mul_sql, mul_params = (users.counter * 2).to_sql()
    div_sql, div_params = (users.counter / 2).to_sql()
    radd_sql, radd_params = (1 + users.counter).to_sql()
    rsub_sql, rsub_params = (1 - users.counter).to_sql()
    rmul_sql, rmul_params = (2 * users.counter).to_sql()
    rdiv_sql, rdiv_params = (2 / users.counter).to_sql()

    assert add_sql == '"a"."counter" + ?'
    assert add_params == (1,)
    assert sub_sql == '"a"."counter" - ?'
    assert sub_params == (1,)
    assert mul_sql == '"a"."counter" * ?'
    assert mul_params == (2,)
    assert div_sql == '"a"."counter" / ?'
    assert div_params == (2,)
    assert radd_sql == '? + "a"."counter"'
    assert radd_params == (1,)
    assert rsub_sql == '? - "a"."counter"'
    assert rsub_params == (1,)
    assert rmul_sql == '? * "a"."counter"'
    assert rmul_params == (2,)
    assert rdiv_sql == '? / "a"."counter"'
    assert rdiv_params == (2,)


def test_nested_arithmetic_expression_keeps_parentheses_and_param_order() -> None:
    """Test nested arithmetic expressions keep grouping and parameter order."""
    users = Table("users")

    expr = (users.counter + 1) * (users.score - 2)
    sql, params = expr.to_sql()

    assert sql == '("a"."counter" + ?) * ("a"."score" - ?)'
    assert params == (1, 2)
