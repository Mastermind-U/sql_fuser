"""Tests for UPDATE queries."""

import pytest

from sql_fusion import Table, update


def test_update_set_returns_sql() -> None:
    table = Table("users")
    query, params = update(table).set(name="Alice", status="active").compile()
    assert (
        query == 'UPDATE "users" AS "a" SET "a"."name" = ?, "a"."status" = ?'
    )
    assert params == ("Alice", "active")


def test_update_set_method_merges_calls() -> None:
    table = Table("users")
    query, params = (
        update(table).set(id=2).set(name="Carol", status="pending").compile()
    )
    assert query == (
        'UPDATE "users" AS "a" '
        'SET "a"."id" = ?, "a"."name" = ?, "a"."status" = ?'
    )
    assert params == (2, "Carol", "pending")


def test_update_with_where_clause() -> None:
    table = Table("users")
    user_id = 5
    query, params = (
        update(table)
        .set(status="inactive")
        .where(table.id == user_id)
        .compile()
    )
    assert (
        query
        == 'UPDATE "users" AS "a" SET "a"."status" = ? WHERE "a"."id" = ?'
    )
    assert params == ("inactive", 5)


def test_update_values_must_be_provided_once() -> None:
    table = Table("users")
    builder = update(table)
    with pytest.raises(ValueError, match="No values provided for update"):
        builder.compile()
    with pytest.raises(ValueError, match="No values provided for update"):
        builder.set().compile()
