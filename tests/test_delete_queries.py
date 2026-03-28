"""Tests for DELETE queries."""

from sql_fusion import Table, delete, func, select


def test_delete_returning_all() -> None:
    table = Table("users")
    query, params = delete().from_(table).returning().compile()
    assert query == 'DELETE FROM "users" AS "a" RETURNING *'
    assert params == ()


def test_delete_returning_method_merges_calls() -> None:
    table = Table("users")
    query, params = (
        delete()
        .from_(table)
        .returning(table.id)
        .returning(table.name)
        .compile()
    )
    assert query == 'DELETE FROM "users" AS "a" RETURNING "a"."id", "a"."name"'
    assert params == ()


def test_delete_with_where_and_returning_expression() -> None:
    table = Table("users")
    query, params = (
        delete()
        .from_(table)
        .where(table.status == "inactive")
        .returning(table.id, func.upper(table.email))
        .compile()
    )
    assert query == (
        'DELETE FROM "users" AS "a" '
        'WHERE "a"."status" = ? '
        'RETURNING "a"."id", UPPER("a"."email")'
    )
    assert params == ("inactive",)


def test_delete_where_only_is_allowed() -> None:
    table = Table("users")
    user_id = 3
    query, params = delete().from_(table).where(table.id == user_id).compile()
    assert query == 'DELETE FROM "users" AS "a" WHERE "a"."id" = ?'
    assert params == (user_id,)


def test_delete_with_subquery_in_where_clause() -> None:
    users = Table("users")
    orders = Table("orders")
    paid_order_user_ids = (
        select(orders.user_id).from_(orders).where_by(status="paid")
    )
    query, params = (
        delete()
        .from_(users)
        .where(users.id.in_(paid_order_user_ids))
        .compile()
    )
    assert query == (
        'DELETE FROM "users" AS "a" WHERE "a"."id" IN '
        '(SELECT "b"."user_id" '
        'FROM "orders" AS "b" '
        'WHERE "b"."status" = ?)'
    )
    assert params == ("paid",)
