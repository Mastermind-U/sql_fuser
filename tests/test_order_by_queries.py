"""Tests for ORDER BY support."""

from sql_fusion import Table, func, select


def test_order_by_single_column() -> None:
    users = Table("users")
    query, params = (
        select(users.id, users.name)
        .from_(users)
        .order_by(
            users.name,
        )
        .compile()
    )

    assert query == (
        'SELECT "a"."id", "a"."name" FROM "users" AS "a" ORDER BY "a"."name"'
    )
    assert params == ()


def test_order_by_desc_and_limit_offset() -> None:
    users = Table("users")
    query, params = (
        select(users.id, users.name)
        .from_(users)
        .order_by(users.name, descending=True)
        .limit(10)
        .offset(5)
        .compile()
    )

    assert query == (
        'SELECT "a"."id", "a"."name" '
        'FROM "users" AS "a" '
        'ORDER BY "a"."name" DESC '
        "LIMIT 10 OFFSET 5"
    )
    assert params == ()


def test_order_by_function_call() -> None:
    orders = Table("orders")
    query, params = (
        select(func.count(orders.id))
        .from_(orders)
        .group_by(orders.status)
        .order_by(func.count(orders.id), descending=True)
        .compile()
    )

    assert query == (
        'SELECT COUNT("a"."id") '
        'FROM "orders" AS "a" '
        'GROUP BY "a"."status" '
        'ORDER BY COUNT("a"."id") DESC'
    )
    assert params == ()
