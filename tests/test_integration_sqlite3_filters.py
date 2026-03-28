"""Integration tests for sqlite3 backend."""

import sqlite3
from collections.abc import Iterator
from typing import Any

import pytest

from sql_fusion import Table, select

MIN_USER_AGE = 30
MIN_JOIN_TOTAL = 100
MAX_PENDING_TOTAL = 60


@pytest.fixture
def sqlite_db() -> Iterator[sqlite3.Connection]:
    """Create an in-memory sqlite database for integration tests."""
    connection = sqlite3.connect(":memory:")
    _create_schema(connection)
    _seed_data(connection)

    try:
        yield connection
    finally:
        connection.close()


def _create_schema(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER NOT NULL,
            status TEXT NOT NULL,
            country TEXT NOT NULL,
            email TEXT NOT NULL
        )
        """,
    )
    connection.execute(
        """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            total INTEGER NOT NULL,
            status TEXT NOT NULL
        )
        """,
    )


def _seed_data(connection: sqlite3.Connection) -> None:
    connection.executemany(
        "INSERT INTO users VALUES (?, ?, ?, ?, ?, ?)",
        [
            (1, "Alice", 34, "active", "US", "alice@example.com"),
            (2, "Bob", 28, "inactive", "CA", "bob@example.com"),
            (3, "Carol", 41, "active", "US", "carol@example.com"),
            (4, "Dave", 25, "inactive", "DE", "dave@example.com"),
            (5, "Erin", 36, "pending", "US", "erin@foo.com"),
        ],
    )
    connection.executemany(
        "INSERT INTO orders VALUES (?, ?, ?, ?)",
        [
            (1, 1, 120, "completed"),
            (2, 1, 50, "pending"),
            (3, 3, 200, "completed"),
            (4, 4, 80, "cancelled"),
            (5, 5, 300, "completed"),
            (6, 2, 20, "completed"),
        ],
    )
    connection.commit()


def _fetch_rows(
    connection: sqlite3.Connection,
    sql: str,
    params: tuple[Any, ...],
) -> list[tuple[Any, ...]]:
    return connection.execute(sql, params).fetchall()


def test_complex_user_filter(sqlite_db: sqlite3.Connection) -> None:
    users = Table("users")
    query, params = (
        select(users.id, users.name)
        .from_(users)
        .where(
            (users.age >= MIN_USER_AGE)
            & ((users.status == "active") | (users.status == "pending"))
            & users.country.not_in(["DE", "FR"]),
        )
        .where(
            users.name.like("A%") | users.email.like("%@foo.com"),
        )
        .compile()
    )

    rows = _fetch_rows(sqlite_db, query, params)

    assert sorted(rows) == [(1, "Alice"), (5, "Erin")]


def test_complex_join_filter(sqlite_db: sqlite3.Connection) -> None:
    users = Table("users")
    orders = Table("orders")
    query, params = (
        select(users.id, users.name, orders.id, orders.total)
        .from_(users)
        .join(orders, users.id == orders.user_id)
        .where_by(status="active")
        .where(users.country.in_(["US", "CA"]))
        .where(
            ((orders.status == "completed") & (orders.total >= MIN_JOIN_TOTAL))
            | (
                (orders.status == "pending")
                & (orders.total < MAX_PENDING_TOTAL)
            ),
        )
        .compile()
    )

    rows = _fetch_rows(sqlite_db, query, params)

    assert sorted(rows) == [
        (1, "Alice", 1, 120),
        (1, "Alice", 2, 50),
        (3, "Carol", 3, 200),
    ]


def test_complex_subquery_filter(sqlite_db: sqlite3.Connection) -> None:
    users = Table("users")
    orders = Table("orders")

    completed_users = (
        select(orders.user_id).from_(orders).where_by(status="completed")
    )
    cancelled_users = (
        select(orders.user_id).from_(orders).where_by(status="cancelled")
    )

    query, params = (
        select(users.id, users.name)
        .from_(users)
        .where_by(status="active")
        .where(users.id.in_(completed_users))
        .where(users.id.not_in(cancelled_users))
        .where((users.age >= MIN_USER_AGE) & users.country.in_(["US", "CA"]))
        .compile()
    )

    rows = _fetch_rows(sqlite_db, query, params)

    assert sorted(rows) == [(1, "Alice"), (3, "Carol")]
