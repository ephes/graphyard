"""Short, materialized read snapshots for related database evidence."""

from contextlib import contextmanager

from django.db import connection, transaction


@contextmanager
def read_snapshot():
    # PostgreSQL READ COMMITTED gives each statement a different snapshot.
    # Own the outer transaction so isolation is set before the first read.
    # durable=True refuses application nesting; only a test-case transaction
    # may already be open, and its isolation can no longer be changed.
    postgres = connection.vendor == "postgresql"
    outermost = not connection.in_atomic_block
    with transaction.atomic(durable=postgres):
        if postgres and outermost:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
                )
        yield
