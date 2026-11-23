"""Transactional, sequential schema migrations for private SQLite stores."""


class UnsupportedSchemaError(ValueError):
    """The database version is outside this release's supported range."""


def migrate_schema(connection, store, migrations, *, prepare=None):
    """Apply version N at migrations[N - 1] and publish versions on commit."""
    latest = len(migrations)

    def read_version():
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        if not 0 <= version <= latest:
            raise UnsupportedSchemaError(
                f"Unsupported {store} schema version {version}. This release supports 0 through {latest}."
            )
        return version

    if read_version() == latest:
        return
    if prepare is not None:
        prepare(connection)
    with connection:
        connection.execute("BEGIN IMMEDIATE")
        # A concurrent initializer may have committed while this one waited.
        current = read_version()
        for version, migrate in enumerate(migrations[current:], start=current + 1):
            migrate(connection)
            connection.execute(f"PRAGMA user_version = {version}")


def apply_ddl(connection, statements):
    """Run static DDL without executescript's implicit transaction commit."""
    for statement in statements.split(";"):
        if statement.strip():
            connection.execute(statement)


def add_missing_columns(connection, table, definitions):
    """Apply static column definitions to older supported layouts."""
    existing = {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}
    for name, definition in definitions:
        if name not in existing:
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")
