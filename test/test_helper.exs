# Exclude adbc (driver optional), adbc_package (adbc + explorer optional),
# adbc_integration (requires live PostgreSQL / DuckDB services), and no_nif
# (requires EX_ARROW_SKIP_NIF=1 — run with: EX_ARROW_SKIP_NIF=1 mix test --only no_nif).
ExUnit.start(exclude: [adbc: true, adbc_package: true, adbc_integration: true, no_nif: true])

# Define Mox mocks for behaviours (Flight, ADBC). Use in tests with Application.put_env and Mox.stub/expect.
Mox.defmock(ExArrow.Flight.ClientMock, for: ExArrow.Flight.ClientBehaviour)
Mox.defmock(ExArrow.ADBC.DatabaseMock, for: ExArrow.ADBC.DatabaseBehaviour)
Mox.defmock(ExArrow.ADBC.ConnectionMock, for: ExArrow.ADBC.ConnectionBehaviour)
Mox.defmock(ExArrow.ADBC.StatementMock, for: ExArrow.ADBC.StatementBehaviour)
Mox.defmock(ExArrow.NimblePoolMock, for: ExArrow.NimblePoolBehaviour)
