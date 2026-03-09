# Exclude adbc (driver optional), adbc_package (adbc + explorer optional), and
# adbc_integration (requires live PostgreSQL / DuckDB services).
# Run with: mix test --include adbc_integration to run integration tests.
ExUnit.start(exclude: [adbc: true, adbc_package: true, adbc_integration: true])

# Define Mox mocks for behaviours (Flight, ADBC). Use in tests with Application.put_env and Mox.stub/expect.
Mox.defmock(ExArrow.Flight.ClientMock, for: ExArrow.Flight.ClientBehaviour)
Mox.defmock(ExArrow.ADBC.DatabaseMock, for: ExArrow.ADBC.DatabaseBehaviour)
Mox.defmock(ExArrow.ADBC.ConnectionMock, for: ExArrow.ADBC.ConnectionBehaviour)
Mox.defmock(ExArrow.ADBC.StatementMock, for: ExArrow.ADBC.StatementBehaviour)
