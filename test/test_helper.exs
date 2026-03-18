# Tags excluded by default and how to opt-in:
#
#   adbc           – ADBC native driver optional
#   adbc_package   – requires :adbc + :explorer Hex packages
#   adbc_integration – requires live PostgreSQL / DuckDB services
#                    run with: mix test --include adbc_integration
#   no_nif         – exercises the :nif_not_loaded stubs in ExArrow.Native;
#                    must be run WITHOUT the NIF loaded so stubs are active.
#                    run with: EX_ARROW_SKIP_NIF=1 mix test --include no_nif
#                    (--include overrides the default exclusion; --only would
#                    still exclude the tag because it is in the exclude list)
ExUnit.start(exclude: [adbc: true, adbc_package: true, adbc_integration: true, no_nif: true])

# Define Mox mocks for behaviours (Flight, ADBC). Use in tests with Application.put_env and Mox.stub/expect.
Mox.defmock(ExArrow.Flight.ClientMock, for: ExArrow.Flight.ClientBehaviour)
Mox.defmock(ExArrow.ADBC.DatabaseMock, for: ExArrow.ADBC.DatabaseBehaviour)
Mox.defmock(ExArrow.ADBC.ConnectionMock, for: ExArrow.ADBC.ConnectionBehaviour)
Mox.defmock(ExArrow.ADBC.StatementMock, for: ExArrow.ADBC.StatementBehaviour)
Mox.defmock(ExArrow.NimblePoolMock, for: ExArrow.NimblePoolBehaviour)
