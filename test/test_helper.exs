ExUnit.start(exclude: [flight: true])

# Define Mox mocks for behaviours (Flight, ADBC). Use in tests with Application.put_env and Mox.stub/expect.
Mox.defmock(ExArrow.Flight.ClientMock, for: ExArrow.Flight.ClientBehaviour)
Mox.defmock(ExArrow.ADBC.DatabaseMock, for: ExArrow.ADBC.DatabaseBehaviour)
Mox.defmock(ExArrow.ADBC.ConnectionMock, for: ExArrow.ADBC.ConnectionBehaviour)
Mox.defmock(ExArrow.ADBC.StatementMock, for: ExArrow.ADBC.StatementBehaviour)
