defmodule ExArrow.Native do
  @moduledoc false
  use Rustler,
    otp_app: :ex_arrow,
    crate: :ex_arrow_native,
    path: "native/ex_arrow_native",
    mode: if(Mix.env() == :prod, do: :release, else: :debug)

  @doc false
  @spec nif_loaded?() :: boolean()
  def nif_loaded? do
    try do
      _ = nif_version()
      true
    catch
      :error, :nif_not_loaded -> false
    end
  end

  def nif_version, do: :erlang.nif_error(:nif_not_loaded)
  def ipc_test_fixture_binary, do: :erlang.nif_error(:nif_not_loaded)
  def ipc_test_fixture_file_binary, do: :erlang.nif_error(:nif_not_loaded)
  def schema_fields(_schema_ref), do: :erlang.nif_error(:nif_not_loaded)
  def record_batch_schema(_batch_ref), do: :erlang.nif_error(:nif_not_loaded)
  def record_batch_num_rows(_batch_ref), do: :erlang.nif_error(:nif_not_loaded)
  def ipc_reader_from_binary(_data), do: :erlang.nif_error(:nif_not_loaded)
  def ipc_reader_from_file(_path), do: :erlang.nif_error(:nif_not_loaded)
  def ipc_stream_schema(_stream_ref), do: :erlang.nif_error(:nif_not_loaded)
  def ipc_stream_next(_stream_ref), do: :erlang.nif_error(:nif_not_loaded)
  def ipc_file_open(_path), do: :erlang.nif_error(:nif_not_loaded)
  def ipc_file_open_from_binary(_binary), do: :erlang.nif_error(:nif_not_loaded)
  def ipc_file_schema(_file_ref), do: :erlang.nif_error(:nif_not_loaded)
  def ipc_file_num_batches(_file_ref), do: :erlang.nif_error(:nif_not_loaded)
  def ipc_file_get_batch(_file_ref, _index), do: :erlang.nif_error(:nif_not_loaded)
  def ipc_writer_to_binary(_schema_ref, _batches), do: :erlang.nif_error(:nif_not_loaded)
  def ipc_writer_to_file(_path, _schema_ref, _batches), do: :erlang.nif_error(:nif_not_loaded)

  def ipc_file_writer_to_file(_path, _schema_ref, _batches),
    do: :erlang.nif_error(:nif_not_loaded)

  # Flight server
  def flight_server_start(_host, _port), do: :erlang.nif_error(:nif_not_loaded)
  def flight_server_port(_server_ref), do: :erlang.nif_error(:nif_not_loaded)
  def flight_server_host(_server_ref), do: :erlang.nif_error(:nif_not_loaded)
  def flight_server_stop(_server_ref), do: :erlang.nif_error(:nif_not_loaded)

  # Flight client
  def flight_client_connect(_host, _port, _connect_timeout_ms, _tls_mode),
    do: :erlang.nif_error(:nif_not_loaded)

  def flight_client_do_put(_client_ref, _schema_ref, _batches),
    do: :erlang.nif_error(:nif_not_loaded)

  def flight_client_do_get(_client_ref, _ticket_binary), do: :erlang.nif_error(:nif_not_loaded)

  def flight_client_list_flights(_client_ref, _criteria_binary, _timeout_ms),
    do: :erlang.nif_error(:nif_not_loaded)

  def flight_client_get_flight_info(_client_ref, _descriptor, _timeout_ms),
    do: :erlang.nif_error(:nif_not_loaded)

  def flight_client_get_schema(_client_ref, _descriptor, _timeout_ms),
    do: :erlang.nif_error(:nif_not_loaded)

  def flight_client_list_actions(_client_ref, _timeout_ms),
    do: :erlang.nif_error(:nif_not_loaded)

  def flight_client_do_action(_client_ref, _action_type, _action_body, _timeout_ms),
    do: :erlang.nif_error(:nif_not_loaded)
end
