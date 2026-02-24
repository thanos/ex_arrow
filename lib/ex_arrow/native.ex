defmodule ExArrow.Native do
  @moduledoc false
  use Rustler,
    otp_app: :ex_arrow,
    crate: :ex_arrow_native,
    path: "native/ex_arrow_native",
    mode: if(Mix.env() == :prod, do: :release, else: :debug)

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
end
