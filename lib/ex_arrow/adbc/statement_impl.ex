defmodule ExArrow.ADBC.StatementImpl do
  @moduledoc false
  @behaviour ExArrow.ADBC.StatementBehaviour

  alias ExArrow.ADBC.{AdbcPackageManager, Connection, Statement}
  alias ExArrow.Stream

  @impl true
  def new(%Connection{resource: :adbc_package}) do
    case AdbcPackageManager.create_statement() do
      {:ok, ref} -> {:ok, %Statement{resource: {:adbc_package, ref}}}
      {:error, _} = err -> err
    end
  end

  def new(%Connection{resource: conn_ref}) do
    case native().adbc_statement_new(conn_ref) do
      {:ok, ref} -> {:ok, %Statement{resource: ref}}
      {:error, msg} -> {:error, msg}
    end
  end

  @impl true
  def set_sql(%Statement{resource: {:adbc_package, ref}}, sql) do
    AdbcPackageManager.set_statement_sql(ref, to_string(sql))
  end

  def set_sql(%Statement{resource: stmt_ref}, sql) do
    case native().adbc_statement_set_sql(stmt_ref, to_string(sql)) do
      :ok -> :ok
      {:error, msg} -> {:error, msg}
    end
  end

  @impl true
  def bind(%Statement{resource: {:adbc_package, _}}, _batch) do
    {:error, "bind not supported for adbc_package backend"}
  end

  def bind(%Statement{resource: stmt_ref}, %ExArrow.RecordBatch{resource: batch_ref}) do
    case native().adbc_statement_bind(stmt_ref, batch_ref) do
      :ok -> :ok
      {:error, msg} -> {:error, msg}
    end
  end

  @impl true
  def execute(%Statement{resource: {:adbc_package, ref}}) do
    AdbcPackageManager.execute_statement(ref)
  end

  def execute(%Statement{resource: stmt_ref}) do
    case native().adbc_statement_execute(stmt_ref) do
      {:ok, stream_ref} -> {:ok, %Stream{resource: stream_ref, backend: :adbc}}
      {:error, msg} -> {:error, msg}
    end
  end

  defp native do
    Application.get_env(:ex_arrow, :adbc_native, ExArrow.Native)
  end
end
