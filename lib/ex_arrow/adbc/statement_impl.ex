defmodule ExArrow.ADBC.StatementImpl do
  @moduledoc false
  @behaviour ExArrow.ADBC.StatementBehaviour

  alias ExArrow.ADBC.{Connection, Statement}
  alias ExArrow.Stream

  @impl true
  def new(%Connection{resource: conn_ref}) do
    case native().adbc_statement_new(conn_ref) do
      {:ok, ref} -> {:ok, %Statement{resource: ref}}
      {:error, msg} -> {:error, msg}
    end
  end

  @impl true
  def set_sql(%Statement{resource: stmt_ref}, sql) do
    case native().adbc_statement_set_sql(stmt_ref, to_string(sql)) do
      :ok -> :ok
      {:error, msg} -> {:error, msg}
    end
  end

  @impl true
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
