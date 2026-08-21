defmodule ExArrow.Parquet.Metadata do
  @moduledoc """
  Parquet footer metadata: row groups, per-column statistics, and key-value
  metadata — without decoding row data.

  ## Example

      {:ok, meta} = ExArrow.Parquet.Metadata.from_file("/data/events.parquet")
      meta.num_row_groups
      Enum.map(meta.row_groups, & &1.num_rows)
  """

  alias ExArrow.Native

  @type column_stats :: %{
          path: String.t(),
          compression: String.t(),
          encodings: [String.t()],
          num_values: integer(),
          min: String.t() | nil,
          max: String.t() | nil
        }

  @type row_group :: %{
          index: non_neg_integer(),
          num_rows: integer(),
          total_byte_size: integer(),
          columns: [column_stats()]
        }

  @type t :: %{
          num_rows: integer(),
          num_row_groups: non_neg_integer(),
          created_by: String.t(),
          row_groups: [row_group()],
          key_value_metadata: [{String.t(), String.t()}]
        }

  @doc """
  Read Parquet footer metadata from a file path.
  """
  @spec from_file(Path.t()) :: {:ok, t()} | {:error, String.t()}
  def from_file(path) when is_binary(path) do
    case Native.parquet_metadata_from_file(path) do
      {:ok, map} -> {:ok, normalise(map)}
      {:error, msg} -> {:error, msg}
    end
  end

  @doc """
  Read Parquet footer metadata from an in-memory binary.
  """
  @spec from_binary(binary()) :: {:ok, t()} | {:error, String.t()}
  def from_binary(binary) when is_binary(binary) do
    case Native.parquet_metadata_from_binary(binary) do
      {:ok, map} -> {:ok, normalise(map)}
      {:error, msg} -> {:error, msg}
    end
  end

  defp normalise(map) when is_map(map) do
    %{
      num_rows: Map.fetch!(map, :num_rows),
      num_row_groups: Map.fetch!(map, :num_row_groups),
      created_by: Map.get(map, :created_by, ""),
      row_groups: Enum.map(Map.get(map, :row_groups, []), &normalise_rg/1),
      key_value_metadata: Map.get(map, :key_value_metadata, [])
    }
  end

  defp normalise_rg(rg) do
    %{
      index: Map.fetch!(rg, :index),
      num_rows: Map.fetch!(rg, :num_rows),
      total_byte_size: Map.fetch!(rg, :total_byte_size),
      columns: Enum.map(Map.get(rg, :columns, []), &normalise_col/1)
    }
  end

  defp normalise_col(col) do
    %{
      path: Map.fetch!(col, :path),
      compression: Map.fetch!(col, :compression),
      encodings: Map.get(col, :encodings, []),
      num_values: Map.fetch!(col, :num_values),
      min: Map.get(col, :min),
      max: Map.get(col, :max)
    }
  end
end
