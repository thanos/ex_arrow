defmodule ExArrow.Schema.Mapper do
  @moduledoc """
  Bidirectional mapping between Arrow type representations and external type
  systems.

  ExArrow interacts with several Elixir libraries that have their own type
  systems — Explorer, Nx, and in the future ExZarr and Dataset.  This module
  is the single authority for converting between Arrow dtype strings (used by
  the NIF layer) and each external representation, eliminating duplicated
  mapping logic across bridge modules.

  ## Arrow dtype strings

  The NIF layer identifies column types with short string codes:

  | Code     | Arrow type  |
  |----------|-------------|
  | `"s8"`   | Int8        |
  | `"s16"`  | Int16       |
  | `"s32"`  | Int32       |
  | `"s64"`  | Int64       |
  | `"u8"`   | UInt8       |
  | `"u16"`  | UInt16      |
  | `"u32"`  | UInt32      |
  | `"u64"`  | UInt64      |
  | `"f32"`  | Float32     |
  | `"f64"`  | Float64     |
  | `"bool"` | Boolean     |
  | `"utf8"` | Utf8        |

  These are the canonical internal representation.  All public conversion
  functions accept and return these strings.

  ## Extensibility

  New external targets (e.g. ExZarr, Dataset) can be added by introducing
  new `target_dtype_to_arrow/1` and `arrow_dtype_to_target/1` clause groups.
  The existing targets are grouped by module section below.
  """

  @type arrow_dtype :: String.t()

  @doc """
  Convert an Nx dtype tuple to an Arrow dtype string.

  Returns `{:ok, dtype_string}` or `{:error, message}`.

  ## Supported Nx dtypes

  | Nx dtype     | Arrow dtype |
  |--------------|-------------|
  | `{:s, 8}`    | `"s8"`      |
  | `{:s, 16}`   | `"s16"`     |
  | `{:s, 32}`   | `"s32"`     |
  | `{:s, 64}`   | `"s64"`     |
  | `{:u, 8}`    | `"u8"`      |
  | `{:u, 16}`   | `"u16"`     |
  | `{:u, 32}`   | `"u32"`     |
  | `{:u, 64}`   | `"u64"`     |
  | `{:f, 32}`   | `"f32"`     |
  | `{:f, 64}`   | `"f64"`     |

  Nx does not have a dedicated boolean dtype; booleans are represented as
  `{:u, 8}` with values 0 and 1.  Arrow Boolean columns map to `{:u, 8}` via
  `arrow_dtype_to_nx/1`.
  """
  @spec nx_dtype_to_arrow(Nx.dtype()) :: {:ok, arrow_dtype()} | {:error, String.t()}
  def nx_dtype_to_arrow({:s, 8}), do: {:ok, "s8"}
  def nx_dtype_to_arrow({:s, 16}), do: {:ok, "s16"}
  def nx_dtype_to_arrow({:s, 32}), do: {:ok, "s32"}
  def nx_dtype_to_arrow({:s, 64}), do: {:ok, "s64"}
  def nx_dtype_to_arrow({:u, 8}), do: {:ok, "u8"}
  def nx_dtype_to_arrow({:u, 16}), do: {:ok, "u16"}
  def nx_dtype_to_arrow({:u, 32}), do: {:ok, "u32"}
  def nx_dtype_to_arrow({:u, 64}), do: {:ok, "u64"}
  def nx_dtype_to_arrow({:f, 32}), do: {:ok, "f32"}
  def nx_dtype_to_arrow({:f, 64}), do: {:ok, "f64"}

  def nx_dtype_to_arrow(dtype),
    do: {:error, "unsupported Nx dtype for Arrow conversion: #{inspect(dtype)}"}

  @doc """
  Convert an Arrow dtype string to an Nx dtype tuple.

  Returns `{:ok, nx_dtype}` or `{:error, message}`.

  Boolean columns (`"bool"`) map to `{:u, 8}` because Nx represents booleans
  as unsigned 8-bit integers with values 0 and 1.
  """
  @spec arrow_dtype_to_nx(arrow_dtype()) :: {:ok, Nx.dtype()} | {:error, String.t()}
  def arrow_dtype_to_nx("s8"), do: {:ok, {:s, 8}}
  def arrow_dtype_to_nx("s16"), do: {:ok, {:s, 16}}
  def arrow_dtype_to_nx("s32"), do: {:ok, {:s, 32}}
  def arrow_dtype_to_nx("s64"), do: {:ok, {:s, 64}}
  def arrow_dtype_to_nx("u8"), do: {:ok, {:u, 8}}
  def arrow_dtype_to_nx("u16"), do: {:ok, {:u, 16}}
  def arrow_dtype_to_nx("u32"), do: {:ok, {:u, 32}}
  def arrow_dtype_to_nx("u64"), do: {:ok, {:u, 64}}
  def arrow_dtype_to_nx("f32"), do: {:ok, {:f, 32}}
  def arrow_dtype_to_nx("f64"), do: {:ok, {:f, 64}}
  def arrow_dtype_to_nx("bool"), do: {:ok, {:u, 8}}

  def arrow_dtype_to_nx(dtype),
    do: {:error, "unsupported Arrow dtype for Nx conversion: #{dtype}"}

  @doc """
  Convert an Explorer dtype atom to an Arrow dtype string.

  Returns `{:ok, dtype_string}` or `{:error, message}`.

  ## Supported Explorer dtypes

  | Explorer dtype | Arrow dtype | Notes                          |
  |----------------|-------------|--------------------------------|
  | `:integer`     | `"s64"`     | Explorer stores as 64-bit int  |
  | `:float`       | `"f64"`     | Explorer stores as 64-bit float|
  | `:boolean`     | `"bool"`    | Arrow Boolean column           |
  | `:string`      | `"utf8"`    | Arrow Utf8 column              |

  Explorer dtypes `:date`, `:datetime`, `:duration`, and `:nil` are not yet
  mapped and return an error.  These will be added as the NIF layer gains
  support for the corresponding Arrow types.
  """
  @spec explorer_dtype_to_arrow(atom()) :: {:ok, arrow_dtype()} | {:error, String.t()}
  def explorer_dtype_to_arrow(:integer), do: {:ok, "s64"}
  def explorer_dtype_to_arrow(:float), do: {:ok, "f64"}
  def explorer_dtype_to_arrow(:boolean), do: {:ok, "bool"}
  def explorer_dtype_to_arrow(:string), do: {:ok, "utf8"}

  def explorer_dtype_to_arrow(dtype),
    do: {:error, "unsupported Explorer dtype for Arrow conversion: #{inspect(dtype)}"}

  @doc """
  Convert an Arrow dtype string to an Explorer dtype atom.

  Returns `{:ok, explorer_dtype}` or `{:error, message}`.

  Integer dtypes (`s8`–`s64`, `u8`–`u64`) all map to `:integer` because Explorer
  does not distinguish integer widths in its dtype system.  Float dtypes (`f32`,
  `f64`) map to `:float`.
  """
  @spec arrow_dtype_to_explorer(arrow_dtype()) :: {:ok, atom()} | {:error, String.t()}
  def arrow_dtype_to_explorer("s8"), do: {:ok, :integer}
  def arrow_dtype_to_explorer("s16"), do: {:ok, :integer}
  def arrow_dtype_to_explorer("s32"), do: {:ok, :integer}
  def arrow_dtype_to_explorer("s64"), do: {:ok, :integer}
  def arrow_dtype_to_explorer("u8"), do: {:ok, :integer}
  def arrow_dtype_to_explorer("u16"), do: {:ok, :integer}
  def arrow_dtype_to_explorer("u32"), do: {:ok, :integer}
  def arrow_dtype_to_explorer("u64"), do: {:ok, :integer}
  def arrow_dtype_to_explorer("f32"), do: {:ok, :float}
  def arrow_dtype_to_explorer("f64"), do: {:ok, :float}
  def arrow_dtype_to_explorer("bool"), do: {:ok, :boolean}
  def arrow_dtype_to_explorer("utf8"), do: {:ok, :string}

  def arrow_dtype_to_explorer(dtype),
    do: {:error, "unsupported Arrow dtype for Explorer conversion: #{dtype}"}

  @doc """
  Convert an Arrow type atom (as returned by `ExArrow.Schema.fields/1`) to an
  Arrow dtype string.

  Returns `{:ok, dtype_string}` or `{:error, message}`.

  This bridges the NIF schema representation (atoms like `:int64`) to the
  dtype strings used by column buffer NIFs (`"s64"`).

  ## Examples

      iex> ExArrow.Schema.Mapper.arrow_type_atom_to_dtype(:int64)
      {:ok, "s64"}

      iex> ExArrow.Schema.Mapper.arrow_type_atom_to_dtype(:boolean)
      {:ok, "bool"}

      iex> ExArrow.Schema.Mapper.arrow_type_atom_to_dtype(:timestamp)
      {:error, "unsupported Arrow type atom for dtype mapping: timestamp"}
  """
  @spec arrow_type_atom_to_dtype(atom()) :: {:ok, arrow_dtype()} | {:error, String.t()}
  def arrow_type_atom_to_dtype(:boolean), do: {:ok, "bool"}
  def arrow_type_atom_to_dtype(:int8), do: {:ok, "s8"}
  def arrow_type_atom_to_dtype(:int16), do: {:ok, "s16"}
  def arrow_type_atom_to_dtype(:int32), do: {:ok, "s32"}
  def arrow_type_atom_to_dtype(:int64), do: {:ok, "s64"}
  def arrow_type_atom_to_dtype(:uint8), do: {:ok, "u8"}
  def arrow_type_atom_to_dtype(:uint16), do: {:ok, "u16"}
  def arrow_type_atom_to_dtype(:uint32), do: {:ok, "u32"}
  def arrow_type_atom_to_dtype(:uint64), do: {:ok, "u64"}
  def arrow_type_atom_to_dtype(:float32), do: {:ok, "f32"}
  def arrow_type_atom_to_dtype(:float64), do: {:ok, "f64"}
  def arrow_type_atom_to_dtype(:utf8), do: {:ok, "utf8"}
  def arrow_type_atom_to_dtype(:large_utf8), do: {:ok, "utf8"}

  def arrow_type_atom_to_dtype(atom),
    do: {:error, "unsupported Arrow type atom for dtype mapping: #{atom}"}

  @doc """
  Convert an Arrow dtype string to an Arrow type atom.

  Returns `{:ok, type_atom}` or `{:error, message}`.

  ## Examples

      iex> ExArrow.Schema.Mapper.arrow_dtype_to_type_atom("s64")
      {:ok, :int64}

      iex> ExArrow.Schema.Mapper.arrow_dtype_to_type_atom("bool")
      {:ok, :boolean}
  """
  @spec arrow_dtype_to_type_atom(arrow_dtype()) :: {:ok, atom()} | {:error, String.t()}
  def arrow_dtype_to_type_atom("s8"), do: {:ok, :int8}
  def arrow_dtype_to_type_atom("s16"), do: {:ok, :int16}
  def arrow_dtype_to_type_atom("s32"), do: {:ok, :int32}
  def arrow_dtype_to_type_atom("s64"), do: {:ok, :int64}
  def arrow_dtype_to_type_atom("u8"), do: {:ok, :uint8}
  def arrow_dtype_to_type_atom("u16"), do: {:ok, :uint16}
  def arrow_dtype_to_type_atom("u32"), do: {:ok, :uint32}
  def arrow_dtype_to_type_atom("u64"), do: {:ok, :uint64}
  def arrow_dtype_to_type_atom("f32"), do: {:ok, :float32}
  def arrow_dtype_to_type_atom("f64"), do: {:ok, :float64}
  def arrow_dtype_to_type_atom("bool"), do: {:ok, :boolean}
  def arrow_dtype_to_type_atom("utf8"), do: {:ok, :utf8}

  def arrow_dtype_to_type_atom(dtype),
    do: {:error, "unsupported Arrow dtype for type atom conversion: #{dtype}"}

  @doc """
  Returns `true` if the given Arrow dtype string maps to a numeric Nx dtype,
  `false` otherwise.

  ## Examples

      iex> ExArrow.Schema.Mapper.numeric?("s64")
      true

      iex> ExArrow.Schema.Mapper.numeric?("bool")
      true

      iex> ExArrow.Schema.Mapper.numeric?("utf8")
      false
  """
  @spec numeric?(arrow_dtype()) :: boolean()
  def numeric?("s8"), do: true
  def numeric?("s16"), do: true
  def numeric?("s32"), do: true
  def numeric?("s64"), do: true
  def numeric?("u8"), do: true
  def numeric?("u16"), do: true
  def numeric?("u32"), do: true
  def numeric?("u64"), do: true
  def numeric?("f32"), do: true
  def numeric?("f64"), do: true
  def numeric?("bool"), do: true
  def numeric?(_), do: false
end
