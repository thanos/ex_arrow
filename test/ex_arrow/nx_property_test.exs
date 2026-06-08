defmodule ExArrow.NxPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  @moduletag :nx

  if Code.ensure_loaded?(Nx) do
    property "rank-1 from_nx/to_nx round-trip preserves shape and values for s64" do
      check all(
              values <- list_of(integer(-1000..1000), min_length: 1, max_length: 100),
              max_runs: 20
            ) do
        tensor = Nx.tensor(values, type: {:s, 64})
        {:ok, batch} = ExArrow.from_nx(tensor)
        {:ok, recovered} = ExArrow.to_nx(batch)
        assert Nx.shape(recovered) == Nx.shape(tensor)
        assert Nx.type(recovered) == {:s, 64}
        assert Nx.to_list(recovered) == values
      end
    end

    property "rank-1 from_nx/to_nx round-trip preserves shape and values for f64" do
      check all(
              values <- list_of(float(), min_length: 1, max_length: 50),
              max_runs: 20
            ) do
        tensor = Nx.tensor(values, type: {:f, 64})
        {:ok, batch} = ExArrow.from_nx(tensor)
        {:ok, recovered} = ExArrow.to_nx(batch)
        assert Nx.shape(recovered) == Nx.shape(tensor)
        assert Nx.type(recovered) == {:f, 64}
        for {a, b} <- Enum.zip(Nx.to_list(recovered), values) do
          assert_in_delta a, b, 0.001
        end
      end
    end

    property "rank-1 from_nx/to_nx round-trip preserves u8 values" do
      check all(
              values <- list_of(integer(0..255), min_length: 1, max_length: 100),
              max_runs: 20
            ) do
        tensor = Nx.tensor(values, type: {:u, 8})
        {:ok, batch} = ExArrow.from_nx(tensor)
        {:ok, recovered} = ExArrow.to_nx(batch)
        assert Nx.shape(recovered) == Nx.shape(tensor)
        assert Nx.type(recovered) == {:u, 8}
        assert Nx.to_list(recovered) == values
      end
    end

    property "rank-2 from_nx/to_nx round-trip preserves shape and values for s64" do
      check all(
              rows <- integer(1..10),
              cols <- integer(2..5),
              max_runs: 10
            ) do
        values = Enum.map(1..(rows * cols), fn i -> rem(i, 1000) end)
        tensor = Nx.reshape(Nx.tensor(values, type: {:s, 64}), {rows, cols})
        {:ok, batch} = ExArrow.from_nx(tensor)
        {:ok, recovered} = ExArrow.to_nx(batch)
        assert Nx.shape(recovered) == {rows, cols}
        assert Nx.type(recovered) == {:s, 64}
        assert Nx.to_list(recovered) == Nx.to_list(tensor)
      end
    end

    property "boolean from_nx/to_nx round-trip preserves values" do
      check all(
              values <- list_of(boolean(), min_length: 1, max_length: 50),
              max_runs: 20
            ) do
        int_values = Enum.map(values, fn true -> 1; false -> 0 end)
        tensor = Nx.tensor(int_values, type: {:u, 8})
        {:ok, batch} = ExArrow.from_nx(tensor, as: :boolean)
        {:ok, recovered} = ExArrow.to_nx(batch)
        assert Nx.type(recovered) == {:u, 8}
        assert Nx.to_list(recovered) == int_values
      end
    end

    property "Schema.Mapper round-trip: nx_dtype -> arrow -> nx_dtype is identity" do
      check all(
              nx_dtype <- one_of([
                constant({:s, 8}),
                constant({:s, 16}),
                constant({:s, 32}),
                constant({:s, 64}),
                constant({:u, 8}),
                constant({:u, 16}),
                constant({:u, 32}),
                constant({:u, 64}),
                constant({:f, 32}),
                constant({:f, 64})
              ]),
              max_runs: 20
            ) do
        {:ok, arrow} = ExArrow.Schema.Mapper.nx_dtype_to_arrow(nx_dtype)
        {:ok, nx_back} = ExArrow.Schema.Mapper.arrow_dtype_to_nx(arrow)
        assert nx_back == nx_dtype
      end
    end
  end
end
