defmodule ExArrow.ExplorerPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  @moduletag :explorer

  if Code.ensure_loaded?(Explorer.DataFrame) do
    property "from_dataframe/to_dataframe round-trip preserves column names and row count" do
      check all(
              n <- integer(1..50),
              col_names <- list_of(string(:alphanumeric, min_length: 1, max_length: 8), min_length: 1, max_length: 5),
              max_runs: 20
            ) do
        unique_names = col_names |> Enum.uniq() |> Enum.take(5)
        data = Enum.map(unique_names, fn name -> {String.to_existing_atom(name), Enum.to_list(1..n//1)} end)
        df = Explorer.DataFrame.new(data)

        {:ok, batch} = ExArrow.from_dataframe(df)
        {:ok, df2} = ExArrow.to_dataframe(batch)

        assert Explorer.DataFrame.n_rows(df2) == n
        assert Enum.sort(Explorer.DataFrame.names(df2)) == Enum.sort(unique_names)
      end
    end

    property "from_dataframe/to_dataframe round-trip preserves integer values" do
      check all(
              values <- list_of(integer(-1000..1000), min_length: 1, max_length: 100),
              max_runs: 20
            ) do
        df = Explorer.DataFrame.new(x: values)
        {:ok, batch} = ExArrow.from_dataframe(df)
        {:ok, df2} = ExArrow.to_dataframe(batch)
        recovered = Explorer.Series.to_list(Explorer.DataFrame.pull(df2, "x"))
        assert recovered == values
      end
    end

    property "from_dataframe/to_dataframe round-trip preserves float values" do
      check all(
              values <- list_of(float(), min_length: 1, max_length: 50),
              max_runs: 20
            ) do
        df = Explorer.DataFrame.new(x: values)
        {:ok, batch} = ExArrow.from_dataframe(df)
        {:ok, df2} = ExArrow.to_dataframe(batch)
        recovered = Explorer.Series.to_list(Explorer.DataFrame.pull(df2, "x"))
        assert length(recovered) == length(values)
        for {a, b} <- Enum.zip(recovered, values) do
          assert_in_delta a, b, 0.001
        end
      end
    end

    property "from_dataframe/to_dataframe round-trip preserves boolean values" do
      check all(
              values <- list_of(boolean(), min_length: 1, max_length: 50),
              max_runs: 20
            ) do
        df = Explorer.DataFrame.new(flags: values)
        {:ok, batch} = ExArrow.from_dataframe(df)
        {:ok, df2} = ExArrow.to_dataframe(batch)
        recovered = Explorer.Series.to_list(Explorer.DataFrame.pull(df2, "flags"))
        assert recovered == values
      end
    end
  end
end
