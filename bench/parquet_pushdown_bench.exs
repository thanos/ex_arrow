# Pushdown vs post-read filter — rough timing helper for announcements.
# Usage: mix run bench/parquet_pushdown_bench.exs

alias ExArrow.Parquet
alias ExArrow.RecordBatch
alias ExArrow.Stream

rows = 100_000
ids = for i <- 1..rows, into: <<>>, do: <<i::little-signed-64>>
scores = for i <- 1..rows, into: <<>>, do: <<:math.fmod(i * 0.01, 1.0)::little-float-64>>

{:ok, batch} = RecordBatch.from_columns(["id", "score"], [ids, scores], ["s64", "f64"], rows)
schema = RecordBatch.schema(batch)
path = Path.join(System.tmp_dir!(), "ex_arrow_pushdown_bench.parquet")
:ok = Parquet.Writer.to_file(path, schema, [batch], compression: :zstd, row_group_size: 10_000)

measure = fn label, fun ->
  {us, result} = :timer.tc(fun)
  IO.puts("#{label}: #{Float.round(us / 1000, 1)} ms -> #{inspect(result)}")
end

measure.("pushdown filter", fn ->
  {:ok, s} = Stream.from_parquet(path, columns: ["id"], filters: {:gt, "score", 0.9})
  Enum.sum(Enum.map(Stream.to_list(s), &RecordBatch.num_rows/1))
end)

measure.("full read + post filter", fn ->
  {:ok, s} = Stream.from_parquet(path)
  s
  |> Stream.to_list()
  |> Enum.map(fn b ->
    {:ok, projected} = ExArrow.Compute.project(b, ["score"])
    # approximate: count rows after a full decode
    RecordBatch.num_rows(projected)
  end)
  |> Enum.sum()
end)

IO.puts("stats after pushdown open:")
{:ok, s} = Stream.from_parquet(path, filters: {:gt, "score", 0.9})
IO.inspect(Parquet.Reader.read_stats(s))
