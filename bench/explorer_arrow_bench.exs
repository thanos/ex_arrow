Code.require_file("bench_helper.exs", __DIR__)

IO.puts("\n== Explorer <-> Arrow Benchmark ==\n")

if Code.ensure_loaded?(Explorer.DataFrame) do
  make_df = fn n ->
    Explorer.DataFrame.new(
      x: Enum.to_list(1..n//1),
      y: Enum.map(1..n//1, fn i -> "val_#{i}" end),
      flag: Enum.map(1..n//1, fn i -> rem(i, 2) == 0 end)
    )
  end

  df_1k = make_df.(1_000)
  df_100k = make_df.(100_000)
  df_1m = make_df.(1_000_000)

  Benchee.run(
    %{
      "from_dataframe (1K rows)" => fn -> ExArrow.from_dataframe(df_1k) end,
      "from_dataframe (100K rows)" => fn -> ExArrow.from_dataframe(df_100k) end,
      "from_dataframe (1M rows)" => fn -> ExArrow.from_dataframe(df_1m) end
    },
    time: 5,
    memory_time: 2,
    formatters: [
      Benchee.Formatters.Console,
      {Benchee.Formatters.HTML, file: Path.join(Bench.DataGen.output_dir(), "explorer_from_dataframe.html")},
      {Benchee.Formatters.JSON, file: Path.join(Bench.DataGen.output_dir(), "explorer_from_dataframe.json")}
    ]
  )

  {:ok, batch_1k} = ExArrow.from_dataframe(df_1k)
  {:ok, batch_100k} = ExArrow.from_dataframe(df_100k)
  {:ok, batch_1m} = ExArrow.from_dataframe(df_1m)

  Benchee.run(
    %{
      "to_dataframe (1K rows)" => fn -> ExArrow.to_dataframe(batch_1k) end,
      "to_dataframe (100K rows)" => fn -> ExArrow.to_dataframe(batch_100k) end,
      "to_dataframe (1M rows)" => fn -> ExArrow.to_dataframe(batch_1m) end
    },
    time: 5,
    memory_time: 2,
    formatters: [
      Benchee.Formatters.Console,
      {Benchee.Formatters.HTML, file: Path.join(Bench.DataGen.output_dir(), "explorer_to_dataframe.html")},
      {Benchee.Formatters.JSON, file: Path.join(Bench.DataGen.output_dir(), "explorer_to_dataframe.json")}
    ]
  )
else
  IO.puts("Explorer not available. Skipping Explorer <-> Arrow benchmark.")
end
