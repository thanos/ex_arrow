Code.require_file("bench_helper.exs", __DIR__)

IO.puts("\n== Nx <-> Arrow Benchmark ==\n")

if Code.ensure_loaded?(Nx) do
  make_tensors = fn n, dtype ->
    rank1 = Nx.tensor(Enum.to_list(1..n//1), type: dtype)
    rows = max(1, div(n, 4))
    cols = max(2, div(n, rows))
    actual_n = rows * cols
    rank2 = Nx.tensor(Enum.to_list(1..actual_n//1), type: dtype) |> Nx.reshape({rows, cols})
    {rank1, rank2}
  end

  for dtype <- [{:s, 64}, {:f, 64}, {:u, 8}] do
    {r1_1k, r2_1k} = make_tensors.(1_000, dtype)
    {r1_100k, _r2_100k} = make_tensors.(100_000, dtype)
    {r1_1m, _r2_1m} = make_tensors.(1_000_000, dtype)

    dtype_label = inspect(dtype)

    Benchee.run(
      %{
        "from_nx rank-1 #{dtype_label} (1K)" => fn -> ExArrow.from_nx(r1_1k) end,
        "from_nx rank-1 #{dtype_label} (100K)" => fn -> ExArrow.from_nx(r1_100k) end,
        "from_nx rank-1 #{dtype_label} (1M)" => fn -> ExArrow.from_nx(r1_1m) end
      },
      time: 5,
      memory_time: 2,
      formatters: [
        Benchee.Formatters.Console,
        {Benchee.Formatters.HTML, file: Path.join(Bench.DataGen.output_dir(), "nx_from_#{dtype_label}.html")},
        {Benchee.Formatters.JSON, file: Path.join(Bench.DataGen.output_dir(), "nx_from_#{dtype_label}.json")}
      ]
    )

    {:ok, batch_1k} = ExArrow.from_nx(r1_1k)
    {:ok, batch_100k} = ExArrow.from_nx(r1_100k)
    {:ok, batch_1m} = ExArrow.from_nx(r1_1m)

    Benchee.run(
      %{
        "to_nx single-col #{dtype_label} (1K)" => fn -> ExArrow.to_nx(batch_1k) end,
        "to_nx single-col #{dtype_label} (100K)" => fn -> ExArrow.to_nx(batch_100k) end,
        "to_nx single-col #{dtype_label} (1M)" => fn -> ExArrow.to_nx(batch_1m) end
      },
      time: 5,
      memory_time: 2,
      formatters: [
        Benchee.Formatters.Console,
        {Benchee.Formatters.HTML, file: Path.join(Bench.DataGen.output_dir(), "nx_to_#{dtype_label}.html")},
        {Benchee.Formatters.JSON, file: Path.join(Bench.DataGen.output_dir(), "nx_to_#{dtype_label}.json")}
      ]
    )

    {:ok, batch_r2_1k} = ExArrow.from_nx(r2_1k)

    Benchee.run(
      %{
        "from_nx rank-2 #{dtype_label} (1K)" => fn -> ExArrow.from_nx(r2_1k) end,
        "to_nx multi-col #{dtype_label} (1K)" => fn -> ExArrow.to_nx(batch_r2_1k) end
      },
      time: 5,
      memory_time: 2,
      formatters: [
        Benchee.Formatters.Console,
        {Benchee.Formatters.HTML, file: Path.join(Bench.DataGen.output_dir(), "nx_rank2_#{dtype_label}.html")},
        {Benchee.Formatters.JSON, file: Path.join(Bench.DataGen.output_dir(), "nx_rank2_#{dtype_label}.json")}
      ]
    )
  end
else
  IO.puts("Nx not available. Skipping Nx <-> Arrow benchmark.")
end
