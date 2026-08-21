# Optional: decode IPC fixtures from apache/arrow-testing when present.
# Populate with: bash script/fetch_arrow_testing.sh
# Run with:      mix test --include arrow_testing

defmodule ExArrow.ArrowTestingFixtureTest do
  use ExUnit.Case, async: true

  @moduletag :arrow_testing

  @fixture_dir Path.expand("../fixtures/arrow_testing", __DIR__)

  setup do
    fixtures? =
      File.dir?(@fixture_dir) and
        match?([_ | _], Path.wildcard(Path.join(@fixture_dir, "**/*.{arrow,arrows}")))

    if fixtures? do
      :ok
    else
      {:skip, "arrow-testing fixtures not present; run script/fetch_arrow_testing.sh"}
    end
  end

  test "opens at least one IPC fixture without error" do
    files =
      Path.wildcard(Path.join(@fixture_dir, "**/*.{arrow,arrows}")) ++
        Path.wildcard(Path.join(@fixture_dir, "*.arrow"))

    assert files != []
    path = hd(files)

    case ExArrow.IPC.File.from_file(path) do
      {:ok, file} ->
        assert ExArrow.IPC.File.batch_count(file) >= 0

      {:error, _} ->
        # Some corpus files are stream-format; try stream reader.
        bin = File.read!(path)
        assert {:ok, stream} = ExArrow.IPC.Reader.from_binary(bin)
        assert {:ok, _schema} = ExArrow.Stream.schema(stream)
    end
  end
end
