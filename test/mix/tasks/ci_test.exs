defmodule Mix.Tasks.CiTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  # Script path is overridable via application config for testing.
  setup do
    stub_path = Path.join(File.cwd!(), "test/support/ci_stub.sh")
    on_exit(fn -> Application.delete_env(:ex_arrow, :ci_script_path) end)
    %{stub_path: stub_path}
  end

  test "run/1 executes script and succeeds when script exits 0", %{stub_path: stub_path} do
    Application.put_env(:ex_arrow, :ci_script_path, stub_path)
    # Task returns nil on success (no raise)
    capture_io(fn -> assert Mix.Tasks.Ci.run([]) == nil end)
  end

  test "run/1 raises when script path does not exist" do
    Application.put_env(:ex_arrow, :ci_script_path, "/nonexistent/script/ci")

    assert_raise Mix.Error, ~r/CI script not found/, fn ->
      Mix.Tasks.Ci.run([])
    end
  end

  test "run/1 raises when script exits non-zero" do
    fail_stub = Path.join(File.cwd!(), "test/support/ci_stub_fail.sh")
    Application.put_env(:ex_arrow, :ci_script_path, fail_stub)

    capture_io(fn ->
      assert_raise Mix.Error, ~r/CI script exited with code 1/, fn ->
        Mix.Tasks.Ci.run([])
      end
    end)
  end
end
