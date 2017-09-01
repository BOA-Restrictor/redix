ExUnit.start()

if Code.ensure_loaded?(PropertyTest) do
  Application.ensure_all_started(:stream_data)
end

host = System.get_env("REDIX_TEST_HOST") || "localhost"
port = String.to_integer(System.get_env("REDIX_TEST_PORT") || "6379")

socket_opts = [
  cacertfile: System.get_env("REDIX_TEST_CA_CERT_FILE"),
  certfile: System.get_env("REDIX_TEST_CERT_FILE"),
  keyfile: System.get_env("REDIX_TEST_KEY_FILE")
]

case :ssl.connect(String.to_charlist(host), port, socket_opts) do
  {:ok, socket} ->
    :ssl.close(socket)
  {:error, reason} ->
    Mix.raise "Cannot connect to Redis (http://#{host}:#{port}): #{:ssl.format_error(reason)}"
end

defmodule Redix.TestHelpers do
  def test_host(), do: unquote(host)
  def test_port(), do: unquote(port)
  def test_socket_opts(), do: unquote(socket_opts)

  def parse_with_continuations(data, parser_fun \\ &Redix.Protocol.parse/1)

  def parse_with_continuations([data], parser_fun) do
    parser_fun.(data)
  end

  def parse_with_continuations([first | rest], parser_fun) do
    import ExUnit.Assertions

    {rest, [last]} = Enum.split(rest, -1)

    assert {:continuation, cont} = parser_fun.(first)

    last_cont =
      Enum.reduce(rest, cont, fn data, cont_acc ->
        assert {:continuation, cont_acc} = cont_acc.(data)
        cont_acc
      end)

    last_cont.(last)
  end
end
