defmodule Redix.Utils do
  @moduledoc false

  require Logger

  alias Redix.Connection.Auth

  # We use exit_on_close: false so that we can consistently close the socket
  # (with :gen_tcp.close/1) in the disconnect/2 callback. If we left the default
  # value of exit_on_close: true and still called :gen_tcp.close/1 in
  # disconnect/2, then we would sometimes close an already closed socket, which
  # is harmless but inconsistent. Credit for this strategy goes to James Fish.
  @socket_opts [:binary, active: false, exit_on_close: false]

  @redis_default_opts [
    host: 'localhost',
    port: 6379,
  ]

  @redix_default_opts [
    socket_opts: [],
    backoff: 2000,
    max_backoff: 30_000,
  ]

  @redis_opts [:host, :port, :password, :database]
  @redix_opts [:socket_opts, :backoff, :sync_connect]

  @default_timeout 5000

  @doc """
  Calls `Connection.start_link/3` on the given `conn_module` after cleaning up
  the given opts.

  `redis_opts` are the options that specify how to connect to the Redis server:
  host, port, password, and database. `other_opts` are a mixture of options to
  tweak the behaviour of the Redix connection (e.g., the backoff time) and
  options to be forwarded to `Connection.start_link/3`.
  """
  @spec start_link(module, Keyword.t, Keyword.t) :: GenServer.on_start
  def start_link(_conn_module, redis_opts, other_opts)
      when is_list(redis_opts) and is_list(other_opts) do
    # `connection_opts` are the opts to be passed to `Connection.start_link/3`.
    # `redix_opts` are the other options to tweak the behaviour of Redix (e.g.,
    # the backoff time).
    {redix_opts, connection_opts} = Keyword.split(other_opts, @redix_opts)

    check_redis_opts(redis_opts)

    redis_opts = Keyword.merge(@redis_default_opts, redis_opts)
    redix_opts = Keyword.merge(@redix_default_opts, redix_opts)
    opts = Keyword.merge(redix_opts, redis_opts)

    size = Application.get_env(:redix, :pool_size, 10)
    max_overflow = Application.get_env(:redix, :pool_max_overflow, 5)
    :poolboy.start_link([worker_module: Redix.Connection.Worker, size: size, max_overflow: max_overflow], [opts, connection_opts])
  end

  def connect(%{opts: opts} = state) do
    {host, port, socket_opts, timeout} = tcp_connection_opts(opts)

    # TODO: let's replace with `with` when we depend on ~> 1.2.
    case :gen_tcp.connect(host, port, socket_opts, timeout) do
      {:ok, socket} ->
        setup_socket_buffers(socket)
        state = %{state | socket: socket}
        case Auth.auth_and_select_db(socket, opts) do
          {:ok, ""} ->
            :inet.setopts(socket, active: :once)
            {:ok, state}
          {:ok, tail} when byte_size(tail) > 0 ->
            {:stop, :unexpected_tail_after_auth, state}
          {:error, reason} ->
            {:stop, reason, state}
        end
      {:error, _reason} = error ->
        error
    end
  end

  @spec format_host(Redix.Connection.state) :: String.t
  def format_host(%{opts: opts} = _state) do
    "#{opts[:host]}:#{opts[:port]}"
  end

  @spec send_reply(Redix.Connection.state, iodata, term) ::
    {:reply, term, Redix.Connection.state} |
    {:disconnect, term, Redix.Connection.state}
  def send_reply(%{socket: socket} = state, data, reply) do
    case :gen_tcp.send(socket, data) do
      :ok ->
        {:reply, reply, state}
      {:error, _reason} = err ->
        {:disconnect, err, state}
    end
  end

  @spec send_noreply(Redix.Connection.state, iodata) ::
    {:noreply, Redix.Connection.state} |
    {:disconnect, term, Redix.Connection.state}
  def send_noreply(%{socket: socket} = state, data) do
    case :gen_tcp.send(socket, data) do
      :ok ->
        {:noreply, state}
      {:error, _reason} = err ->
        {:disconnect, err, state}
    end
  end

  @doc """
  This function unwraps the actual reason if an 'unknown POSIX error' is returned
  from :inet.format_error/1
  """
  @spec format_error(term) :: binary
  def format_error(reason)

  # Apparently :inet.format_error/1 doesn't format :tcp_closed.
  def format_error(:tcp_closed) do
    "TCP connection closed"
  end

  def format_error(reason) do
    case :inet.format_error(reason) do
      'unknown POSIX error' -> inspect(reason)
      message -> List.to_string(message)
    end
  end

  # Extracts the TCP connection options (host, port and socket opts) from the
  # given `opts`.
  defp tcp_connection_opts(opts) do
    host = to_char_list(Keyword.fetch!(opts, :host))
    port = Keyword.fetch!(opts, :port)
    socket_opts = @socket_opts ++ Keyword.fetch!(opts, :socket_opts)
    timeout = opts[:timeout] || @default_timeout

    {host, port, socket_opts, timeout}
  end

  # Setups the `:buffer` option of the given socket.
  defp setup_socket_buffers(socket) do
    {:ok, [sndbuf: sndbuf, recbuf: recbuf, buffer: buffer]} =
      :inet.getopts(socket, [:sndbuf, :recbuf, :buffer])

    buffer = buffer |> max(sndbuf) |> max(recbuf)
    :ok = :inet.setopts(socket, [buffer: buffer])
  end

  defp check_redis_opts(opts) when is_list(opts) do
    Enum.each opts, fn {opt, _value} ->
      unless opt in @redis_opts do
        raise ArgumentError,
          "unknown Redis connection option: #{inspect opt}." <>
          " The first argument to start_link/1 should only" <>
          " contain Redis-specific options (host, port," <>
          " password, database)"
      end
    end
  end
end
