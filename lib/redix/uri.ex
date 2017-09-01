defmodule Redix.URI do
  @moduledoc false

  defmodule URIError do
    @moduledoc """
    Error in parsing a Redis URI or error in the content of the URI.
    """
    defexception [:message]
  end

  @spec opts_from_uri(binary) :: Keyword.t
  def opts_from_uri(uri) when is_binary(uri) do
    %URI{host: host, port: port, scheme: scheme, userinfo: userinfo, path: path} = URI.parse(uri)

    unless scheme == "redis" || scheme == "rediss" do
      raise URIError, message: "expected scheme to be redis:// or rediss://, got: #{scheme}://"
    end

    reject_nils([
      host: host,
      port: port,
      password: password(userinfo),
      database: database(path),
    ])
  end

  defp password(nil), do: nil
  defp password(userinfo), do: userinfo |> String.split(":", parts: 2) |> List.last

  defp database(nil), do: nil
  defp database("/"), do: nil
  defp database("/" <> db), do: String.to_integer(db)

  defp reject_nils(opts) when is_list(opts) do
    Enum.reject(opts, &match?({_, nil}, &1))
  end
end
