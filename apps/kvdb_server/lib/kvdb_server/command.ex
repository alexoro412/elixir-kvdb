defmodule KVDBServer.Command do
  require Logger

  def parse(line) do
    [op | rest] = String.split(line)
    case [String.upcase(op) | rest] do
      ["ASYNC" | rest]
        -> {:ok, cmd} = rest |> Enum.join(" ") |> parse
           {:ok, {:async, cmd}}

      ["GET", key]
        -> {:ok, {:get, key}}

      ["SET", key, value]
        -> {:ok, {:set, key, value}}

      ["EXISTS" | keys] when length(keys) > 0
        -> {:ok, {:exists, keys}}

      ["DEL" | keys] when length(keys) > 0
        -> {:ok, {:del, keys}}

      ["HDEL", map_key | keys] when length(keys) > 0
        -> {:ok, {:hdel, map_key, keys}}

      ["HGET", map_key, key]
        -> {:ok, {:hget, map_key, key}}

      ["HSET", key | rest] when rem(length(rest),2) == 0
        -> {:ok, {:hset, key, rest}}

      ["CLEAR"] -> {:ok, {:clear}}
      _ -> {:error, :syntax}
    end
  end

  def run({:ok, {:async, cmd}}) do
    Task.async(fn -> run({:ok, cmd}) end)
    :ok
  end

  def run({:ok, {:get, key}}) do
    KVDB.Database.get(KVDB.Database, key)
  end

  def run({:ok, {:hget, map_key, key}}) do
    KVDB.Database.hget(KVDB.Database, map_key, key)
  end

  def run({:ok, {:exists, keys}}) do
    KVDB.Database.exists(KVDB.Database, keys)
  end

  def run({:ok, cmd}) do
    GenServer.call(KVDB.Database, cmd, :infinity)
  end

  # Incorrectly parsed commands get sent back and dealt
  # with like errors
  def run(error) do
    error
  end
end
