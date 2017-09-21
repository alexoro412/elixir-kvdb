defmodule KVDB.Database do
  use GenServer

  ## Client API

  def start_link(opts) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  ## Logic functions

  defp kvs_into_map(map, []) do
    map
  end

  defp kvs_into_map(map, [k, v | kvs]) do
    kvs_into_map(Map.put(map, k, v), kvs)
  end

  # Returns number of existing keys
  defp exists(map, keys) do
    Enum.reduce(keys, 0, fn(key, acc) ->
        if Map.has_key?(map, key) do
          acc + 1
        else
          acc
        end
      end)
  end

  ## GenServer Callbacks

  def init(:ok) do
    {:ok, %{}}
  end

  # Returns the value, or nil
  def handle_call({:get, key}, _from, map) do
    case Map.fetch(map, key) do
      {:ok, pid} ->
        case KVDB.Entry.type_and_val(pid) do
          {:raw, val} -> {:reply, {:ok, val}, map}
          {:map, _} -> {:reply, {:error, :type_mismatch}, map}
        end
      :error -> {:reply, {:error, :no_key}, map}
    end
  end

  # Returns :ok
  def handle_call({:set, key, value}, _from, map) do
    if Map.has_key?(map, key) do
      :ok = Map.get(map, key)
            |> KVDB.Entry.set(value)
      {:reply, :ok, map}
    else
      # Supervisor
      {:ok, pid} = KVDB.Entry.start_link(value)
      {:reply, :ok, Map.put(map,key,pid)}
    end
  end

  # replies with number of keys existing
  def handle_call({:exists, keys}, _from, map) do
    {:reply, {:ok, exists(map, keys)}, map}
  end

  # replies with number of keys deleted
  def handle_call({:del, keys}, _from, map) do
    new_map = Enum.reduce(map, %{}, fn({k, pid}, new_map) ->
      if k in keys do
        KVDB.Entry.del(pid)
        new_map
      else
        Map.put(new_map, k, pid)
        # %{new_map | k => pid}
      end
    end)

    {:reply, {:ok, exists(map, keys)}, new_map}
  end

  def handle_call({:hdel, map_key, keys}, _from, map) do
    {:ok, pid} = Map.fetch(map, map_key)
    case KVDB.Entry.type_and_val(pid) do
      {:raw, _} -> {:reply, {:error, :type_mismatch}, map}
      {:map, hash_map}
        -> KVDB.Entry.set(pid,
              Map.drop(hash_map, keys))
          {:reply,
              {:ok, exists(hash_map, keys)},
              map}
    end
  end

  def handle_call({:hget, map_key, key}, _from, map) do
    {:ok, pid} = Map.fetch(map, map_key)
    case KVDB.Entry.type_and_val(pid) do
      {:raw, _} -> {:reply, {:error, :type_mismatch}, map}
      {:map, hash_map}
        -> case Map.fetch(hash_map, key) do
          {:ok, val} -> {:reply, {:ok, val}, map}
          :error -> {:reply, {:error, :no_hash_key}, map}
        end
    end
  end

  def handle_call({:hset, map_key, key_values}, _from, map) do
    case Map.fetch(map, map_key) do
      {:ok, pid}
        -> case KVDB.Entry.type_and_val(pid) do
             {:raw, _} -> {:reply, {:error, :type_mismatch}, map}
             {:map, hash_map}
               -> KVDB.Entry.set(pid, kvs_into_map(hash_map, key_values))
                 {:reply, :ok, map}
           end
      :error
        -> {:ok, pid} = KVDB.Entry.start_link(kvs_into_map(%{}, key_values))
            {:reply, :ok, Map.put(map, map_key, pid)}
    end

  end

  def handle_call({:clear}, _from, map) do
    running_agents = Map.values(map)
    Enum.each(running_agents, &KVDB.Entry.del(&1))
    {:reply, :ok, %{}}
  end
end
