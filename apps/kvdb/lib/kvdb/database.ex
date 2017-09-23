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
    {:ok, {%{}, %{}}}
  end

  # Returns the value, or nil
  def handle_call({:get, key}, _from, {agents, _} = state) do
    case Map.fetch(agents, key) do
      {:ok, pid} ->
        case KVDB.Entry.type_and_val(pid) do
          {:raw, val} -> {:reply, {:ok, val}, state}
          {:map, _} -> {:reply, {:error, :type_mismatch}, state}
        end
      :error -> {:reply, {:error, :no_key}, state}
    end
  end

  # Returns :ok
  def handle_call({:set, key, value}, _from, {agents, refs} = state) do
    if Map.has_key?(agents, key) do
      pid = Map.get(agents, key)
      case KVDB.Entry.type_and_val(pid) do
        {:raw, val} -> :ok = KVDB.Entry.set(pid, val)
                       {:reply, :ok, state}
        {:map, _} -> {:reply, {:error, :type_mismatch}, state}
      end
    else
      # Supervisor
      {:ok, pid} = KVDB.Entry.start_link(value)
      ref = Process.monitor(pid)
      {:reply, :ok, {Map.put(agents,key,pid), Map.put(refs,ref,key)}}
    end
  end

  # replies with number of keys existing
  def handle_call({:exists, keys}, _from, {agents, _} = state) do
    {:reply, {:ok, exists(agents, keys)}, state}
  end

  # replies with number of keys deleted
  # TODO should this also delete refs?
  def handle_call({:del, keys}, _from, {agents, refs}) do

    {:reply, {:ok, exists(agents, keys)}, {Enum.reduce(agents, %{}, fn({k, pid}, new_agents) ->
      if k in keys do
        KVDB.Entry.del(pid)
        new_agents
      else
        Map.put(new_agents, k, pid)
        # %{new_map | k => pid}
      end
    end), refs}}
  end

  def handle_call({:hdel, map_key, keys}, _from, {agents, _} = state) do
    {:ok, pid} = Map.fetch(agents, map_key)
    case KVDB.Entry.type_and_val(pid) do
      {:raw, _} -> {:reply, {:error, :type_mismatch}, state}
      {:map, hash_map}
        -> KVDB.Entry.set(pid,
              Map.drop(hash_map, keys))
          {:reply,
              {:ok, exists(hash_map, keys)},
              state}
    end
  end

  def handle_call({:hget, map_key, key}, _from, {agents, _} = state) do
    {:ok, pid} = Map.fetch(agents, map_key)
    case KVDB.Entry.type_and_val(pid) do
      {:raw, _} -> {:reply, {:error, :type_mismatch}, state}
      {:map, hash_map}
        -> case Map.fetch(hash_map, key) do
          {:ok, val} -> {:reply, {:ok, val}, state}
          :error -> {:reply, {:error, :no_hash_key}, state}
        end
    end
  end

  def handle_call({:hset, map_key, key_values}, _from, {agents, refs} = state) do
    case Map.fetch(agents, map_key) do
      {:ok, pid}
        -> case KVDB.Entry.type_and_val(pid) do
             {:raw, _} -> {:reply, {:error, :type_mismatch}, state}
             {:map, hash_map}
               -> KVDB.Entry.set(pid, kvs_into_map(hash_map, key_values))
                 {:reply, :ok, state}
           end
      :error
        -> {:ok, pid} = KVDB.Entry.start_link(kvs_into_map(%{}, key_values))
            ref = Process.monitor(pid)
            {:reply, :ok, {Map.put(agents, map_key, pid), Map.put(refs, ref, map_key)}}
    end
  end

  def handle_call({:clear}, _from, {agents, _}) do
    running_agents = Map.values(agents)
    Enum.each(running_agents, &KVDB.Entry.del(&1))
    {:reply, :ok, {%{}, %{}}}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, {agents, refs}) do
    {dead_agent_key, new_refs} = Map.pop(refs, ref)
    if Map.has_key?(agents, dead_agent_key) do
      {:noreply, {Map.delete(agents, dead_agent_key), new_refs}}
    else
      {:noreply, {agents, new_refs}}
    end
  end
end
