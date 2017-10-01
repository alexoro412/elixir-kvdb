defmodule KVDB.Database do
  use GenServer

  # Still left to convert to ETS based
  # CLEAR

  ## Client API

  def start_link(opts) do # opts will be name: :name
    ets_table_name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, ets_table_name, opts)
  end

  ## Logic functions

  defp kvs_into_map(map, []) do
    map
  end

  defp kvs_into_map(map, [k, v | kvs]) do
    kvs_into_map(Map.put(map, k, v), kvs)
  end

  # Returns number of existing keys
  def exists(ets_table_name, keys) do
    num_existing = Enum.reduce(keys, 0, fn(key, acc) ->
        if :ets.member(ets_table_name, key) do
          acc + 1
        else
          acc
        end
      end)
    {:ok, num_existing}
  end

  defp exists_map(map, keys) do
    Enum.reduce(keys, 0, fn(key, acc) ->
      if Map.has_key?(map, key) do
        acc + 1
      else
        acc
      end
    end)
  end

  ## GenServer Callbacks

  def init(ets_table_name) do
    agents = :ets.new(ets_table_name, [:named_table, read_concurrency: true])
    {:ok, {agents, %{}}}
  end

  defp get_agent(ets_table_name, key) do
    case :ets.lookup(ets_table_name, key) do
      [{^key, agent}] -> {:ok, agent}
      [] -> {:error, :no_key}
    end
  end

  # Returns the value, or nil
  def get(ets_table_name, key) do
    case get_agent(ets_table_name, key) do
      {:ok, agent} ->
        case KVDB.Entry.type_and_val(agent) do
          {:raw, val} -> {:ok, val}
          {:map, _} -> {:error, :type_mismatch}
        end
      other -> other
    end
  end

  def hget(ets_table_name, map_key, key) do
    case get_agent(ets_table_name, map_key) do
      {:ok, agent} ->
        case KVDB.Entry.type_and_val(agent) do
          {:raw, _} -> {:error, :type_mismatch}
          {:map, hash_map}
            -> case Map.fetch(hash_map, key) do
                 {:ok, val} -> {:ok, val}
                 :error -> {:error, :no_hash_key}
               end
        end
      other -> other
    end
  end

  # def handle_call({:hget, map_key, key}, _from, {agents, _} = state) do
  #   {:ok, pid} = Map.fetch(agents, map_key)
  #   case KVDB.Entry.type_and_val(pid) do
  #     {:raw, _} -> {:reply, {:error, :type_mismatch}, state}
  #     {:map, hash_map}
  #       -> case Map.fetch(hash_map, key) do
  #         {:ok, val} -> {:reply, {:ok, val}, state}
  #         :error -> {:reply, {:error, :no_hash_key}, state}
  #       end
  #   end
  # end

  # Returns :ok
  def handle_call({:set, key, value}, _from, {ets_table_name, refs} = state) do
    case get_agent(ets_table_name, key) do
      {:ok, agent} ->
        case KVDB.Entry.type(agent) do
          :raw ->
            KVDB.Entry.set(agent, value)
            {:reply, :ok, state}
          :map ->
            {:reply, {:error, :type_mismatch}, state}
        end
      {:error, :no_key} ->
        {:ok, pid} = KVDB.Entry.start_link(value)
        ref = Process.monitor(pid)
        true = :ets.insert(ets_table_name, {key, pid})
        {:reply, :ok, {ets_table_name, Map.put(refs, ref, key)}}
      other -> other
    end
  end

  # # replies with number of keys existing
  # def handle_call({:exists, keys}, _from, {ets_table_name, _} = state) do
  #   {:reply, {:ok, exists(ets_table_name, keys)}, state}
  # end

  # replies with number of keys deleted
  # TODO should this also delete refs?
  def handle_call({:del, keys}, _from, {agents, _refs} = state) do
    {:ok, num_deleted} = exists(agents, keys)

    Enum.each(keys, fn key ->
      case get_agent(agents, key) do
        {:ok, agent} -> KVDB.Entry.del(agent)
        {:error, _} -> nil
      end
      true = :ets.delete(agents, key)
    end)

    {:reply, {:ok, num_deleted}, state}
  end

  def handle_call({:hdel, map_key, keys}, _from, {agents, _} = state) do
    case get_agent(agents, map_key) do
      {:ok, agent} ->
        case KVDB.Entry.type_and_val(agent) do
          {:raw, _} -> {:reply, {:error, :type_mismatch}, state}
          {:map, hash_map}
            -> KVDB.Entry.set(agent, Map.drop(hash_map, keys))
              {:reply, {:ok, exists_map(hash_map, keys)}, state}
        end
      other -> {:reply, other, state}
    end
  end

  def handle_call({:hset, map_key, key_values}, _from, {agents, refs} = state) do
    case get_agent(agents, map_key) do
      {:ok, agent} ->
        case KVDB.Entry.type_and_val(agent) do
          {:raw, _} -> {:reply, {:error, :type_mismatch}, state}
          {:map, hash_map} ->
            KVDB.Entry.set(agent, kvs_into_map(hash_map, key_values))
            {:reply, :ok, state}
        end
      {:error, :no_key} ->
        {:ok, pid} = KVDB.Entry.start_link(kvs_into_map(%{}, key_values))
        ref = Process.monitor(pid)
        true = :ets.insert(agents, {map_key, pid})
        {:reply, :ok, {agents, Map.put(refs, ref, map_key)}}
    end
  end

  # def handle_call({:hset, map_key, key_values}, _from, {agents, refs} = state) do
  #   case Map.fetch(agents, map_key) do
  #     {:ok, pid}
  #       -> case KVDB.Entry.type_and_val(pid) do
  #            {:raw, _} -> {:reply, {:error, :type_mismatch}, state}
  #            {:map, hash_map}
  #              -> KVDB.Entry.set(pid, kvs_into_map(hash_map, key_values))
  #                {:reply, :ok, state}
  #          end
  #     :error
  #       -> {:ok, pid} = KVDB.Entry.start_link(kvs_into_map(%{}, key_values))
  #           ref = Process.monitor(pid)
  #           {:reply, :ok, {Map.put(agents, map_key, pid), Map.put(refs, ref, map_key)}}
  #   end
  # end

  def handle_call({:clear}, _from, {agents, _}) do
    :ets.foldl(fn ({key, pid}, _) ->
      KVDB.Entry.del(pid)
      :ets.delete(agents, key)
    end, 0, agents)
    {:reply, :ok, {agents, %{}}}
    # running_agents = Map.values(agents)
    # Enum.each(running_agents, &KVDB.Entry.del(&1))
    # {:reply, :ok, {%{}, %{}}}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, {agents, refs}) do
    {dead_agent_key, new_refs} = Map.pop(refs, ref)
    :ets.delete(agents, dead_agent_key)
    {:noreply, {agents, new_refs}}
    # if :ets.member(agents, dead_agent_key) do
    #
    #   {:noreply, {, new_refs}}
    # else
    #   {:noreply, {agents, new_refs}}
    # end
  end
end
