defmodule KVDB.Entry do
  def start_link(initial_value) when is_binary(initial_value) do
    Agent.start_link(fn -> {:raw, initial_value} end)
  end

  def start_link(initial_value) when is_map(initial_value) do
    Agent.start_link(fn -> {:map, initial_value} end)
  end

  @doc """
  Returns the type of the stored data.

  Either `:raw` or `:map`
  """
  def type(pid) do
    Agent.get(pid, fn({type, _}) -> type end)
  end

  def type_and_val(pid) do
    Agent.get(pid, &(&1))
  end

  def get(pid) do
    Agent.get(pid, fn({_,val}) -> val end)
  end

  def set(pid, val) do
    Agent.update(pid, fn {type,_} -> {type,val} end)
  end

  def del(pid) do
    Agent.stop(pid)
  end
end
