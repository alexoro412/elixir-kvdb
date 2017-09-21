defmodule KVDBTest do
  use ExUnit.Case
  doctest KVDB

  test "greets the world" do
    assert KVDB.hello() == :world
  end
end
