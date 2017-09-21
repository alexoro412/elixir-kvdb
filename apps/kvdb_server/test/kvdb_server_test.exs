defmodule KVDBServerTest do
  use ExUnit.Case
  doctest KVDBServer

  test "greets the world" do
    assert KVDBServer.hello() == :world
  end
end
