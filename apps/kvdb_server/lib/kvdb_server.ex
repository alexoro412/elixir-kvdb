defmodule KVDBServer do
  def child_spec(opts) do
      %{
        id: __MODULE__,
        start: {__MODULE__, :start_link, [opts]},
        type: :worker,
        restart: :permanent,
        shutdown: 1000
      }
    end

  def start_link(_) do
    :ranch.start_listener(make_ref(), :ranch_tcp, [{:port, 4040}], KVDBServer.RanchProto, [])
  end
end
