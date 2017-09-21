defmodule KVDBServer.Application do
  use Application

  def start(_type, _args) do
    children = [
      {Task.Supervisor, name: KVDBServer.TaskSupervisor},
      Supervisor.child_spec(
        {Task, fn -> KVDBServer.accept(4040) end},
        restart: :permanent),
      {KVDB.Database, name: KVDB.Database}
      # Turns into `Task.start_link(fn -> KVDBServer.accept(4040) end)`
    ]

    opts = [strategy: :one_for_one,
      name: KVDBServer.Supervisor] # gives a name so we can find it later
    Supervisor.start_link(children, opts)
  end
end
