defmodule KVDBServer do
  require Logger

  @moduledoc """
  Documentation for KVDBServer.
  """

  def accept(port) do
    {:ok, socket} = :gen_tcp.listen(port,
      [:binary,
      packet: :line,
      active: false,
      reuseaddr: true])
    Logger.info "Accepting connections on port #{port}"
    loop_acceptor(socket)
  end

  def loop_acceptor(socket) do
    # Accept a client
    {:ok, client} = :gen_tcp.accept(socket)
    # Start a task to handle the client
    {:ok, pid} = Task.Supervisor.start_child(KVDBServer.TaskSupervisor,
      fn -> serve(client) end)
    # Transfer the socket to the task
    :ok = :gen_tcp.controlling_process(client, pid)
    loop_acceptor(socket)
  end

  defp send_response(:ok, socket) do
    write_line("OK\r\n", socket)
  end

  defp send_response({:ok, x}, socket) do
    write_line("#{x}\r\n", socket)
  end

  defp send_response({:error, e}, socket) do
    write_line("ERROR #{e |> Atom.to_string() |> String.upcase() |> String.replace("_", " ")}\r\n", socket)
  end

  defp send_response(unknown, socket) do
    Logger.error "so this happened: #{unknown}"
    write_line("Wow, you really messed it up", socket)
  end

  # <<packet_size :: size(32), rest :: binary>> = data
  # would get the packet_size as bytes, and then the rest

  # {:ok, <<packet_size :: size(32)>>} = :gen_tcp.recv(client, 4)
  # {:ok, data} = :gen_tcp.recv(packet_size)

  # This works, but maybe look at something like the gloss frame in clojure
  defp serve(socket) do
    read_line(socket)
    |> KVDBServer.Command.parse()
    |> KVDBServer.Command.run()
    |> send_response(socket)

    serve(socket)
    # WOOPS, the following block is irrelevant because of packet: :line in :gen_tcp.listen
    # {cmds, new_state} = cond do
    #   String.ends_with?(new_data, "\r\n") -> # only complete commands
    #     {String.split(new_data, "\r\n"), ""} # ends with command string, so return them all
    #
    #   String.contains?(new_data, "\r\n") -> # some incomplete commands
    #     String.split(new_data, "\r\n")
    #     |> Enum.split(-1)
    #     # cmds become all completed commands, state is remaining incomplete command
    #
    #   _ -> # incomplete command, wait for more data
    #     {[], new_data}
    # end
    #
    # Enum.each(cmds,
    #   fn(cmd) -> KVDBServer.Command.parse(cmd)
    #              |> KVDB.run_cmd() # TODO, also, should this be KVDB or KVDBServer
    #              |> send_response(socket) end)
  end

  defp read_line(socket) do
    # {:ok, data} = :gen_tcp.recv(socket, 0)
    case :gen_tcp.recv(socket, 0) do
      {:ok, data} -> data
      {:error, reason} -> Logger.error "ERROR #{inspect reason}"
                          exit(:abnormal)
    end
  end

  defp write_line(line, socket) do
    :gen_tcp.send(socket, line)
  end
end
