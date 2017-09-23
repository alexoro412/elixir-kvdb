defmodule KVDBServer.RanchProto do
  use GenServer
  require Logger

  @behaviour :ranch_protocol

  def start_link(ref, socket, transport, _opts) do
    pid = :proc_lib.spawn_link(__MODULE__, :init, [ref, socket, transport])
    {:ok, pid}
  end

  defp format_response(:ok) do
    "OK\r\n"
  end

  defp format_response({:ok, x}) do
    "#{x}\r\n"
  end

  defp format_response({:error, e}) do
    "ERROR #{e |> Atom.to_string() |> String.upcase() |> String.replace("_", " ")}\r\n"
  end

  defp format_response(unknown) do
    Logger.error "so this happened: #{unknown}"
    "Wow, you really messed it up\r\n"
  end

  def init(ref, socket, transport) do
    :ok = :ranch.accept_ack(ref)
    :ok = transport.setopts(socket, [{:active, true}, {:packet, :line}])
    :gen_server.enter_loop(__MODULE__, [], %{socket: socket, transport: transport})
  end

  def handle_info({:tcp, socket, data}, state = %{socket: socket, transport: transport}) do
    response = data
                |> KVDBServer.Command.parse()
                |> KVDBServer.Command.run()
                |> format_response
    transport.send(socket, response)
    {:noreply, state}
  end

  def handle_info({:tcp_closed, socket}, state = %{socket: socket, transport: transport}) do
    transport.close(socket)
    {:stop, :normal, state}
  end

  def handle_info(_, state) do
    IO.puts "so this happened"
    {:stop, :normal, state}
  end
end
