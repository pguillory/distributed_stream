defmodule DistributedStream do
  defstruct [:input_stream, :stages]

  @debug false
  @default_timeout 5000

  defmacro debug(string) do
    if @debug do
      quote do
        IO.puts(unquote(string))
      end
    end
  end

  def fan_out(%__MODULE__{} = distributed_stream) do
    # TODO: redistribute?
    distributed_stream
  end

  def fan_out(stream) do
    fan_out(stream, generate_fan_out_func(strategy: :random))
  end

  @doc false
  def generate_fan_out_func(opts \\ []) when is_list(opts) do
    strategy = Keyword.get(opts, :strategy, :random)
    partitions = partitions(opts) |> List.to_tuple()
    num_partitions = tuple_size(partitions)

    case strategy do
      :random ->
        fn _ ->
          elem(partitions, :rand.uniform(num_partitions) - 1)
        end

      :deterministic ->
        fn value ->
          elem(partitions, :erlang.phash2(value) |> rem(num_partitions))
        end
    end
  end

  @doc false
  def partitions(opts \\ []) when is_list(opts) do
    max_concurrency = Keyword.get(opts, :concurrency, :infinity)
    {responses, _} = :rpc.multicall(__MODULE__, :node_schedulers, [], @default_timeout)

    Enum.flat_map(responses, fn
      :badrpc ->
        []

      {node, schedulers} ->
        concurrency = apply_max_concurrency(schedulers, max_concurrency)
        Enum.map(1..concurrency, &{node, &1})
    end)
  end

  defp apply_max_concurrency(concurrency, :infinity) do
    concurrency
  end

  defp apply_max_concurrency(concurrency, max_concurrency) do
    min(concurrency, max_concurrency)
  end

  @doc false
  def node_schedulers do
    {node(), System.schedulers_online()}
  end

  def fan_out(distributed_stream, opts) when is_list(opts) do
    fan_out(distributed_stream, generate_fan_out_func(opts))
  end

  def fan_out(%__MODULE__{} = distributed_stream, fan_out_func)
      when is_function(fan_out_func, 1) do
    update_in(distributed_stream.stages, fn stages ->
      [{fan_out_func, []} | stages]
    end)
  end

  def fan_out(stream, fan_out_func) when is_function(fan_out_func, 1) do
    %__MODULE__{
      input_stream: stream,
      stages: [{fan_out_func, []}]
    }
  end

  def transform(%__MODULE__{} = distributed_stream, transform_func)
      when is_function(transform_func, 1) do
    update_in(distributed_stream.stages, fn
      [{fan_out_func, transform_funcs} | stages] ->
        [{fan_out_func, [transform_func | transform_funcs]} | stages]
    end)
  end

  def transform(stream, transform_func) do
    transform_func.(stream)
  end

  def map(%__MODULE__{} = distributed_stream, map_func) do
    transform(distributed_stream, fn stream ->
      Stream.map(stream, map_func)
    end)
  end

  def map(stream, map_func) do
    Stream.map(stream, map_func)
  end

  def flat_map(%__MODULE__{} = distributed_stream, map_func) do
    transform(distributed_stream, fn stream ->
      Stream.flat_map(stream, map_func)
    end)
  end

  def flat_map(stream, map_func) do
    Stream.flat_map(stream, map_func)
  end

  def fan_in(%__MODULE__{} = distributed_stream) do
    input_stream = distributed_stream.input_stream
    stages = distributed_stream.stages

    final_pid = self()
    final_node = node()

    final_fan_out_func = fn _ ->
      {final_node, 0}
    end

    {next_fan_out_func, next_router} =
      Enum.reduce(stages, {final_fan_out_func, final_pid}, fn
        {fan_out_func, transform_funcs}, {next_fan_out_func, next_router} ->
          transform_funcs = Enum.reverse(transform_funcs)
          router = spawn_router({next_fan_out_func, next_router}, transform_funcs)
          {fan_out_func, router}
      end)

    spawn_initial_worker({next_fan_out_func, next_router}, input_stream)
    gather_stream()
  end

  def fan_in(stream) do
    stream
  end

  defp spawn_initial_worker({fan_out_func, router}, input_stream) do
    spawn_link(fn ->
      debug("initial worker #{inspect(self())} started")
      distribute_stream(input_stream, fan_out_func, router)
      send(router, {:done, [self()]})
      debug("initial worker #{inspect(self())} finished")
    end)
  end

  defp spawn_router({fan_out_func, router}, transform_funcs) do
    spawn_link(fn ->
      debug("router #{inspect(self())} started")

      distribute_func = fn stream ->
        distribute_stream(stream, fan_out_func, router)
      end

      worker_pids = route(%{}, transform_funcs, distribute_func)
      send(router, {:done, worker_pids})
      debug("router #{inspect(self())} finished")
    end)
  end

  defp distribute_stream(stream, fan_out_func, router) do
    stream
    |> chunk_every_by(1000, fan_out_func)
    |> Enum.reduce(%{}, fn {{node, shard}, values}, node_map ->
      {worker_pid, node_map} =
        case Map.fetch(node_map, {node, shard}) do
          {:ok, worker_pid} ->
            receive do
              {:ready_for_input, ^worker_pid} -> :ok
            after
              @default_timeout -> throw :timeout
            end

            {worker_pid, node_map}

          :error ->
            send(router, {:spawn, self(), node, shard})

            worker_pid =
              receive do
                {:spawned, ^node, ^shard, worker_pid} -> worker_pid
              after
                @default_timeout -> throw :timeout
              end

            node_map = Map.put(node_map, {node, shard}, worker_pid)
            {worker_pid, node_map}
        end

      debug("worker #{inspect(self())} sending #{inspect(values)} to #{inspect(worker_pid)}")
      send(worker_pid, {:input, self(), values})
      node_map
    end)

    # send(router, :done)
  end

  defp route(node_map, transform_funcs, distribute_func) do
    receive do
      {:spawn, input_pid, node, shard} ->
        case Map.fetch(node_map, {node, shard}) do
          {:ok, worker_pid} ->
            send(input_pid, {:spawned, node, shard, worker_pid})
            route(node_map, transform_funcs, distribute_func)

          :error ->
            worker_pid = spawn_worker(node, transform_funcs, distribute_func)
            node_map = Map.put(node_map, {node, shard}, worker_pid)
            send(input_pid, {:spawned, node, shard, worker_pid})
            route(node_map, transform_funcs, distribute_func)
        end

      {:done, previous_layer_worker_pids} ->
        worker_pids = Map.values(node_map)
        Enum.each(worker_pids, &send(&1, {:done, previous_layer_worker_pids}))
        debug("router #{inspect(self())} waiting for #{inspect(worker_pids)}")
        :ok = await_all_pids_to_exit(worker_pids)
        worker_pids
    after
      @default_timeout -> throw :timeout
    end
  end

  defp spawn_worker(node, transform_funcs, distribute_func) do
    Node.spawn(node, fn ->
      debug("worker #{inspect(self())} started")
      stream = gather_stream()
      stream = Enum.reduce(transform_funcs, stream, & &1.(&2))
      distribute_func.(stream)
      debug("worker #{inspect(self())} finished")
    end)
  end

  defp gather_stream do
    start_func = fn ->
      nil
    end

    next_func = fn refs ->
      receive do
        # This clause allows the current process to act as the final router.
        {:spawn, input_pid, node, shard} ->
          worker_pid = self()
          send(input_pid, {:spawned, node, shard, worker_pid})
          {[], refs}

        {:input, pid, values} ->
          debug("worker #{inspect(self())} got #{inspect(values)} from #{inspect(pid)}")
          send(pid, {:ready_for_input, self()})
          {values, refs}

        {:done, []} ->
          {:halt, nil}

        {:done, [_ | _] = pids} ->
          debug("worker #{inspect(self())} waiting for #{inspect(pids)}")
          refs = Map.new(pids, &{Process.monitor(&1), &1})
          {[], refs}

        {:DOWN, ref, _process, pid, _reason} ->
          {^pid, refs} = Map.pop!(refs, ref)

          if map_size(refs) > 0 do
            debug("worker #{inspect(self())} got DOWN from #{inspect(pid)}, continuing")
            {[], refs}
          else
            debug("worker #{inspect(self())} got DOWN from #{inspect(pid)}, halting")
            {:halt, nil}
          end
      end
    end

    after_func = fn _ ->
      nil
    end

    Stream.resource(start_func, next_func, after_func)
  end

  defp chunk_every_by(stream, chunk_size_bytes, fan_out_func) do
    start_func = fn ->
      %{}
    end

    reducer = fn value, acc ->
      group = fan_out_func.(value)
      {size, values} = Map.get(acc, group, {0, []})
      size = size + :erlang.external_size(value)
      values = [value | values]

      if size >= chunk_size_bytes do
        values = Enum.reverse(values)
        chunks = [{group, values}]
        acc = Map.delete(acc, group)
        {chunks, acc}
      else
        chunks = []
        acc = Map.put(acc, group, {size, values})
        {chunks, acc}
      end
    end

    last_func = fn acc ->
      chunks =
        Enum.map(acc, fn {group, {_count, values}} ->
          values = Enum.reverse(values)
          {group, values}
        end)

      {chunks, acc}
    end

    after_func = fn _acc ->
      :ok
    end

    Stream.transform(stream, start_func, reducer, last_func, after_func)
  end

  defp await_all_pids_to_exit(pids, timeout \\ @default_timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout

    pids
    |> Enum.map(&Process.monitor/1)
    |> Enum.each(fn ref ->
      timeout = max(0, deadline - System.monotonic_time(:millisecond))

      receive do
        {:DOWN, ^ref, _, _pid, _reason} -> :ok
      after
        timeout -> throw :timeout
      end
    end)
  end
end
