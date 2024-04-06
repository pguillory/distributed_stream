defimpl Enumerable, for: DistributedStream do
  def count(_) do
    {:error, __MODULE__}
  end

  def member?(_, _) do
    {:error, __MODULE__}
  end

  def slice(_) do
    {:error, __MODULE__}
  end

  def reduce(distributed_stream, acc, fun) do
    stream = DistributedStream.fan_in(distributed_stream)
    Enumerable.reduce(stream, acc, fun)
  end
end
