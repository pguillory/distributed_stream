# DistributedStream

The Elixir standard library provides [Task.async_stream], which allows you to
use a pool of processes to process a stream. However, these processes will
all be spawned on the local node. Maybe you want to use an entire cluster to
process a stream? You've come to the right place.

[Task.async_stream/3]: https://hexdocs.pm/elixir/Task.html#async_stream/3

## Examples

Here's a short example. We fan_out a stream into a distributed stream, do some
work on it, then fan_in to convert it back to a normal stream.

```elixir
iex> import DistributedStream
iex> [1, 2, 3] |> fan_out() |> map(fn n -> n * 2 end) |> fan_in() |> Enum.to_list()
[2, 4, 6]
```

`fan_out` splits up one stream into many. By default it creates a stream for
every scheduler (i.e. CPU) on every node in the cluster. As it consumes each
value from the input stream, it randomly assigns the value to one of the
output streams.

`map` does work on every value in every stream. This work happens in other
processes potentially on other nodes. (Note that some things like ETS table
access and Process.info care which node they're on.)

`fan_in` merges the distributed streams back into one stream again.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `distributed_stream` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:distributed_stream, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/distributed_stream>.

