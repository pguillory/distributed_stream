defmodule DistributedStreamTest do
  import DistributedStream
  use ExUnit.Case, async: true

  test "fan_out, transform, fan_in" do
    output =
      [1, 2, 3, 4, 5]
      |> fan_out()
      |> transform(fn stream ->
        stream
        |> Stream.map(fn x -> x * 2 end)
        |> Stream.map(fn x -> x + 10 end)
      end)
      |> fan_in()
      |> Enum.sort()

    assert output == [12, 14, 16, 18, 20]
  end
end
