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

  test "generate_fan_out_func (deterministic)" do
    fan_out_func = generate_fan_out_func(strategy: :deterministic, concurrency: 2)
    assert fan_out_func.(0) == {node(), 2}
    assert fan_out_func.(1) == {node(), 1}
    assert fan_out_func.(2) == {node(), 1}
    assert fan_out_func.(3) == {node(), 2}
    assert fan_out_func.(4) == {node(), 2}
    assert fan_out_func.(5) == {node(), 1}
    assert fan_out_func.(6) == {node(), 1}
    assert fan_out_func.(7) == {node(), 1}
    assert fan_out_func.(8) == {node(), 1}
    assert fan_out_func.(9) == {node(), 1}
  end

  test "generate_fan_out_func (random)" do
    fan_out_func = generate_fan_out_func(strategy: :random, concurrency: 2)
    partitions = Stream.map(1..1000, fan_out_func) |> Enum.uniq() |> Enum.sort()
    assert partitions == [{node(), 1}, {node(), 2}]
  end

  # test "distribution over cluster" do
  #   nodes = LocalCluster.start_nodes("test", 3, files: [__ENV__.file])

  #   :"manager@127.0.0.1" = node()
  #   [:"test1@127.0.0.1", :"test2@127.0.0.1", :"test3@127.0.0.1"] = nodes

  #   assert [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
  #          |> fan_out(strategy: :deterministic, concurrency: 1)
  #          |> map(&{&1, node()})
  #          |> Enum.sort() == [
  #            {0, :"test1@127.0.0.1"},
  #            {1, :"test2@127.0.0.1"},
  #            {2, :"manager@127.0.0.1"},
  #            {3, :"test3@127.0.0.1"},
  #            {4, :"test3@127.0.0.1"},
  #            {5, :"test2@127.0.0.1"},
  #            {6, :"manager@127.0.0.1"},
  #            {7, :"test2@127.0.0.1"},
  #            {8, :"manager@127.0.0.1"},
  #            {9, :"test2@127.0.0.1"}
  #          ]
  # end
end
