:ok = LocalCluster.start()

Application.ensure_all_started(:distributed_stream)

ExUnit.start()
