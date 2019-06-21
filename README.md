# Project description

There are two parts on a thorough workflow demo: the first is setting the project up, and the second is to make a query for the problem that the [previous project](https://github.com/anshu3769/SmartDevice_DataPipeline) addresses.

This query is defined as follows: patients are users whose heart rates are out of their historical range but did not move in the previous X seconds (`WINDOW_TUMBLING_SECONDS` in .env file). Continuously find them out.

# Current progress

1. Currently, the whole project runs on a **single** machine. 
2. Setup part (the first part) is completed
3. Per-person historical heart rate range is currently hard coded. However, my query still involves joining the historical table (so that I can later iterate this project easily). My plan for addressing this part is: since historical heart rate range is the only information the query needs from the historical table, the historical table for this project can simply store these ranges but nothing else.
4. The querying part of the project worked before but not now. It worked only when number of partitions (`NUM_PARTITIONS` in `.env`) equals to 1. But this doesn't make sense for a large amount of data, so I'm working on it. Since I tweaking around, the querying code likely won't work now.
5. Future plans ordered by priority
  1. Fix the partition bug, so that both parts can work fluently in a thorough demo
  2. Breaks the architecture to multiple machines and do the benchmarking.
  3. Benchmark using built-in kafka streaming. It also needs devOps things setup and configured for many machines.


# Step 1: setup

Firstly bring up 1 m4.large EC2 ubuntu machine with sufficient storage (currently I use 20 GB)

Then ssh into it, and then run everything in `setup/setup.sh`.

If you use [pegasus](https://github.com/InsightDataScience/pegasus), you can do `peg up setup/brokers.yml` and then `peg ssh brokers 1`. Then run the setup script.

# Step 2: run it

On the EC2 machine, run `python3 query.py setup` to setup interim tables and streams, then `python3 query.py run` to see the query result, and finally `python3 query.py teardown` to remove the interim objects.
