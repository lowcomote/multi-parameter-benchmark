# Multi-Parameter Benchmark Framework

A framework prototype to benchmark Spark applications with multiple parameters.

## Setup

1. Install Python 3.8 or a newer version.
2. Install required Python packages: `pip install -r python/benchmark/requirements.txt`
3. [Download Spark](https://spark.apache.org/downloads.html) on the machine where you want to start the Spark cluster.
4. Copy the application you want to benchmark to the machine where the cluster is going to be deployed.
5. Create the benchmark and cluster configuration file (`config.json`) and the application parameters config file (`parameters.json`). As an example see `python/examples/` or `python/eaxmples/CountWord`).
6. Run the benchmark executor with these configuration files: `python --file python/executor.py -c python/examples/config.json -p python/examples/parameters.json`

As an example application you may use CountWord in this repository.

1. Create a fat jar from the application: `CountWord-gradle/gradlew shadowJar`
2. Copy the fat jar to the Spark cluster: `cp CountWord-gradle/app/build/libs/ParametrizableCountWord.jar <spark-cluster-path>`
3. Copy the `bible.txt` to the Spark cluster: `cp bible.txt <spark-cluster-path>`
4. Adapt the `python/examples/CountWord/config.json` and `python/examples/CountWord/parameters.json` according to your Spark cluster.
5. Run the benchmark: `python --file python/executor.py -c python/examples/CountWord/config.json -p python/examples/CountWord/parameters.json`
