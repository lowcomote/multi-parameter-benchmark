import argparse
from pathlib import Path
from config import Configuration

"""
INPUT:
  - parameters JSON
  - application path (JAR)
  - number of warmup and measurement rounds

OUTPUT:
  - best configuration and the corresponding metrics
  - all configurations and the corresponding metrics

ALGORITHM:
1. Load parameters from a JSON file.
2. Instantiate Sweeper with these parameters.
3. Get the next config from sweeper.
4. Submit the application to the cluster, wait for it until it finishes, collect the output CSVs after finishing (or upload them somewhere from the application).
  4.1. Rerun the experiments N number of times and collect the metrics.
  4.2. Do not save the CSVs of the warmup rounds, only the measurement rounds.
  4.3. Take the avg/median (with the std) of the measured metrics and save them in a CSV. -> Call these metrisc as "summarized metrics".
5. Save the configuration with the summarized metrics into the Sweeper (or somewhere else).
6. Get the next configuration from the sweeper...
7. Do it until Sweeper finishes or we did enough experiment runs.
8. Return the best configuration, the corresponding metrics values AND the whole metrics results: <config - summarized metrics>
"""


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--parameters", help="Application parameters JSON path", required=True)
    parser.add_argument("-a", "--application", help="Application JAR path", required=True)
    parser.add_argument("-c", "--benchmark_config", help="Benchmark config JSON path", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_arguments()
    config_path = arguments.benchmark_config
    config_text = Path(config_path).read_text()
    config = Configuration.Schema().loads(config_text)
    print(config)
    print(config.benchmark_config.warmup_rounds)
