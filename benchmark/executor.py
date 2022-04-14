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

    '''
    1. Parsing arguments.parameters
    Example parameters.json
    [
        {
            "name": "c1",
            "priority": 1,
            "values": ["p1", "p2", "p3"],
            "constraints": [
            {
               "source": "c1.p1",
               "target": "c2.p4",
               "type": "must/forbidden"  
            }
            ]
        }
    ]
    Parse it as an array and pass it on to the Sweeper that will build the parameter-combinations from it.
    
    2. Sweeper.get_next() returns a HashtableDict that we have to transform to a string that will be passed on 
    to the application (--> application-specific serialization?)
    
    3. We submit the application to the spark cluster with these parameters...
        SparkG5kConf:
            - setting JAVA_HOME is optional
            - setting SPARK_HOME is mandatory
            - path to the application JAR is mandatory
            - application class is mandatory
        We could refactor SparkG5kConf and extract the Spark-submit functions so that they can be used for local clusters as well.
    
    4. When the application finishes, we collect the logs and the CSVs with the metrics
        Where to look for the CSVs?
            - download from g5k..
        What is the structure of the CSV?
            - header: configuration, metric
            - example: 
                "param1=v1,param2=v2 ... paramn=vn", "46546545"
                "param1=v1,param2=v2 ... paramn=vn", "465461235"
                "param1=v1,param2=v2 ... paramn=vn", "46546541445"
                "param1=v1,param2=v2 ... paramn=vn", "465465445"
            
            - header: metric
            - example:
    '''

