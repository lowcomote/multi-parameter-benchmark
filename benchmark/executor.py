import argparse
from pathlib import Path
from data.config import Configuration, ApplicationParameters
from deploy.sweep import Sweeper
from deploy.utils import DefaultConfigSerializer
from deploy.sparklib import ClusterReserver, NoopClusterReserver, SparkSubmit, LocalSparkSubmit

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

    # load benchmark config
    config_path = arguments.benchmark_config
    config_text = Path(config_path).read_text()
    config = Configuration.Schema().loads(config_text)

    '''
    1. [done] Parsing arguments.parameters
    Example parameters.json
    {
        "parameters":[{
            "name": "c1",
            "priority": 1,
            "values": ["p1", "p2", "p3"],
            "constraints": [{
               "source": "c1.p1",
               "target": "c2.p4",
               "type": "must/forbidden"  
            }]
        }]
    }
    Parse it as an array and pass it on to the Sweeper that will build the parameter-combinations from it.
    '''
    # load application parameters
    parameters_path = arguments.parameters
    parameters_text = Path(parameters_path).read_text()
    parameters = ApplicationParameters.Schema().loads(parameters_text)

    '''
    3. [done] We submit the application to the spark cluster with these parameters...
    SparkG5kConf:
        - setting JAVA_HOME is optional
        - setting SPARK_HOME is mandatory
        - path to the application JAR is mandatory
        - application class is mandatory
    We could refactor SparkG5kConf and extract the Spark-submit functions so that they can be used for local clusters as well.
    '''
    # Reserve cluster (optional step?)
    cluster_reserver : ClusterReserver = NoopClusterReserver()
    cluster_reserver.start()

    # Start the spark cluster
    # TODO should you deploy to G5k, then copy Spark and the application to your HOME folder, because G5k will upload it to the cluster
    spark_submit : SparkSubmit = LocalSparkSubmit()
    spark_submit.set_spark_path(config.spark_config.spark_home)
    spark_submit.start()

    '''
    2. [done] Sweeper.get_next() returns a HashableDict that we have to transform to a string that will be passed on 
    to the application (--> application-specific serialization?)
    '''
    # setup sweeper
    sweeper = Sweeper(parameters.parameters, remove_workdir=True)
    application_configuration = sweeper.get_next()
    cli_arguments = DefaultConfigSerializer(application_configuration).serialize()

    # In each loop iteration:
    # 1. get the next arguments from param_sweeper
    # 2. submit the application to the cluster with these parameters
    # 3. wait for the application to finish
    # 4. collect the CSVs from the cluster
    # 5. get metrics from the CSVs (see below)
    # 6. save the metrics + the parametrization in the ParamSweeper
    # 7. goto step 1, until ParamSweeper gives next param
    # 8. if ParamSweeper does not give next param, then get (1) the best parametrization from it, (2) the corresponding metrics, (3) all parametrizations and all metrics that have been recorded so far
    # 9. export these results to a file (CSV?)

    # Undeploy (TODO even if an error occurred!)
    spark_submit.stop()
    cluster_reserver.stop()

    '''    
    4. When the application finishes, we collect the logs and the CSVs with the metrics
        Where to look for the CSVs?
            - download from g5k..
        What is the structure of the CSV?
            - header: configuration, metric_name, metric_value
            - example: 
                "param1=v1,param2=v2 ... paramn=vn", "[cpu(op),memory(GB),time(ms)]", "[r1, r2, r3]"
                ...
            
            - header: metric
            - example:

        "param1=1,param2=1 ... paramn=1", "[cpu(op),memory(GB),time(ms)]", "[16732, 2162, 399]"
        "param1=1,param2=1 ... paramn=1", "[cpu(op),memory(GB),time(ms)]", "[111232, 2162, 399]"
        "param1=1,param2=1 ... paramn=1", "[cpu(op),memory(GB),time(ms)]", "[1132612, 2162, 399]"
        We run the measurments n times, we release the csv, and calculate the avg for this specific configuration

        Future discussion: fault tolerance 
    
    6. Anaylse the .csv with R, or external analysis tool
    '''


