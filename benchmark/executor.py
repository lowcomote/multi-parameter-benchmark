import argparse
import time
from pathlib import Path
from data.config import Configuration, ApplicationParameters, SparkConfig, G5kClusterConfig
from sweeper.sweep import Sweeper
from application.config_transformer import ToCliConfigTransformer
from deploy.sparklib import ClusterReserver, NoopClusterReserver, SparkSubmit, LocalSparkSubmit, G5kClusterReserver, \
    G5kSparkSubmit
from application.csv_utils import CsvReader, CsvWriter

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
    G5kClusterReserver:
        - setting JAVA_HOME is optional
        - setting SPARK_HOME is mandatory
        - path to the application JAR is mandatory
        - application class is mandatory
    We could refactor SparkG5kConf (new name: G5kClusterReserver) and extract the Spark-submit functions so that they can be used for local clusters as well.
    '''
    # Reserve cluster (optional step?)
    # g5k_config: G5kClusterConfig = config.cluster_config
    # set_cluster_args = g5k_config.filter_none_fields()
    # cluster_reserver: G5kClusterReserver = G5kClusterReserver(**set_cluster_args)
    cluster_reserver: NoopClusterReserver = NoopClusterReserver()
    cluster_reserver.start()

    # Start the spark cluster
    # TODO should you deploy to G5k, then copy Spark and the application to your HOME folder, because G5k will upload it to the cluster
    # roles = cluster_reserver.roles
    # username = cluster_reserver.username
    # spark_submit: SparkSubmit = G5kSparkSubmit(username=username, roles=roles)
    spark_submit: SparkSubmit = LocalSparkSubmit()

    spark_config: SparkConfig = config.spark_config
    spark_submit.set_spark_path(spark_config.spark_home)
    spark_submit.start()

    '''
    2. [done] Sweeper.get_next() returns a HashableDict that we have to transform to a string that will be passed on 
    to the application (--> application-specific serialization?)
    '''
    # setup sweeper
    # Is train = rerun the application N times with the same config? If not, then we shall repeat the spark_submit N times and take the avg of the results?
    sweeper = Sweeper(parameters.parameters, remove_workdir=True, train=10)
    application_configuration = sweeper.get_next()
    has_next = application_configuration is not None

    metric_name = None  # used in CSV Writer to print the metric name
    while has_next:
        # In each loop iteration:
        # 1. serialize the arguments received from the param sweeper
        cli_arguments = ToCliConfigTransformer(application_configuration).transform()

        # TODO Do we want to run the application N times with the same parametrization? If so, where do we execute the warmup and the benchmark rounds?
        # (The corresponding config parameters are in BenchmarkConfig.warmup_rounds and measurement_rounds.)
        # Yes, we should do it, but do not forget to save it (step 6) and avoid reading the earlier results from the CSV twice (--> delete the CSVs, before processing them).

        # 2. submit the application to the cluster with these parameters
        spark_args = {"deploy-mode": "cluster"}  # see https://stackoverflow.com/a/55178223
        submission_id = spark_submit.submit_with_log(path_jar=spark_config.application_jar_path,
                                                     classname=spark_config.application_classname,
                                                     java_args=cli_arguments, spark_args=spark_args)

        # 3. wait for the application to finish
        # TODO implement a blocking-wait instead (see how we can do it asynchronously in python)
        finished = spark_submit.is_finished(submission_id)
        while not finished:
            time.sleep(10)  # sleep x seconds
            finished = spark_submit.is_finished(submission_id)

        # If the application throws an exception, then mark the config as erroneous
        # how to check if the app threw an exception?
        exception_occurred = False
        if exception_occurred:
            sweeper.skip(application_configuration)
            continue

        '''    
        4. [partially done] When the application finishes, we collect the logs and the CSVs with the metrics
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
        '''

        # 4. collect the CSVs from the cluster
        csv_path = None  # TODO get CSV path from cluster or from local file system
        csv_reader = CsvReader(csv_path)

        # 5. get metrics from the CSVs
        csv_reader.read()
        metric_name = csv_reader.get_metric_name()
        metric = csv_reader.get_summarized_metric()

        # 6. save the metrics + the parametrization in the ParamSweeper
        sweeper.score(application_configuration, metric)
        sweeper.done(application_configuration)

        # 7. get the next parametrization
        application_configuration = sweeper.get_next()
        has_next = application_configuration is not None

    # Export benchmark results
    if sweeper.has_best():
        # 8. if ParamSweeper does not give next param, then get (1) the best parametrization from it, (2) the corresponding metrics, (3) all parametrizations and all metrics that have been recorded so far
        # TODO instead of the sweeper.best() method we need a method which creates a config from the best value bindings
        # i.e. [1,A,a] -> p1=1,p2=A,p3=a; Because sweeper.__scores store the concrete bindings.
        best_config = sweeper.best()
        best_score = sweeper.get_score(best_config)
        print(f"Best score: {best_score}")
        print(f"Best config: {best_config}")

        # 9. export all results to a file (CSV?)
        # Analyze the .csv with R, or external analysis tool
        all_scores_by_config = sweeper.get_all_scores_by_config()
        output_path = config.benchmark_config.output_csv_path
        csv_writer = CsvWriter(output_path, all_scores_by_config, metric_name)
        csv_writer.write()
        print(f"All benchmark results are saved to {output_path}")
    else:
        print("No best configuration was found, check the logs.")

    # Stop Spark cluster
    try:
        print("Stopping Spark cluster")
        spark_submit.stop()
        print("Spark cluster stopped")
    except Exception as err:
        print(f"Exception occurred: {err}")

    # Undeploy computation platform
    try:
        print("Undeploying computation cluster")
        cluster_reserver.stop()
        print("Computation cluster undeployed")
    except Exception as err:
        print(f"Exception occurred: {err}")

    '''
     Future discussion: fault tolerance, parallel execution of multiple configurations?
    '''
