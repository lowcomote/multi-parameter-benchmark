import argparse
from benchmark.data.config import Configuration, ApplicationParameters, SparkConfig, G5kClusterConfig, BenchmarkConfig
from benchmark.sweeper.sweep import Sweeper
from benchmark.application.config_transformer import ToCliConfigTransformer, ToCsvConfigTransformer
from benchmark.deploy.sparklib import ClusterReserver, NoopClusterReserver, SparkSubmit, LocalSparkSubmit, \
    G5kClusterReserver, \
    G5kSparkSubmit
from benchmark.application.csv_utils import CsvReader, CsvWriter
from benchmark.data.utils import JsonUtil

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


class BenchmarkExecutor:
    # block spark-submit until the application finishes
    _spark_args = {"deploy-mode": "client"}

    def __init__(self, args):
        self._initialize_configs(args)

    def execute(self):
        self._setup_cluster()
        self._setup_spark()
        self._setup_sweeper()

        self._execute_workflow()

        self._stop_spark()
        self._stop_cluster()

    def _initialize_configs(self, args):
        # load application parameters
        self.application_parameters = JsonUtil.deserialize(args.parameters, ApplicationParameters)

        # load benchmark config
        config = JsonUtil.deserialize(args.benchmarkConfig, Configuration)
        self.spark_config: SparkConfig = config.spark_config

        self.benchmark_config: BenchmarkConfig = config.benchmark_config
        self.all_in_one_csv_path = self.benchmark_config.all_in_one_benchmark_results_csv_path
        self.metrics_csv_param_name = self.benchmark_config.metrics_csv_cli_param_name
        self.path_metrics_csv = self.benchmark_config.metrics_csv_cli_param_value

        if self.metrics_csv_param_name is not None and self.path_metrics_csv is None:
            raise Exception(
                "Set \"application_metrics_csv_path\" in the BenchmarkConfig, because \"application_metrics_csv_param_name\" is set.")

    def _setup_cluster(self):
        print("Reserving the computation cluster.")
        # g5k_config: G5kClusterConfig = config.cluster_config
        # set_cluster_args = g5k_config.filter_none_fields()
        # cluster_reserver: G5kClusterReserver = G5kClusterReserver(**set_cluster_args)
        self.cluster_reserver: NoopClusterReserver = NoopClusterReserver()
        self.cluster_reserver.start()

    def _setup_spark(self):
        # TODO should you deploy to G5k, then copy Spark and the application to your HOME folder, because G5k will upload it to the cluster
        print("Starting Spark on the computation cluster.")
        # roles = cluster_reserver.roles
        # username = cluster_reserver.username
        # spark_submit: SparkSubmit = G5kSparkSubmit(username=username, roles=roles)
        self.spark_submit: SparkSubmit = LocalSparkSubmit()
        self.spark_submit.set_spark_path(self.spark_config.spark_home)
        self.spark_submit.set_java_path(self.spark_config.java_home)
        self.spark_submit.start()

    def _setup_sweeper(self):
        print("Starting the parametrization provider.")
        self.sweeper: Sweeper = Sweeper(application_parameters=self.application_parameters, remove_workdir=True,
                                        train=self.benchmark_config.train)

    def _stop_cluster(self):
        # Undeploy computation platform
        try:
            print("Undeploying computation cluster.")
            self.cluster_reserver.stop()
            print("Computation cluster undeployed.")
        except Exception as err:
            print(f"Exception occurred: {err}")

    def _stop_spark(self):
        try:
            print("Stopping Spark cluster.")
            self.spark_submit.stop()
            print("Spark cluster stopped.")
        except Exception as err:
            print(f"Exception occurred: {err}")

    def _execute_workflow(self):
        metric_name = None  # used in CSV Writer to print the metric name
        while self.sweeper.has_next():
            # In each iteration of the loop:
            # 0. get th next parametrization
            application_configuration = self.sweeper.get_next()

            # 1. Serialize the arguments received from the param sweeper
            cli_arguments = ToCliConfigTransformer(application_configuration).transform()
            log_arguments = ToCsvConfigTransformer(application_configuration).transform()
            print()
            print(f"Deploying spark application with parameters: {log_arguments}")

            # Setup parameters
            if self.metrics_csv_param_name is not None:
                cli_arguments[self.metrics_csv_param_name] = self.path_metrics_csv

            # 2. Warmup rounds: submit the application to the cluster, but discard the results
            for iteration in range(self.benchmark_config.warmup_rounds):
                print()
                print(f"{iteration + 1}. warmup round of {log_arguments}")
                self._submit_application_to_cluster(cli_arguments)

            # 2. Benchmark rounds: submit the application to the cluster, but save the results
            finished_with_error = False
            for iteration in range(self.benchmark_config.measurement_rounds):
                print()
                print(f"{iteration + 1}. benchmark round of {log_arguments}")
                csv_path = self._submit_application_to_cluster(cli_arguments)

                if csv_path is None:
                    finished_with_error = True
                    break

                # 3. Collect the CSVs from the cluster
                print("Reading metrics from CSV.")
                csv_reader = CsvReader(csv_path)

                # 4. Get metrics from the CSVs
                csv_reader.read()
                metric_name = csv_reader.get_metric_name()
                metric = csv_reader.get_summarized_metric()

                # 5. Save the metrics + the parametrization in the ParamSweeper
                print(f"Saving metric ({metric}) to parametrization ({log_arguments}).")
                self.sweeper.score(application_configuration, metric)

            if not finished_with_error:
                self.sweeper.done(application_configuration)
            else:
                print(f"Parametrization ({log_arguments}) finished with error.")
                self.sweeper.skipped(application_configuration)

        print()
        print("Benchmark finished for all parameters. Parametrization provider does not return any new configuration.")
        print()

        # Export benchmark results
        if self.sweeper.has_best():
            # 7. If ParamSweeper does not give next param, then get (1) the best parametrization from it, (2) the corresponding metrics, (3) all parametrizations and all metrics that have been recorded so far
            best_config = self.sweeper.best
            best_score = self.sweeper.get_score(best_config)
            print(f"Best score: {best_score}")
            print(f"Best config: {best_config}")
            print()

            print(f"Configurations skipped due to an error:")
            for config in self.sweeper.skipped_configs:
                print(config)
            else:
                print("-")

            print()
            print("Exporting all benchmark results to a file.")

            # 8. Export all results to a file (CSV?)
            # Analyze the .csv with R, or external analysis tool
            all_scores_by_config = self.sweeper.get_all_scores_by_config()
            output_path = self.benchmark_config.all_in_one_benchmark_results_csv_path
            csv_writer = CsvWriter(output_path, all_scores_by_config, metric_name)
            csv_writer.write()
            print(f"All benchmark results are saved to {output_path}")
        else:
            print("No best configuration was found, check the logs.")

    def _submit_application_to_cluster(self, cli_arguments: dict):
        return self.spark_submit.submit_with_log(path_jar=self.spark_config.application_jar_path,
                                                 classname=self.spark_config.application_classname,
                                                 spark_args=BenchmarkExecutor._spark_args, java_args=cli_arguments,
                                                 path_metrics_csv=cli_arguments[self.metrics_csv_param_name])


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--parameters", help="Application parameters JSON path", required=True)
    parser.add_argument("-c", "--benchmarkConfig", help="Benchmark config JSON path", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_arguments()
    executor = BenchmarkExecutor(arguments)
    executor.execute()
