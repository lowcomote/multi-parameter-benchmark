from dataclasses import dataclass
import csv
from typing import List
from benchmark.data.metric import Metric
from benchmark.application.config_transformer import ToCsvConfigTransformer

CSV_HEADERS = ["configuration", "metric_name", "metric_value"]


@dataclass
class CsvRecord:
    configuration: str
    metric_name: str
    metric_value: Metric


class CsvReader:

    def __init__(self, csv_path):
        self.csv_path: str = csv_path
        self.csv_records: List[CsvRecord] = []

    def read(self):
        with open(self.csv_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in list(reader):
                configuration = row[CSV_HEADERS[0]].replace("\"", "")
                metric_name = row[CSV_HEADERS[1]].replace("\"", "")
                metric_value = row[CSV_HEADERS[2]].replace("\"", "")
                metric = Metric.from_string(metric_value)
                record = CsvRecord(configuration=configuration, metric_name=metric_name, metric_value=metric)
                self.csv_records.append(record)

    def get_summarized_metric(self):
        number_of_records = len(self.csv_records)
        if number_of_records == 0:
            return None

        metric = self.csv_records[0].metric_value
        if number_of_records == 1:
            return metric

        for record in self.csv_records[1:]:
            metric += record.metric_value

        return metric / number_of_records

    def get_metric_name(self):
        number_of_records = len(self.csv_records)
        if number_of_records == 0:
            return None
        return self.csv_records[0].metric_name


class CsvWriter:

    def __init__(self, csv_path, scores_by_config, metric_name):
        self.csv_path: str = csv_path
        self.scores_by_config = scores_by_config
        self.metric_name = metric_name

    def write(self):
        with open(self.csv_path, 'w') as file:
            csv_writer = csv.writer(file, quoting=csv.QUOTE_NONNUMERIC)  # write quotes around nonnumeric values
            csv_writer.writerow(CSV_HEADERS)

            for config, score in self.scores_by_config.items():
                config_str = ToCsvConfigTransformer(config).transform()
                score_str = str(score)
                csv_row = [config_str, self.metric_name, score_str]
                csv_writer.writerow(csv_row)
