from dataclasses import dataclass
import csv
from typing import List
from ..data.metric import Metric

@dataclass
class CsvRecord:
    configuration: str
    metric_name: str
    metric_value: Metric

class CsvReader:

    def __init__(self, csv_path):
        self.csv_path : str = csv_path
        self.csv_records : List[CsvRecord] = []

    def read(self):
        with open(self.csv_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in list(reader):
                configuration = row["configuration"].replace("\"","")
                metric_name = row["metric_name"].replace("\"","")
                metric_value = row["metric_value"].replace("\"","")
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



