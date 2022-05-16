package csv

import org.apache.commons.csv.{CSVFormat, CSVPrinter, QuoteMode}

import java.io.FileWriter

class CSVWriter(csvPath: String) {

  def createCsv(): Unit = {
    var printer: CSVPrinter = null
    try {
      val csvFormat = CSVFormat.Builder.create().setQuoteMode(QuoteMode.ALL).setHeader("configuration", "metric_name", "metric_value").build()
      val fileWriter = new FileWriter(csvPath)
      printer = new CSVPrinter(fileWriter, csvFormat)
    } finally {
      if (printer != null) {
        printer.close()
      }
    }
  }

  def writeToCsv(configuration: String, metricName: String, metricValue: String): Unit = {
    var csvPrinter: CSVPrinter = null
    try {
      val csvFormat = CSVFormat.Builder.create().setQuoteMode(QuoteMode.ALL).setSkipHeaderRecord(true).build()
      val fileWriter = new FileWriter(csvPath, true)
      csvPrinter = new CSVPrinter(fileWriter, csvFormat)
      csvPrinter.printRecord(configuration, metricName, metricValue)
    } finally {
      if (csvPrinter != null) {
        csvPrinter.close()
      }
    }
  }

}
