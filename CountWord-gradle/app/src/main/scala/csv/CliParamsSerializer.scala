package csv

import scala.collection.mutable

object CliParamsSerializer {

  def serialize(args: Array[String]): String = {
    val stringBuilder = new mutable.StringBuilder()
    for (index <- args.indices by 2) {
      val paramName = args(index).replaceFirst("-", "")
      val paramValue = args(index + 1)
      val serialized = "%s=%s".format(paramName, paramValue)
      stringBuilder.append(serialized)

      // only append delimiter if we are not at the last argument
      if (index < args.length - 2) {
        stringBuilder.append(",")
      }
    }
    stringBuilder.toString
  }

}
