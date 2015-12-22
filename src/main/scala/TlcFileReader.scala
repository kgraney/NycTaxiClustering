/**
  * Created by kmg on 11/19/15.
  */

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scala.collection.Map


class TlcFileReader(sc: SparkContext, sqlContext: SQLContext, csvFilePath: Path) {
  val fs = FileSystem.get(sc.hadoopConfiguration)

  // Returns lists of CSV file paths by schema
  def getFileGroups(): Map[String, Iterable[String]] = {
    val csvHeaderMapRDD = sc.parallelize(getCsvFilePaths)
      .map(x => (Utils.readFirstLine(x), x))
      .groupByKey()
    return csvHeaderMapRDD.collectAsMap
  }

  def getCsvFilePaths(): Array[String] = {
    return fs.listStatus(csvFilePath).map(x => x.getPath.toString)
  }
}
