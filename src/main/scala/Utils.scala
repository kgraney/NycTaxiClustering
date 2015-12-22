import java.io.{BufferedReader, InputStreamReader}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object Utils {

  def stripWhitespace(s: String): String = {
    return s.replaceAll("\\s+", "")
  }

  def readFirstLineStripWhitespace(filePath: String): String = {
    return stripWhitespace(readFirstLine(filePath))
  }

  def readFirstLine(filePath: String): String = {
    val fs = new DistributedFileSystem()
    val conf = new Configuration()
    fs.initialize(new URI("hdfs://e6893-cluster-m"), conf)
    val fhandle = fs.open(new Path(filePath))
    val reader = new BufferedReader(new InputStreamReader(fhandle))
    return reader.readLine()
  }

  // Convert the trips DataFrame to an RDD of Vectors containing the trip
  // pickup and dropoff locations.
  def dataFrameToPickupDropoffVector(d: DataFrame): RDD[Vector] =
    d.select("pickup_latitude", "pickup_longitude",
      "dropoff_latitude", "dropoff_longitude").map { r =>
      Vectors.dense((0 until r.size) map {
        r.getDouble(_)
      } toArray)}


  def getSparkContext(className: String): SparkContext = {
    val conf = new SparkConf().setAppName("%s - NYC Taxi Data Pipeline".format(className))
      .set("spark.app.id", "NYCTaxi")
      .set("spark.driver.memory", "6g")
      .set("spark.executor.memory", "10g")
      .set("spark.akka.frameSize", "50")
    return new SparkContext(conf)
  }
}
