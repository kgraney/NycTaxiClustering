import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/* Counts the total number of trips present in the trip database.
 */
object CountTrips {
  def main(args: Array[String]): Unit = {
    val sc = Utils.getSparkContext(this.getClass.getName)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val sqlContext = new SQLContext(sc)

    val tripsDb = sqlContext.read.parquet(Constants.TripDatabase.toString).cache()
    val count = tripsDb.count()
    println("Total number of trips = %d", count)
  }
}
