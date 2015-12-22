import org.apache.hadoop.fs.FileSystem
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/* Runs the KMeans algorithm.
 *
 */
object ClusterTrips {

  def main(args: Array[String]): Unit = {
    val sc = Utils.getSparkContext(this.getClass.getName)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val sqlContext = new SQLContext(sc)

    val tripsDb = sqlContext.read.parquet(Constants.TripDatabase.toString).cache()
    tripsDb.registerTempTable("trips")

    val pickupPoints = Utils.dataFrameToPickupDropoffVector(
      tripsDb).filter { v =>
        v.toArray.forall(_ != 0.0) // filter out points with clearly incorrect coordinates
      }.filter { v =>
        Constants.NYC_MIN_LAT <= v(0) && v(0) <= Constants.NYC_MAX_LAT &&
          Constants.NYC_MIN_LAT <= v(2) && v(2) <= Constants.NYC_MAX_LAT &&
          Constants.NYC_MIN_LONG <= v(1) && v(1) <= Constants.NYC_MAX_LONG &&
          Constants.NYC_MIN_LONG <= v(3) && v(3) <= Constants.NYC_MAX_LONG
      }.sample(false, 0.25).repartition(32)
    pickupPoints.cache()
    pickupPoints.count()

    fs.delete(Constants.ClusterModel, true)
    val clusters = KMeans.train(pickupPoints, 100, 20)
    clusters.save(sc, Constants.ClusterModel.toString)
  }
}
