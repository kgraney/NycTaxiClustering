import org.apache.hadoop.fs.Path

object Constants {

  val HdfsRoot = new Path("hdfs://e6893-cluster-m/user/kmg/nyc-tlc/")

  val SourceCsvData = Path.mergePaths(HdfsRoot, new Path("/trip-data"))
  val OutputRoot = Path.mergePaths(HdfsRoot, new Path("/out"))
  val TripDatabase = Path.mergePaths(OutputRoot, new Path("/trip-db.parquet"))
  val ClusterModel = Path.mergePaths(OutputRoot, new Path("/trip-cluster-model.parquet"))

  val ClassifiedTrips = Path.mergePaths(OutputRoot, new Path("/classified-trips.parquet"))


  // Latitude/Longitude bounding box around the NYC region.  We consider trips
  // outside this region to be noise.
  val NYC_MIN_LONG = -74.6782
  val NYC_MAX_LONG = -71.808
  val NYC_MIN_LAT = 40.3591
  val NYC_MAX_LAT = 41.6031
}
