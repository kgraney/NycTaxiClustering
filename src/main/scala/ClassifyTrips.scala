import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by kmg on 12/14/15.
  */
object ClassifyTrips {

  class ClusterRoute(index: Int, count: Int, proportion: Double,
                     pLat : Double, pLong : Double, dLat: Double, dLong: Double){
    def getProportion() = proportion

    def toXml() =
      <Folder>
        <name>Cluster {index}</name>
        <description>Cluster {index} (count = {count}, proportion = {proportion})</description>
        <Placemark>
          <LineString>
            <coordinates>
              {pLong},{pLat},0.
              {dLong},{dLat},0.
            </coordinates>
          </LineString>
        </Placemark>
        <Placemark>
          <styleUrl>#pickupPoint</styleUrl>
          <Point>
            <coordinates>
              {pLong},{pLat},0.
            </coordinates>
          </Point>
        </Placemark>
        <Placemark>
          <styleUrl>#dropoffPoint</styleUrl>
          <Point>
            <coordinates>
              {dLong},{dLat},0.
            </coordinates>
          </Point>
        </Placemark>
      </Folder>
  }

  def main(args: Array[String]): Unit = {
    val sc = Utils.getSparkContext(this.getClass.getName)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val sqlContext = new SQLContext(sc)

    val tripsDb = sqlContext.read.parquet(Constants.TripDatabase.toString).cache()
    tripsDb.registerTempTable("trips")

    val clusters = KMeansModel.load(sc, Constants.ClusterModel.toString)

    // Compute the total number of trips that fall into each cluster
    val vectors = Utils.dataFrameToPickupDropoffVector(tripsDb)
    val clusterCounts = clusters.predict(vectors).map {
      x => (x, 1)
    }.reduceByKey(_ + _, 5).sortByKey().cache()

    val clusterFreq = Path.mergePaths(Constants.OutputRoot, new Path("/cluster-frequency"))
    fs.delete(clusterFreq, true)
    clusterCounts.saveAsTextFile(clusterFreq.toString)

    val numTotalTrips = tripsDb.count()

    val pointList = (
      0 until clusters.clusterCenters.size,
      clusterCounts.map{ x => (x._2, x._2.toDouble / numTotalTrips) }.collect(),
      clusters.clusterCenters.map{ x => x.toArray }
      ).zipped.toSeq

    val kmlPoints = pointList.map(
      Function.tupled{(idx, count, x) => new ClusterRoute(idx, count._1, count._2, x(0), x(1), x(2), x(3))})
      .sortWith(_.getProportion > _.getProportion)

    var kml =
      <kml xmlns="http://www.opengis.net/kml/2.2">
        <Document>
          <Style id="pickupPoint">
            <IconStyle>
              <Icon>
                <href>http://maps.google.com/mapfiles/kml/shapes/placemark_circle.png</href>
                <scale>1.0</scale>
              </Icon>
            </IconStyle>
          </Style>
          <Style id="dropoffPoint">
            <IconStyle>
              <Icon>
                <href>http://maps.google.com/mapfiles/kml/shapes/placemark_circle_highlight.png</href>
                <scale>1.0</scale>
              </Icon>
            </IconStyle>
          </Style>
          { kmlPoints map {_.toXml} }
        </Document>
      </kml>

    val clusterPath = Path.mergePaths(Constants.OutputRoot, new Path("/cluster-routes.kml"))
    fs.delete(clusterPath, true)
    val kmlFile = fs.create(clusterPath)
    kmlFile.write(kml.toString.getBytes)
    kmlFile.close()
  }
}
