/**
  * A Spark application that reads the NYC TLC trip data CSV data set and
  * outputs DataFrames that represent the trip data.
  */

/// hadoop fs -ls nyc-tlc/trip-data/ | awk '{print $8}' | xargs -I {} bash -c "hadoop fs -cat {} | head | hadoop fs -put - tmp/{}"

/// gcloud beta dataproc jobs submit spark --cluster e6893-cluster --jars gs://kmg/nyctaxi_2.10-1.0.jar --class ConvertCsvToParquet

import java.text.SimpleDateFormat

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}


object ConvertCsvToParquet {

  abstract class ColumnSpec {

    def name: String

    def compare(that: ColumnSpec): Int = this.name compare that.name
  }

  case class StringColumnSpec(name: String) extends ColumnSpec

  case class IntegerColumnSpec(name: String) extends ColumnSpec

  case class DoubleColumnSpec(name: String) extends ColumnSpec

  case class TimestampColumnSpec(name: String) extends ColumnSpec

  val columnNameEquivilence = Map(
    TimestampColumnSpec("pickup_datetime") -> Seq("trip_pickup_datetime", "tpep_pickup_datetime",
                                                  "lpep_pickup_datetime"),
    TimestampColumnSpec("dropoff_datetime") -> Seq("trip_dropoff_datetime", "tpep_dropoff_datetime",
                                                   "lpep_dropoff_datetime"),
    DoubleColumnSpec("pickup_longitude") -> Seq("start_lon"),
    DoubleColumnSpec("pickup_latitude") -> Seq("start_lat"),
    DoubleColumnSpec("dropoff_longitude") -> Seq("end_lon"),
    DoubleColumnSpec("dropoff_latitude") -> Seq("end_lat"),
    IntegerColumnSpec("passenger_count") -> Seq(),
    DoubleColumnSpec("trip_distance") -> Seq(),
    StringColumnSpec("payment_type") -> Seq(),
    DoubleColumnSpec("tip_ammount") -> Seq("tip_amt"),
    DoubleColumnSpec("total_amount") -> Seq("total_amt")
  )


  def sparkType(colSpec: ColumnSpec): DataType = colSpec match {
    case StringColumnSpec(_) => StringType
    case IntegerColumnSpec(_) => IntegerType
    case DoubleColumnSpec(_) => DoubleType
    case TimestampColumnSpec(_) => TimestampType
  }

  def convertString(colSpec: ColumnSpec, v: Option[String]) = (colSpec, v) match {
    case (StringColumnSpec(_), Some(v)) => v
    case (IntegerColumnSpec(_), Some(v)) => v.toInt
    case (DoubleColumnSpec(_), Some(v)) => v.toDouble
    case (TimestampColumnSpec(_), Some(v)) => {
      val dateFormat = new SimpleDateFormat("yyyy-MM-ddhh:mm:ss")
      new java.sql.Timestamp(dateFormat.parse(v).getTime())
    }
    case (cs, v) => v
  }

  val columnNameNormalizationMap = (columnNameEquivilence map {
    case (k, v) => (k, v :+ k.name)
  }) flatMap { case (k, v) => v.map(x => (x, k)) }
  val orderedColumnList = columnNameEquivilence.keys.toList.sortBy(x => x.name)

  def main(args: Array[String]) {
    val sc = Utils.getSparkContext(this.getClass.getName)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val sqlContext = new SQLContext(sc)

    val reader = new TlcFileReader(sc, sqlContext, Constants.SourceCsvData)
    val csvHeaderGroups = sc.broadcast(reader.getFileGroups)
    printf("** Found %d different schemas with {%s} files.\n",
      csvHeaderGroups.value.values.size,
      csvHeaderGroups.value.values.map(x => x.size).mkString(","))

    val rddList = csvHeaderGroups.value map { case (csvHeader, filenames) =>
      readFilesOfSameHeader(sc, csvHeader, filenames)
    }
    val tripsRdd = rddList.reduce(_ union _).cache()
    printf("** Processing %d total trip records\n", tripsRdd.count())


    val rowsRdd = tripsRdd map { case (csvHeader, trip) => csvLineToRow(csvHeader, trip) }

    val schema = StructType(orderedColumnList.map(field => StructField(field.name, sparkType(field), true)))
    val rowsDf = sqlContext.createDataFrame(rowsRdd, schema)
    rowsDf.cache()

    fs.delete(Constants.TripDatabase, true)
    rowsDf.repartition(10).write.parquet(Constants.TripDatabase.toString)
  }

  def readFilesOfSameHeader(sc: SparkContext, csvHeader: String, filenames: Iterable[String]): RDD[(String, String)] = {
    printf("** Reading files with schema: %s\n", csvHeader)
    printf("** Files are:\n%s\n", filenames.map(x => "**   " + x).mkString("\n"))

    val lines = sc.textFile(filenames.mkString(","))
      .filter(x => x != csvHeader)
      .map(x => (csvHeader, x))
    return lines
  }

  // Returns the ColumnSpec of a given column name or alias
  def getColumnSpec(x: String): ColumnSpec = {
    return columnNameNormalizationMap.get(x.toLowerCase) getOrElse StringColumnSpec(x.toLowerCase)
  }

  def normalizeAndIndexCsvHeader(csvHeader: String): Map[String, Int] = {
    return (csvHeader.split(",") map Utils.stripWhitespace map {
      _.toLowerCase
    } map getColumnSpec
      map {
      _.name
    } zipWithIndex).toMap
  }

  def csvLineToRow(csvHeader: String, csvLine: String): Row = {
    val headerMap = normalizeAndIndexCsvHeader(csvHeader)
    val splitCsvLine = csvLine.split(",").map(Utils.stripWhitespace)
    return Row.fromSeq(orderedColumnList map {
      cs => (cs, headerMap.get(cs.name))
    } map {
      case (cs, Some(idx)) => (cs, splitCsvLine.lift(idx))
      case (cs, None) => (cs, None)
    } map {
      case (cs, Some("")) => (cs, None)
      case (cs, v) => (cs, v)
    } map {
      (convertString _).tupled(_)
    })
  }
}
