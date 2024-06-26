package cse511

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

import scala.math.pow

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

    def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
    {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    pickupInfo.show();
      pickupInfo.createOrReplaceTempView("points");

    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

      /*
      Filtering the points checking the give range above.
       */
      var filterPoints = pickupInfo.filter(col("x") >= minX && col("x") <= maxX && col("y") >= minY && col("y") <= maxY
      && col("z") >= minZ && col("z") <= maxZ).toDF();
      filterPoints.createOrReplaceTempView("filterPoints")

      /*
      The metric xj used to calculate the z_score is no.of trips in a particular cell. Counting the no.of trips in a cell.
       */
      var pointsAndTrips = spark.sql("select x, y, z, count(*) as cnt from filterPoints group by x,y,z");

      /*
      Calculating the sum and squared sum of xj i.e. no.of trips to calculate the mean and standard deviation of xj. Calculates the mean, standard Deviation.
       */
      val sumOfTrips = pointsAndTrips.select(col("cnt")).groupBy().sum().first().getLong(0);
      //println(sumOfTrips);
      val squaredSumOfTrips = pointsAndTrips.select(col("cnt") * col("cnt")).groupBy().sum().first().getLong(0);
      //println(squaredSumOfTrips);
      val mean = sumOfTrips/numCells;
      val SD = scala.math.sqrt((squaredSumOfTrips/numCells)-pow(mean,2));
      //println(mean,SD);


      /*
      Calculate the weighted sum of xj for all the neighbours, no.of neighbours.
       */
      pointsAndTrips.createOrReplaceTempView("pointsAndTripsTable");
      var pointsAndMetrics = spark.sql("select p1.x as x,p1.y as y,p1.z as z,count(*) as neighbours,sum(p2.cnt) as x_j from pointsAndTripsTable p1, pointsAndTripsTable p2 where (abs(p1.x-p2.x) <= 1) and (abs(p1.y-p2.y) <= 1) and (abs(p1.z-p2.z) <= 1) group by p1.x,p1.y,p1.z");
      pointsAndMetrics.show();
      pointsAndMetrics.createOrReplaceTempView("pointsAndMetricsTable")


      /*
      registering the user defined functions to calculate the Z-score and then calculating the z_score
       */
      spark.udf.register("zscoreDenominator",(SD:Double, n : Double, numCells : Double)=>((
        HotcellUtils.zscoreDenominator(SD, n,numCells)
      )))

      spark.udf.register("zscoreNumerator", (mean:Double, x_j : Double, n : Double) => ((
        HotcellUtils.zscoreNumerator(mean, x_j, n)
        )))
      var result = spark.sql("select x,y,z from pointsAndMetricsTable order by zscoreNumerator("+mean+",x_j,neighbours)/zscoreDenominator("+SD+",neighbours,"+numCells+") desc").toDF();
      result.show();
    return result;
    }
}
