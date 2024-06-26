package cse511

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

	val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
	pointDf.createOrReplaceTempView("point")

	// YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
	spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>{
	//Extracting the coordinates of the point, rectangle from the string
      var point = pointString.split(",").map(_.trim).map(_.toDouble);
      var rect = queryRectangle.split(",").map(_.trim).map(_.toDouble);
		
	//checking a setting the left and right coordinates of the rectangle.
      var(left_x,right_x) = if(rect(0) < rect(2)) (rect(0),rect(2)) else (rect(2),rect(0));
      var(left_y,right_y) = if(rect(1) < rect(3)) (rect(1),rect(3)) else (rect(3),rect(1));
	
	//checking whether the point lies inside the rectangle
      if (point(0) >= left_x && point(0) <= right_x && point(1) >= left_y && point(1) <= right_y) {
        true;
      } else {
        false;
      }
    });

	val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
	resultDf.show()

	return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

	val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
	pointDf.createOrReplaceTempView("point")

	val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
	rectangleDf.createOrReplaceTempView("rectangle")

	// YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
	spark.udf.register("ST_Contains", (queryRectangle: String, pointString: String) => {
	
      // Getting the query results as list of double datatype values
      val list_QR = queryRectangle.split(",").map(_.trim).map(_.toDouble).toBuffer;
      val list_PR = pointString.split(",").map(_.trim).map(_.toDouble).toBuffer;
	  
      // Finding the minimum and maximum values of the queryRectangle values
      val (min_x, max_x) = if (list_QR(0) > list_QR(2)) (list_QR(2), list_QR(0)) else (list_QR(0), list_QR(2))
      val (min_y, max_y) = if (list_QR(1) > list_QR(3)) (list_QR(3), list_QR(1)) else (list_QR(1), list_QR(3))

      // Identifying if the point query value belongs in the range of minimum and maximum values of rectangle query values determined earlier
      if (min_x <= list_PR(0) && list_PR(0) <= max_x && min_y <= list_PR(1) && list_PR(1) <= max_y) { true;}
      else { false;}
    });

	val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
	resultDf.show()

	return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

	val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
	pointDf.createOrReplaceTempView("point")

	// YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
	spark.udf.register("ST_Within", (pointString1: String, pointString2: String, distance: Double) => {
	      // Parse the strings to type Double and load them into lists.
	      val p1 = pointString1.split(",").map(_.trim).map(_.toDouble).toList
	      val p2 = pointString2.split(",").map(_.trim).map(_.toDouble).toList

	      // Intermediate calculation for distance
	      val x = p1(0) - p2(0)
	      val y = p1(1) - p2(1)

	      // Calculate square of distance, using previous calculation
	      var calc_dist: Double = (x * x) + (y * y)

	      calc_dist <= (distance * distance) // Return true if the calculated distance is less than or equal to given distance. Else false.
	    })

	val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
	resultDf.show()

	return resultDf.count()
  }

  // This function takes in a SparkSession Object and 3 arguments
  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {
	// Read in the first file as tab-seperated csv and create a temporary view in spark
	val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")
	// Read in the Second file as tab-seperated csv and create a temporary view in spark
    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")
    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>{
      var point1 = pointString1.split(",").map(_.trim).map(_.toDouble);
      var point2 = pointString2.split(",").map(_.trim).map(_.toDouble);
      var pointDist:Double = sqrt(pow(point1(0) - point2(0), 2) + pow(point1(1) - point2(1), 2));
      if(distance >= pointDist){
        true;
      }else{
        false;
      }
    })
	// Run a sql query using the udf to perform a distance based join on the two views
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
	// print the resulting dataframe to the console
    resultDf.show()
	// return the number of rows in the resulting data frame
    return resultDf.count()
  }
	
  def pow(base: Double, exponent: Double): Double = {
    import scala.math._
    math.pow(base, exponent)
  }

  def sqrt(base: Double) = {
    import scala.math._
    math.sqrt(base)
  }
	
}
