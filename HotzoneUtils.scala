package cse511

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    var point = pointString.split(",").map(_.trim).map(_.toDouble);
    var rect = queryRectangle.split(",").map(_.trim).map(_.toDouble);
    //checking a setting the left and right coordinates of the rectangle.
    var (left_x, right_x) = if (rect(0) < rect(2)) (rect(0), rect(2)) else (rect(2), rect(0));
    var (left_y, right_y) = if (rect(1) < rect(3)) (rect(1), rect(3)) else (rect(3), rect(1));
    //checking whether the point lies inside the rectangle
    if (point(0) >= left_x && point(0) <= right_x && point(1) >= left_y && point(1) <= right_y) {true;} else {false;}
  }
}
