# Spatial Queries with Apache SparkSQL

## Overview

This project focuses on developing and running spatial queries on a large database containing geographic data and real-time location data of customers for a peer-to-peer taxi cab service. Using Apache Spark and SparkSQL, I implemented user-defined functions to handle various spatial queries efficiently.

## Getting Started

### Installation

To get started with this project, you'll need to have Apache Spark and SparkSQL installed on your machine. You can follow the instructions provided in the [Apache Spark documentation](https://spark.apache.org/docs/latest/).

Alternatively, you can use Docker to streamline the installation process. Here are some helpful resources to get started with Docker:
- [Docker Get Started Guide](https://www.docker.com/get-started/)
- [Installing Docker on Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-20-04)

### Resources

For understanding Apache SparkSQL and spatial queries, you can refer to the following resources:
- [IBM Documentation on Executing Spatial Query in Spark](https://www.ibm.com/docs/en/ias?topic=toolkit-executing-spatial-query-in-spark)
- [Spark SQL Quick Guide on TutorialsPoint](https://www.tutorialspoint.com/spark_sql/spark_sql_quick_guide.htm)

## Project Structure

The project is divided into two phases, each focusing on different aspects of spatial queries.

### Phase 1

In the first phase, I implemented two user-defined functions, `ST_Contains` and `ST_Within`, in SparkSQL. These functions are used to run the following four spatial queries:

1. **Range Query:** Finds all points within a given query rectangle using the `ST_Contains` function.
2. **Range Join Query:** Finds all (point, rectangle) pairs such that the point is within the rectangle.
3. **Distance Query:** Finds all points within a given distance from a fixed point using the `ST_Within` function.
4. **Distance Join Query:** Finds all (point1, point2) pairs such that point1 is within a given distance from point2.

### Phase 2

The second phase involves optimizing these queries for better performance and scalability on large datasets.

## User-Defined Functions

### ST_Contains

This function checks whether a given point lies within a specified rectangle.

**Inputs:**
- `pointString: String` (e.g., "-88.331492,32.324142")
- `queryRectangle: String` (e.g., "-155.940114,19.081331,-155.618917,19.5307")

**Output:**
- `Boolean` (true or false)

### ST_Within

This function checks whether two points are within a specified distance from each other.

**Inputs:**
- `pointString1: String` (e.g., "-88.331492,32.324142")
- `pointString2: String` (e.g., "-88.331492,32.324142")
- `distance: Double`

**Output:**
- `Boolean` (true or false)

## Queries

### Range Query

Find all points within a given query rectangle.

### Range Join Query

Find all (point, rectangle) pairs such that the point is within the rectangle.

### Distance Query

Find all points within a given distance from a fixed point.

### Distance Join Query

Find all (point1, point2) pairs such that point1 is within a given distance from point2.

## Running the Project

To run the project, follow these steps:

1. Clone the repository.
2. Install the required dependencies.
3. Set up Apache Spark and SparkSQL.
4. Use the provided Docker image for a streamlined setup.
5. Execute the queries using the implemented functions.

## Conclusion

This project showcases the power of Apache Spark and SparkSQL in handling large-scale spatial data. By implementing custom user-defined functions, I was able to efficiently process and query geographic data for a taxi cab service.

Feel free to explore the code and provide feedback!

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
