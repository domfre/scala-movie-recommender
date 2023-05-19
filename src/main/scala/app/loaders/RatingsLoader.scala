package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io.File

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class RatingsLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the rating file in the given path and convert it into an RDD
   *
   * @return The RDD for the given ratings
   */
  def load() : RDD[(Int, Int, Option[Double], Double, Int)] = {
    val fileItem = new File(getClass.getResource(path).getFile).getPath
    val ratings = sc.textFile(fileItem).map(RatingsLoaderFunctions.toRatingTuple)
    ratings.persist()
  }
}

/**
 * Helper functions for the ratings loader class
 */
object RatingsLoaderFunctions {

  /**
   * Maps a rating represented as String in the form userId|movieId|rating|timestamp
   * to the required formatted tuple (userId, movieId, Option[oldRating], newRating, timestamp)
   *
   * @return The formatted tuple (userId, movieId, Option[oldRating], newRating, timestamp) for the given rating line
   */
  def toRatingTuple(line: String): (Int, Int, Option[Double], Double, Int) = {
    val rating = line.split("\\|")
    val userId = rating(0).toInt
    val movieId = rating(1).toInt
    val oldRating = Option.empty
    val newRating = rating(2).toDouble
    val timestamp = rating(3).toInt
    (userId, movieId, oldRating, newRating, timestamp)
  }
}