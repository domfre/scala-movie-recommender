package app.recommender.baseline

import org.apache.spark.rdd.RDD

class BaselinePredictor() extends Serializable {

  private var state = null
  private var avgRatingsByUser: RDD[(Int, Double)] = null
  private var globalAvgDevByMovie: RDD[(Int, Double)] = null

  /**
   * Initialize the requested data avgRatingsByUser and globalAvgDevByMovie
   * @param ratingsRDD RDD of all ratings of the form (userId, movieId, Option[oldRating], newRating, timestamp) tuples
   */
  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    avgRatingsByUser = avgRatingsByUser(ratingsRDD).persist()
    globalAvgDevByMovie = globalAvgDevByMovie(ratingsRDD).persist()
  }

  def predict(userId: Int, movieId: Int): Double = {
    val avgRatingOfUser = avgRatingsByUser.filter(_._1 == userId).first()._2
    val globalAvgDevOfMovie = globalAvgDevByMovie.filter(_._1 == movieId).first()._2

    avgRatingOfUser + globalAvgDevOfMovie * scale((avgRatingOfUser + globalAvgDevOfMovie), avgRatingOfUser)
  }

  /**
   * Calculates the average rating of a user across all her ratings
   *
   * @param ratingsRDD ratings in the form of (userId, movieId, Option[oldRating], newRating, timestamp) tuples
   * @return RDD of (userId, averageRating) tuples
   */
  def avgRatingsByUser(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): RDD[(Int, Double)] = {
    ratingsPerUserAndMovie(ratingsRDD)
      .map(ratingsPerUserAndMovie => (ratingsPerUserAndMovie._1, ratingsPerUserAndMovie._2._2))
      .groupByKey()
      .map(ratingsPerUser => (ratingsPerUser._1, ratingsPerUser._2.sum/ratingsPerUser._2.size))
  }

  /**
   * Maps the ratingsRDD to unique (userId, (movieId, rating)) tuples with rating being the most recent rating
   * casted by each user
   *
   * @param ratingsRDD ratings in the form of (userId, movieId, Option[oldRating], newRating, timestamp) tuples
   * @return (userId, (movieId, rating)) tuples
   */
  def ratingsPerUserAndMovie(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): RDD[(Int, (Int, Double))] = {
    ratingsRDD
      .map(rating => ((rating._1, rating._2), rating._3, rating._4, rating._5))
      .groupBy(_._1)
      .map(ratingsPerUserAndMovie =>
        (ratingsPerUserAndMovie._1._1, (ratingsPerUserAndMovie._1._2, ratingsPerUserAndMovie._2.maxBy(_._4)._3)))
  }

  /**
   * Calculates the normalized deviation of ratings for each user/movie-rating pair
   *
   * @param ratingsRDD ratings in the form of (userId, movieId, Option[oldRating], newRating, timestamp) tuples
   * @return RDD of ((userId, movieId), normalizedDeviation) tuples
   */
  def normalizedDeviationPerUserAndMovie(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): RDD[((Int, Int), Double)] = {
    ratingsPerUserAndMovie(ratingsRDD)
      .join(avgRatingsByUser(ratingsRDD))
      .map(userRatingPerMovieAvgRatingTuple =>
        ((userRatingPerMovieAvgRatingTuple._1, userRatingPerMovieAvgRatingTuple._2._1._1),
          normalizedDeviation(userRatingPerMovieAvgRatingTuple._2._1._2, userRatingPerMovieAvgRatingTuple._2._2)))
  }

  /**
   * Calculates the average deviations of ratings for each movie
   *
   * @param ratingsRDD ratings in the form of (userId, movieId, Option[oldRating], newRating, timestamp) tuples
   * @return RDD of (movieId, avgDeviation) tuples
   */
  def globalAvgDevByMovie(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): RDD[(Int, Double)] = {
    normalizedDeviationPerUserAndMovie(ratingsRDD)
      .groupBy(_._1._2)
      .map(perMovieDeviations => (perMovieDeviations._1, perMovieDeviations._2.map(_._2).sum/perMovieDeviations._2.size))
  }

  def scale(rating: Double, avgRatingOfOneUser: Double): Double = {
    if (rating > avgRatingOfOneUser) 5 - avgRatingOfOneUser else if (rating < avgRatingOfOneUser) avgRatingOfOneUser - 1 else 1
  }

  def normalizedDeviation(rating: Double, avgRatingOfOneUser: Double) : Double = {
    (rating - avgRatingOfOneUser) / scale(rating, avgRatingOfOneUser)
  }

  def globalAvgDevForOneMovie(normalizedDeviations: RDD[Double]): Double = {
    normalizedDeviations.sum() / normalizedDeviations.count()
  }
}
