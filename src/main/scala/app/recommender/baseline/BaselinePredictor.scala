package app.recommender.baseline

import org.apache.spark.rdd.RDD

class BaselinePredictor() extends Serializable {

  private var state = null
  private var avgRatingsByUser: RDD[(Int, Double)] = null
  private var globalAvgDevByMovie: RDD[(Int, Double)] = null

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    avgRatingsByUser = avgRatingsByUser(ratingsRDD).persist()
    globalAvgDevByMovie = globalAvgDevByMovie(ratingsRDD).persist()
  }

  def predict(userId: Int, movieId: Int): Double = {
    val avgRatingOfUser = avgRatingsByUser.filter(_._1 == userId).first()._2
    val globalAvgDevOfMovie = globalAvgDevByMovie.filter(_._1 == movieId).first()._2

    avgRatingOfUser + globalAvgDevOfMovie * scale((avgRatingOfUser + globalAvgDevOfMovie), avgRatingOfUser)
  }

  def avgRatingsByUser(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): RDD[(Int, Double)] = {
    ratingsPerUserAndMovie(ratingsRDD)
      .map(ratingsPerUserAndMovie => (ratingsPerUserAndMovie._1, ratingsPerUserAndMovie._2._2))
      .groupByKey()
      .map(ratingsPerUser => (ratingsPerUser._1, ratingsPerUser._2.sum/ratingsPerUser._2.size))
  }

  def ratingsPerUserAndMovie(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): RDD[(Int, (Int, Double))] = {
    ratingsRDD
      .map(rating => ((rating._1, rating._2), rating._3, rating._4, rating._5))
      .groupBy(_._1)
      .map(ratingsPerUserAndMovie =>
        (ratingsPerUserAndMovie._1._1, (ratingsPerUserAndMovie._1._2, ratingsPerUserAndMovie._2.maxBy(_._4)._3)))
  }

  def normalizedDeviationPerUserAndMovie(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): RDD[((Int, Int), Double)] = {
    ratingsPerUserAndMovie(ratingsRDD)
      .join(avgRatingsByUser(ratingsRDD))
      .map(singleJoinedAvg =>
        ((singleJoinedAvg._1, singleJoinedAvg._2._1._1), normalizedDeviation(singleJoinedAvg._2._1._2, singleJoinedAvg._2._2)))
  }

  def globalAvgDevByMovie(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): RDD[(Int, Double)] = {
    normalizedDeviationPerUserAndMovie(ratingsRDD)
      .groupBy(_._1._2)
      .map(perMovieDeviations => (perMovieDeviations._1, perMovieDeviations._2.map(_._2).sum/perMovieDeviations._2.size))
  }

  def scale(x: Double, avgRatingOfOneUser: Double): Double = {
    if (x > avgRatingOfOneUser) 5 - avgRatingOfOneUser else if (x < avgRatingOfOneUser) avgRatingOfOneUser - 1 else 1
  }

  def normalizedDeviation(rating: Double, avgRatingOfOneUser: Double) : Double = {
    (rating - avgRatingOfOneUser) / scale(rating, avgRatingOfOneUser)
  }

  def globalAvgDevForOneMovie(normalizedDeviations: RDD[Double]): Double = {
    normalizedDeviations.sum() / normalizedDeviations.count()
  }
}
