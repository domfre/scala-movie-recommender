package app.recommender.collaborativeFiltering


import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}

class CollaborativeFiltering(rank: Int,
                             regularizationParameter: Double,
                             seed: Long,
                             n_parallel: Int) extends Serializable {

  // NOTE: set the parameters according to the project description to get reproducible (deterministic) results.
  private val maxIterations = 20
  private var model: MatrixFactorizationModel = null

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    val latestRatingsPerUserAndMovie = mapLatestRatingsPerUserAndMovie(ratingsRDD)
    model =
      ALS
        .train(latestRatingsPerUserAndMovie, rank, maxIterations, regularizationParameter, n_parallel, seed)
  }

  def predict(userId: Int, movieId: Int): Double = {
    model.predict(userId, movieId)
  }

  def mapLatestRatingsPerUserAndMovie(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): RDD[Rating] = {
    ratingsRDD
      .map(rating => ((rating._1, rating._2), rating._3, rating._4, rating._5))
      .groupBy(_._1)
      .map(ratingsPerUserAndMovie =>
        Rating(ratingsPerUserAndMovie._1._1, ratingsPerUserAndMovie._1._2, ratingsPerUserAndMovie._2.maxBy(_._4)._3))
  }

}
