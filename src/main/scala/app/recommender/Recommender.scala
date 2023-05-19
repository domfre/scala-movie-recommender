package app.recommender

import app.recommender.LSH.{LSHIndex, NNLookup}
import app.recommender.baseline.BaselinePredictor
import app.recommender.collaborativeFiltering.CollaborativeFiltering
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Class for performing recommendations
 */
class Recommender(sc: SparkContext,
                  index: LSHIndex,
                  ratings: RDD[(Int, Int, Option[Double], Double, Int)]) extends Serializable {

  private val nn_lookup = new NNLookup(index)
  private val collaborativePredictor = new CollaborativeFiltering(10, 0.1, 0, 4)
  collaborativePredictor.init(ratings)

  private val baselinePredictor = new BaselinePredictor()
  baselinePredictor.init(ratings)

  /**
   * Returns the top K recommendations for movies similar to the List of genres
   * for userID using the BaseLinePredictor
   */
  def recommendBaseline(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {
    val predictions = Nil
    for (movieId <- retrieveSimilarMovies(genre)) (movieId, baselinePredictor.predict(userId, movieId)) :: predictions
    topK(predictions, K)
  }

  /**
   * The same as recommendBaseline, but using the CollaborativeFiltering predictor
   */
  def recommendCollaborative(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {
    val predictions = Nil
    for (movieId <- retrieveSimilarMovies(genre)) (movieId, collaborativePredictor.predict(userId, movieId)) :: predictions
    topK(predictions, K)
  }

  /**
   * Search for similar movies based on genre
   *
   * @param genre to be searched for among available movies
   * @return movies associated to the requested genre
   */
  def retrieveSimilarMovies(genre: List[String]): List[Int] = {
    nn_lookup.lookup(sc.parallelize(List(genre)))
      .map(_._2.map(_._1))
      .first()
  }

  /**
   * Sort movies by rating and return top K rated movies
   * @param movies
   * @param K param defining number of top movies to return
   * @return
   */
  def topK(movies: List[(Int, Double)], K: Int): List[(Int, Double)] = {
    movies
      .sortWith(_._2 > _._2)
      .take(K)
  }
}
