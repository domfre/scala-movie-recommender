package app.aggregator

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc: SparkContext) extends Serializable {

  private var state = null
  private var partitioner: HashPartitioner = new HashPartitioner(100)
  private var avgRatings: RDD[(Int, String, Double, Int, List[String])] = null

  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   * @param title   The RDD of titles in the file
   */
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            title: RDD[(Int, String, List[String])]
          ): Unit = {
    val movies =
      title
        .groupBy(_._1)
        .map(movie => (movie._1, movie._2.head))

    avgRatings =
      ratings
        .groupBy(rating => (rating._1, rating._2))
        .map(rating => (rating._1._2, rating._2.maxBy(_._5)._4))
        .groupByKey()
        .partitionBy(partitioner)
        .map(ratings => (ratings._1, (ratings._2.sum/ratings._2.size, ratings._2.size)))
        .rightOuterJoin(movies)
        .map(avgRatings =>
          (avgRatings._1, avgRatings._2._2._2, avgRatings._2._1.map(_._1).getOrElse(0.0), avgRatings._2._1.map(_._2).getOrElse(0), avgRatings._2._2._3))

    avgRatings.persist()
  }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult(): RDD[(String, Double)] = {
    avgRatings
      .map(rating => (rating._2, rating._3))
  }

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords: List[String]): Double = {
    val filteredByKeywords =
      avgRatings
      .filter(movie =>
        keywords.forall(movie._5.contains))

    if (filteredByKeywords.isEmpty())
      return -1.0

    val rated =
      filteredByKeywords

      // check if movie is already rated (rating != 0.0)
      .filter(movie => movie._3 != 0.0)

    if (rated.isEmpty())
      0.0
    else
      rated
        .map(movie => movie._3)
        .mean()
  }

  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   *  @param delta Delta ratings that haven't been included previously in aggregates
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]): Unit = {
    val deltaGroupedByMovie =
      sc.parallelize(delta_)
        .groupBy(rating => (rating._1, rating._2))
        .map(rating => (rating._1._2, rating._2.maxBy(_._5)))

    val deltaGroupedWithNewCounts =
      deltaGroupedByMovie
        .groupBy(_._1)
        .map(ratingsPerMovie =>
          (ratingsPerMovie._1, ratingsPerMovie._2.map(rating => (rating._2._3.getOrElse(0.0), rating._2._4))))
        .map(ratings =>
          (ratings._1, ratings._2.count(_._1 == 0.0), ratings._2))

  val existingJoinedWithDelta =
    avgRatings
      .groupBy(_._1)
      .leftOuterJoin(deltaGroupedWithNewCounts.groupBy(_._1))
      .map(movie =>
        (movie._2._1.head,
          movie._2._2.getOrElse(Iterable((movie._2._1.head._1, 0, Iterable((0.0, 0.0))))).head._2,
          movie._2._2.getOrElse(Iterable((movie._2._1.head._1, 0, Iterable((0.0, 0.0))))).head._3.map(_._1),
          movie._2._2.getOrElse(Iterable((movie._2._1.head._1, 0, Iterable((0.0, 0.0))))).head._3.map(_._2)))

  avgRatings =
      existingJoinedWithDelta
      .map(movie =>
        (movie._1._1,
          movie._1._2,
          if ((movie._1._4 + movie._2) != 0) (movie._1._3 * movie._1._4 - movie._3.sum + movie._4.sum)/(movie._1._4 + movie._2) else 0.0,
          movie._1._4 + movie._2,
          movie._1._5))
  avgRatings.persist()

  }

}
