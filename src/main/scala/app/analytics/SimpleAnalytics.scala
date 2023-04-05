package app.analytics

import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime


class SimpleAnalytics() extends Serializable {

  private var ratingsPartitioner: HashPartitioner = new HashPartitioner(100)
  private var moviesPartitioner: HashPartitioner = new HashPartitioner(100)

  var titlesGroupedById: RDD[(Int, Iterable[(Int, String, List[String])])] = null
  var ratingsGroupedByYearByTitle: RDD[((Int, Int), Iterable[(Int, Int, Option[Double], Double, Int)])] = null

  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            movies: RDD[(Int, String, List[String])]
          ): Unit = {
    titlesGroupedById = movies.groupBy(movie => movie._1).partitionBy(moviesPartitioner)
    titlesGroupedById.persist()

    ratingsGroupedByYearByTitle =
      ratings
        .groupBy(rating => (new DateTime(rating._5.toLong * 1000).year().get(), rating._2))
        .partitionBy(ratingsPartitioner)
    ratingsGroupedByYearByTitle.persist()

  }

  def getNumberOfMoviesRatedEachYear: RDD[(Int, Int)] = {
    ratingsGroupedByYearByTitle
      .map(perYearAndMovieRatings => perYearAndMovieRatings._1)
      .groupByKey()
      .map(moviesGroupedByYear => (moviesGroupedByYear._1, moviesGroupedByYear._2.toList.length))
  }

  def getMostRatedMovieEachYear: RDD[(Int, String)] = {
    val grouped = ratingsGroupedByYearByTitle
      .map(ratingsPerYearAndMovie =>
        (ratingsPerYearAndMovie._1._1, (ratingsPerYearAndMovie._1._2, ratingsPerYearAndMovie._2.toList.length)))
      .groupByKey()
    grouped.collect().foreach(movie => println(movie))
    val ratings = grouped
      .map(numberOfRatingsPerMovieAndYear =>
        (numberOfRatingsPerMovieAndYear._2.toList.sortBy(_._1).maxBy(movieRatings => movieRatings._2)._1, numberOfRatingsPerMovieAndYear._1))
    //ratings.collect().foreach(movie => println(movie))
    val joined = ratings
      .join(titlesGroupedById)
    //joined.collect().foreach(movie => println(movie))
    val sorted = joined
      .map(movie => (movie._2._1, movie._2._2.head._2))
      .sortByKey()
    //sorted.collect().foreach(movie => println(movie))
    sorted
  }

  def getMostRatedGenreEachYear: RDD[(Int, List[String])] = ???

  // Note: if two genre has the same number of rating, return the first one based on lexicographical sorting on genre.
  def getMostAndLeastRatedGenreAllTime: ((String, Int), (String, Int)) = ???

  /**
   * Filter the movies RDD having the required genres
   *
   * @param movies         RDD of movies dataset
   * @param requiredGenres RDD of genres to filter movies
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre(movies: RDD[(Int, String, List[String])],
                          requiredGenres: RDD[String]): RDD[String] = ???

  /**
   * Filter the movies RDD having the required genres
   * HINT: use the broadcast callback to broadcast requiresGenres to all Spark executors
   *
   * @param movies            RDD of movies dataset
   * @param requiredGenres    List of genres to filter movies
   * @param broadcastCallback Callback function to broadcast variables to all Spark executors
   *                          (https://spark.apache.org/docs/2.4.8/rdd-programming-guide.html#broadcast-variables)
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre_usingBroadcast(movies: RDD[(Int, String, List[String])],
                                         requiredGenres: List[String],
                                         broadcastCallback: List[String] => Broadcast[List[String]]): RDD[String] = ???

}

