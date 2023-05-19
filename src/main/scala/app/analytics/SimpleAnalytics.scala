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
    titlesGroupedById = movies.groupBy(_._1).partitionBy(moviesPartitioner)
    titlesGroupedById.persist()

    ratingsGroupedByYearByTitle =
      ratings
        .groupBy(rating => (new DateTime(rating._5.toLong * 1000).year().get(), rating._2))
        .partitionBy(ratingsPartitioner)
    ratingsGroupedByYearByTitle.persist()

  }

  def getNumberOfMoviesRatedEachYear: RDD[(Int, Int)] = {
    ratingsGroupedByYearByTitle
      .map(_._1)
      .groupByKey()
      .map(moviesGroupedByYear => (moviesGroupedByYear._1, moviesGroupedByYear._2.size))
  }

  def getMostRatedMovieEachYear: RDD[(Int, String)] = {
    getNameAndKeywordsForMostRatedMoviesPerYear
      .map(movie => (movie._2._1, movie._2._2.head._2))
      .sortByKey()
  }

  def getMostRatedGenreEachYear: RDD[(Int, List[String])] = {
    getNameAndKeywordsForMostRatedMoviesPerYear
      .map(movie => (movie._2._1, movie._2._2.head._3))
      .sortByKey()
  }

  // Note: if two genre has the same number of rating, return the first one based on lexicographical sorting on genre.
  def getMostAndLeastRatedGenreAllTime: ((String, Int), (String, Int)) = {
    val genreOccurrencesSorted =
      getNameAndKeywordsForMostRatedMoviesPerYear

        // map to list of List[keywords]
        .map(_._2._2.head._3)

        // flatmap to list of keyword strings
        .flatMap(_.toList)

        // count genre occurrences
        .map(genre => (genre, 1))
        .reduceByKey(_ + _)

        // for each count, if more than one genre,
        // get min based on lexicographical sorting
        .map(_.swap)
        .groupByKey()
        .map(genres => (genres._1, genres._2.min))

        // sort by number of occurrences ascending
        .sortByKey()

        .map(_.swap)

    val max = genreOccurrencesSorted.collect().last
    val min = genreOccurrencesSorted.collect().head

    (min, max)
  }

  /**
   * Filter the movies RDD having the required genres
   *
   * @param movies         RDD of movies dataset
   * @param requiredGenres RDD of genres to filter movies
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre(movies: RDD[(Int, String, List[String])],
                          requiredGenres: RDD[String]): RDD[String] = {
    val genres = requiredGenres.collect().toList
    filterMoviesBasedOnGenres(movies, genres)
  }

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
                                         broadcastCallback: List[String] => Broadcast[List[String]]): RDD[String] = {
    val genres = broadcastCallback.apply(requiredGenres)
    filterMoviesBasedOnGenres(movies, genres.value)
  }

  /**
   * Scans movie ratings and filters most rated movies per year and joins with movie info via movieId
   *
   * @return RDD of (movieId, (year, Iterable[movie info])) pairs
   */
  def getNameAndKeywordsForMostRatedMoviesPerYear: RDD[(Int, (Int, Iterable[(Int, String, List[String])]))] = {
    ratingsGroupedByYearByTitle
      .map(ratingsPerYearAndMovie =>
        (ratingsPerYearAndMovie._1._1, (ratingsPerYearAndMovie._1._2, ratingsPerYearAndMovie._2.size)))
      .groupByKey()
      .map(numberOfRatingsPerMovieAndYear =>
        (numberOfRatingsPerMovieAndYear._2.toSeq.sortWith(_._1 > _._1).maxBy(_._2)._1, numberOfRatingsPerMovieAndYear._1))
      .join(titlesGroupedById)
  }

  def filterMoviesBasedOnGenres(movies: RDD[(Int, String, List[String])],
                                requiredGenres: List[String]) : RDD[String] = {
    movies
      .filter(movie =>
        movie._3.exists(genre => requiredGenres.contains(genre)))
      .map(_._2)
  }

}

