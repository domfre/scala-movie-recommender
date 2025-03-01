package app.recommender.LSH

import org.apache.spark.rdd.RDD

/**
 * Class for performing LSH lookups
 *
 * @param lshIndex A constructed LSH index
 */
class NNLookup(lshIndex: LSHIndex) extends Serializable {

  /**
   * Lookup operation for queries
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (keyword list, result) pairs, where a result is of the form
   *         List[(movieId, movieTitle, List[keyword])])
   */
  def lookup(queries: RDD[List[String]])
  : RDD[(List[String], List[(Int, String, List[String])])] = {
    lshIndex
      .lookup(lshIndex.hash(queries))
      .map(queryResult => (queryResult._2, queryResult._3))
  }
}
