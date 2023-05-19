package app.recommender.LSH


import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

import scala.reflect.{ClassTag, ensureAccessible}

/**
 * Class for indexing the data for LSH
 *
 * @param data The title data to index
 * @param seed The seed to use for hashing keyword lists
 */
class LSHIndex(data: RDD[(Int, String, List[String])], seed : IndexedSeq[Int]) extends Serializable {

  private val minhash = new MinHash(seed)
  private var buckets: RDD[(IndexedSeq[Int], List[(Int, String, List[String])])] = getBuckets()

  /**
   * Hash function for an RDD of queries (where a query is a list of keywords).
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (signature, keyword list) pairs
   */
  def hash(input: RDD[List[String]]) : RDD[(IndexedSeq[Int], List[String])] = {
    input.map(keywordList => (minhash.hash(keywordList), keywordList))
  }

  /**
   * Return data structure of LSH index for testing
   *
   * @return Data structure of LSH index
   */
  def getBuckets(): RDD[(IndexedSeq[Int], List[(Int, String, List[String])])] = {
    val hashedKeywordLists = hash(data.map(_._3))
    val moviesGroupedByKeywordLists = data.map(movie => (movie._3, (movie._1, movie._2))).groupByKey()
    hashedKeywordLists
      .map(_.swap)
      .join(moviesGroupedByKeywordLists)

      // create map of hashed keyword lists and its associated (movieId, movieTitle, keywords) tuples
      // (hashedKeywords => List[(movieId, movieTitle, List[keyword])])
      .map(hashedKeywordsMoviesGroupForKeywordList =>
        (hashedKeywordsMoviesGroupForKeywordList._2._1,
          hashedKeywordsMoviesGroupForKeywordList._2._2.map(movie =>
            (movie._1, movie._2, hashedKeywordsMoviesGroupForKeywordList._1)).toList))

      .groupByKey()

      // remove duplicates by mapping toSet
      .mapValues(_.flatten.toSet.toList)
      .partitionBy(new HashPartitioner(hashedKeywordLists.groupByKey().count().toInt))
      .cache()
  }

  /**
   * Lookup operation on the LSH index
   *
   * @param queries The RDD of queries. Each query contains the pre-computed signature
   *                and a payload
   * @return The RDD of (signature, payload, result) triplets after performing the lookup.
   *         If no match exists in the LSH index, return an empty result list.
   */
  def lookup[T: ClassTag](queries: RDD[(IndexedSeq[Int], T)])
  : RDD[(IndexedSeq[Int], T, List[(Int, String, List[String])])] = {
    queries
      .leftOuterJoin(buckets)
      .map(query => (query._1, query._2._1, query._2._2.getOrElse(List.empty)))
  }
}
