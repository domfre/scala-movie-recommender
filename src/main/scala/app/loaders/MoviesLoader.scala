package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io.File

/**
 * Helper class for loading the input
 *
 * @param sc   The Spark context for the given application
 * @param path The path for the input file
 */
class MoviesLoader(sc: SparkContext, path: String) extends Serializable {

  /**
   * Read the title file in the given path and convert it into an RDD
   *
   * @return The RDD for the given titles
   */
  def load(): RDD[(Int, String, List[String])] = {
    val fileItem = new File(getClass.getResource(path).getFile).getPath
    val movies = sc.textFile(fileItem).map(MoviesLoaderFunctions.toMovieTuple)
    movies.persist()
  }
}

object MoviesLoaderFunctions {

  /**
   * Maps a movie represented as String in the form id|name|keyword1|keyword2| . . . |keywordn
   * to the required formatted tuple (id, name, List[keywords])
   *
   * @return The formatted tuple for the given movie line
   */
  def toMovieTuple(line: String): (Int, String, List[String]) = {
    val movie = line.replaceAll("\"", "").split("\\|")
    val movieId = movie(0).toInt
    val movieName = movie(1)
    val movieKeywords = movie.toList.drop(2)
    (movieId, movieName, movieKeywords)
  }
}

