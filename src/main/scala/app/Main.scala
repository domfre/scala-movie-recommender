package app

import app._
import app.analytics.SimpleAnalytics
import app.loaders.{MoviesLoader, RatingsLoader}
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)

    //your code goes here
    val moviesLoader = new MoviesLoader(sc, "./src/main/resources/movies_small.csv")
    val ratingsLoader = new RatingsLoader(sc, "./src/main/resources/ratings_small.csv")

    val movies = moviesLoader.load()
    val ratings = ratingsLoader.load()

    val simpleAnalytics = new SimpleAnalytics
    simpleAnalytics.init(ratings, movies)
  }
}
