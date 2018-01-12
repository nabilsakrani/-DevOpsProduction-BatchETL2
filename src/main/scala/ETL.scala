
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf

object ETL {

  val log = Logger.getLogger(getClass.getName)

  def enrichMovies(movies : DataFrame, links : DataFrame) : DataFrame = {

    val fullLink : String => String = {tmdb =>
      "https://www.themoviedb.org/movie/"+tmdb
    }
    val fullLinkUDF = udf(fullLink)

    var outMovies = movies
      .join(links, movies("movieid")===links("movieid"))

    val movieCols = Seq("movieid", "title", "genres", "full")

    outMovies = outMovies.withColumn("full", fullLinkUDF(outMovies("tmdbid")))
      .toDF("id", "movieid", "title", "genres",  "linkid", "linkmovieid", "imdbid", "tmdbid", "full")
      .select(movieCols.head, movieCols.tail: _*)
      .toDF("movieid", "title", "genres", "link")

    outMovies
  }


}
