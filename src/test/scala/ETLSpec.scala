
import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import sys.process._


class ETLSpec
  extends FlatSpec{

  var CONF_DIR = ""
  var CONFIG_FILE = "BatchETL_staging.conf"

  "The ETL process" should
    "merge movies and links in orther to take only useful info" in {

    //CONF_DIR = scala.util.Properties.envOrElse("DEVOPS_CONF_DIR", "conf")
    CONF_DIR = "conf"

    println(s"\n\n${CONF_DIR}\n\n")

    val configuration = ConfigFactory.parseFile(new File(s"${CONF_DIR}/${CONFIG_FILE}"))

    val SPARK_APPNAME = configuration.getString("betl.spark.app_name")
    val SPARK_MASTER = configuration.getString("betl.spark.master")

    val conf = new SparkConf()
      .setMaster(SPARK_MASTER)
      .setAppName(SPARK_APPNAME)

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val movies = sc.parallelize(Seq(
      (1, 1, "anna", "romantico")
    ))

    val links = sc.parallelize(Seq(
      (1, 1, "imdbID-9", "tmdbID-7")
    ))

//    movies.foreach(println)
//    links.foreach(println)

    val mdf = sqlContext.createDataFrame(movies).toDF("id","movieid","title", "genres")
    val ldf = sqlContext.createDataFrame(links).toDF("id","movieid","imdbid", "tmdbid")

    val em = ETL.enrichMovies(mdf, ldf)

    assert(em.rdd.map{case Row(movieid, title, genres, link) => link}.collect()(0) == "https://www.themoviedb.org/movie/tmdbID-7")
  }


}
