import java.io.File

import com.typesafe.config.ConfigFactory
import io.prometheus.client.{CollectorRegistry, Gauge}
import io.prometheus.client.exporter.PushGateway
import it.reply.data.pasquali.Storage
import org.apache.kudu.spark.kudu._
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession


object BatchETL {

/*
  //TODO in the next update
  // For now it will be do by batch reccomender

  val OUTPUT_MOVIES_BY_GENRES = "impala::datamart.genres"
*/

  var CONF_DIR = ""
  var CONFIG_FILE = "application.conf"
  var storage = Storage()

  def main(args: Array[String]): Unit = {

    val log = Logger.getLogger(getClass.getName)

    //CONF_DIR = scala.util.Properties.envOrElse("DEVOPS_CONF_DIR", "conf")
    //val configuration = ConfigFactory.parseFile(new File(s"${CONF_DIR}/${CONFIG_FILE}"))

    val configuration = ConfigFactory.load()

    log.info(configuration.toString)

    val INPUT_MOVIES = configuration.getString("betl.hive.input.movies")
    val INPUT_LINKS = configuration.getString("betl.hive.input.links")
    val INPUT_GTAGS = configuration.getString("betl.hive.input.gtags")
    val HIVE_DATABASE = configuration.getString("betl.hive.database")

    val KUDU_ADDRESS = configuration.getString("betl.kudu.address")
    val KUDU_PORT = configuration.getString("betl.kudu.port")
    val KUDU_MOVIES = configuration.getString("betl.kudu.movies_table")
    val KUDU_GTAGS = configuration.getString("betl.kudu.gtags_table")
    val KUDU_TABLE_BASE = configuration.getString("betl.kudu.table_base")
    val KUDU_DATABASE = configuration.getString("betl.kudu.database")

    val SPARK_APPNAME = configuration.getString("betl.spark.app_name")
    val SPARK_MASTER = configuration.getString("betl.spark.master")

    //**************************************************************
    // Metrics

    val ENV = configuration.getString("betl.metrics.environment")
    val JOB_NAME = configuration.getString("betl.metrics.job_name")

    val GATEWAY_ADDR = configuration.getString("betl.metrics.gateway.address")
    val GATEWAY_PORT = configuration.getString("betl.metrics.gateway.port")

    val LABEL_MOVIES_HIVE_NUMBER = s"${configuration.getString("betl.metrics.labels.movies_hive_number")}"
    val LABEL_LINKS_HIVE_NUMBER = s"${configuration.getString("betl.metrics.labels.links_hive_number")}"
    val LABEL_GTAGS_HIVE_NUMBER = s"${configuration.getString("betl.metrics.labels.genometags_hive_number")}"

    val LABEL_MOVIES_KUDU_NUMBER = s"${configuration.getString("betl.metrics.labels.movies_kudu_number")}"
    val LABEL_GTAGS_KUDU_NUMBER = s"${configuration.getString("betl.metrics.labels.genometags_kudu_number")}"

    val LABEL_PROCESS_DURATION = s"${configuration.getString("betl.metrics.labels.process_duration")}"

    //***************************************************************

    val pushGateway : PushGateway = new PushGateway(s"$GATEWAY_ADDR:$GATEWAY_PORT")
    val registry = new CollectorRegistry

    val gaugeMoviesHive : Gauge = Gauge.build().name(LABEL_MOVIES_HIVE_NUMBER)
      .help("\"Number of movie in the datalake before the ETL process\"").register(registry)

    val gaugeLinksHive : Gauge = Gauge.build().name(LABEL_LINKS_HIVE_NUMBER)
      .help("Number of links in the datalake before the ETL process").register(registry)

    val gaugeGtagsHive : Gauge = Gauge.build().name(LABEL_GTAGS_HIVE_NUMBER)
      .help("Number of genometags in the datalake befores the ETL process").register(registry)

    val gaugeMoviesKudu : Gauge = Gauge.build().name(LABEL_MOVIES_KUDU_NUMBER)
      .help("Number of enriched movies in the datamart after the ETL process").register(registry)

    val gaugeGTagsKudu : Gauge = Gauge.build().name(LABEL_GTAGS_KUDU_NUMBER)
      .help("Number of gtags in the datamart after the ETL process").register(registry)

    val gaugeDuration : Gauge = Gauge.build().name(LABEL_PROCESS_DURATION)
      .help("Duration of Batch ETL process").register(registry)

    //*************************************************************************

    storage = Storage()
      .init(SPARK_MASTER, SPARK_MASTER, withHive = true)
      .initKudu(KUDU_ADDRESS, KUDU_PORT, KUDU_TABLE_BASE)

    log.info(s"INPUT_MOVIES -> $INPUT_MOVIES")
    log.info(s"INPUT_LINKS -> $INPUT_LINKS")
    log.info(s"INPUT_GTAGS -> $INPUT_GTAGS")
    log.info(s"HIVE_DATABASE -> $HIVE_DATABASE")

    log.info(s"KUDU_ADDRESS -> $KUDU_ADDRESS")
    log.info(s"KUDU_PORT -> $KUDU_PORT")
    log.info(s"KUDU_MOVIES -> $KUDU_MOVIES")
    log.info(s"KUDU_GTAGS -> $KUDU_GTAGS")
    log.info(s"KUDU_TABLE_BASE -> $KUDU_TABLE_BASE")
    log.info(s"KUDU_DATABASE -> $KUDU_DATABASE")

    log.info(s"SPARK_APPNAME -> $SPARK_APPNAME")
    log.info(s"SPARK_MASTER -> $SPARK_MASTER")

    val OUTPUT_KUDU_MOVIES = s"$KUDU_DATABASE.$KUDU_MOVIES"
    val OUTPUT_KUDU_GTAGS = s"$KUDU_DATABASE.$KUDU_GTAGS"

    log.info(s"Kudu Master = $KUDU_ADDRESS:$KUDU_PORT")
    log.info(s"Kudu Gtag table = $OUTPUT_KUDU_GTAGS")
    log.info(s"Kudu Movies table = $OUTPUT_KUDU_MOVIES")


    log.warn("***** KUDU TABLES MUST EXISTS!!!!! *****")

    val genomeExists = storage.kuduContext.tableExists(OUTPUT_KUDU_GTAGS)
    val movieExists = storage.kuduContext.tableExists(OUTPUT_KUDU_MOVIES)

    if(!genomeExists || !movieExists){
      log.error("***** CREATE KUDU TABLES BEFORE RUN THIS SHIT *****")
      storage.closeSession()
      sys.exit(1)
    }

    log.info("***** Load Movies, Links and Genome_tags from Hive Data Lake *****")

    val timer : Gauge.Timer = gaugeDuration.startTimer()

    val movies = storage.readHiveTable(s"$HIVE_DATABASE.$INPUT_MOVIES")
    val links = storage.readHiveTable(s"$HIVE_DATABASE.$INPUT_LINKS")
    val gtags = storage.readHiveTable(s"$HIVE_DATABASE.$INPUT_GTAGS")

    gaugeMoviesHive.set(movies.count())
    gaugeLinksHive.set(movies.count())
    gaugeGtagsHive.set(movies.count())


    log.info("***** Do nothing on tags *****")

    val gtagCols = Seq("tagid", "tag")
    val outGTag = gtags.select(gtagCols.head, gtagCols.tail: _*)

    log.info("***** Merge movies and links (links are useless) *****")

    val outMovies = ETL.enrichMovies(movies, links)

    outMovies.show()

    log.info("***** Store Genometags and enriched movies to Kudu Data Mart *****")

    storage.upsertKuduRows(outGTag, OUTPUT_KUDU_GTAGS)
    storage.upsertKuduRows(outMovies, OUTPUT_KUDU_MOVIES)

    timer.setDuration()

    gaugeMoviesKudu.set(storage.readKuduTable(s"$KUDU_DATABASE.$KUDU_MOVIES").count())
    gaugeGTagsKudu.set(storage.readKuduTable(s"$KUDU_DATABASE.$KUDU_GTAGS").count())

    pushGateway.push(registry, s"${ENV}_${JOB_NAME}")

    log.info("***** Close Spark session *****")

    println("BATCH ETL PROCESS DONE")

    storage.closeSession()

  }

}
