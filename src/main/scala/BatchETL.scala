import java.io.File

import com.typesafe.config.ConfigFactory
import it.reply.data.pasquali.Storage
import org.apache.kudu.spark.kudu._
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession


object BatchETL {

  var INPUT_MOVIES = ""
  var INPUT_LINKS = ""
  var INPUT_GTAGS = ""
  var HIVE_DATABASE = ""

  var KUDU_ADDRESS = "" //"cloudera-vm.c.endless-upgrade-187216.internal:7051"
  var KUDU_PORT = ""

  var KUDU_MOVIES = ""
  var KUDU_GTAGS = ""
  var OUTPUT_KUDU_MOVIES = ""
  var OUTPUT_KUDU_GTAGS = ""
  var KUDU_TABLE_BASE = ""
  var KUDU_DATABASE = ""

  var SPARK_APPNAME = ""
  var SPARK_MASTER = ""

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

    INPUT_MOVIES = configuration.getString("betl.hive.input.movies")
    INPUT_LINKS = configuration.getString("betl.hive.input.links")
    INPUT_GTAGS = configuration.getString("betl.hive.input.gtags")
    HIVE_DATABASE = configuration.getString("betl.hive.database")

    KUDU_ADDRESS = configuration.getString("betl.kudu.address")
    KUDU_PORT = configuration.getString("betl.kudu.port")
    KUDU_MOVIES = configuration.getString("betl.kudu.movies_table")
    KUDU_GTAGS = configuration.getString("betl.kudu.gtags_table")
    KUDU_TABLE_BASE = configuration.getString("betl.kudu.table_base")
    KUDU_DATABASE = configuration.getString("betl.kudu.database")

    SPARK_APPNAME = configuration.getString("betl.spark.app_name")
    SPARK_MASTER = configuration.getString("betl.spark.master")

    log.info(s"INPUT_MOVIES -> ${INPUT_MOVIES}")
    log.info(s"INPUT_LINKS -> ${INPUT_LINKS}")
    log.info(s"INPUT_GTAGS -> ${INPUT_GTAGS}")
    log.info(s"HIVE_DATABASE -> ${HIVE_DATABASE}")

    log.info(s"KUDU_ADDRESS -> ${KUDU_ADDRESS}")
    log.info(s"KUDU_PORT -> ${KUDU_PORT}")
    log.info(s"KUDU_MOVIES -> ${KUDU_MOVIES}")
    log.info(s"KUDU_GTAGS -> ${KUDU_GTAGS}")
    log.info(s"KUDU_TABLE_BASE -> ${KUDU_TABLE_BASE}")
    log.info(s"KUDU_DATABASE -> ${KUDU_DATABASE}")

    log.info(s"SPARK_APPNAME -> ${SPARK_APPNAME}")
    log.info(s"SPARK_MASTER -> ${SPARK_MASTER}")

    storage = Storage()
      .init(SPARK_MASTER, SPARK_MASTER, withHive = true)

    val spark = SparkSession.builder().master(SPARK_MASTER).appName(SPARK_APPNAME).getOrCreate()
    var kuduContext = new KuduContext(s"$KUDU_ADDRESS:$KUDU_PORT", spark.sparkContext)




    log.info(s"Kudu Master = $KUDU_ADDRESS:$KUDU_PORT")
    log.info(s"Kudu Gtag table = $OUTPUT_KUDU_GTAGS")
    log.info(s"Kudu Movies table = $OUTPUT_KUDU_MOVIES")


    log.warn("***** KUDU TABLES MUST EXISTS!!!!! *****")

    val genomeExists = kuduContext.tableExists(OUTPUT_KUDU_GTAGS)
    val movieExists = kuduContext.tableExists(OUTPUT_KUDU_MOVIES)

    if(!genomeExists || !movieExists){
      log.error("***** CREATE KUDU TABLES BEFORE RUN THIS SHIT *****")
      spark.stop()
      sys.exit(1)
    }

    log.info("***** Load Movies, Links and Genome_tags from Hive Data Lake *****")

//    val movies = spark.sql(s"select * from ${INPUT_MOVIES}")
//    val links = spark.sql(s"select * from ${INPUT_LINKS}")
//    val gtags = spark.sql(s"select * from ${INPUT_GTAGS}")

    val movies = storage.readHiveTable(s"$HIVE_DATABASE.$INPUT_MOVIES")
    val links = storage.readHiveTable(s"$HIVE_DATABASE.$INPUT_LINKS")
    val gtags = storage.readHiveTable(s"$HIVE_DATABASE.$INPUT_GTAGS")

    movies.show()
    links.show()
    gtags.show()

    log.info("***** Do nothing on tags *****")

    val gtagCols = Seq("tagid", "tag")
    val outGTag = gtags.select(gtagCols.head, gtagCols.tail: _*)

    log.info("***** Merge movies and links (links are useless) *****")

    val outMovies = ETL.enrichMovies(movies, links)

    outMovies.show()

    log.info("***** Store Genometags and enriched movies to Kudu Data Mart *****")

    OUTPUT_KUDU_MOVIES = s"$KUDU_TABLE_BASE::$KUDU_DATABASE.$KUDU_MOVIES"


    kuduContext.upsertRows(outGTag, OUTPUT_KUDU_GTAGS)
    kuduContext.upsertRows(outMovies, OUTPUT_KUDU_MOVIES)

    log.info("***** Close Spark session *****")

    println("BATCH ETL PROCESS DONE")

    spark.stop()

  }

}
