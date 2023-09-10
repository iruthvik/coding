import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.util.concurrent.Executors
import scala.concurrent.duration._

object StreamingJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("StreamingJob")
      .getOrCreate()

    // Define your schema
    val ElineRefschema = ???

    import spark.implicits._

    // Define a function to update the reference data
    def updateReferenceData(referencePath: String): Unit = {
      // Read the updated reference data into a new DataFrame
      val newReferenceDF = spark.read
        .option("delimiter", "|")
        .schema(ElineRefschema)
        .csv(referencePath)

      // Perform any necessary transformations on newReferenceDF

      // Register the new reference DataFrame as a temporary view
      newReferenceDF.createOrReplaceTempView("reference_data")
    }

    // Schedule the updateReferenceData function to run every 24 hours
    val executor = Executors.newScheduledThreadPool(1)

    executor.scheduleAtFixedRate(new Runnable {
      def run(): Unit = {
        // Update the reference data within each batch
        updateReferenceData("gs://internal/cpa/date=20230905")
      }
    }, 0, 1.day.toMillis, TimeUnit.MILLISECONDS)

    val streamingDF = spark.readStream
      .format("pulsar")
      .option("topic", "cpa")
      .load()

    val selectDF = streamingDF.withColumn("valueString", $"value".cast("string"))

    val parseDF = selectDF.withColumn("value1", from_json($"valueString", JsonSchema)).select($"value1.*")

    val filterDF = parseDF.filter($"dataType" === "IP")

    // Use foreachBatch to process each batch of data
    filterDF.writeStream
      .foreachBatch { (batchDF, batchId) =>
        // Perform the join with the updated reference data
        val refJoin = batchDF.join(
          spark.table("reference_data"),
          batchDF("devicename") === spark.table("reference_data")("switch_name") && batchDF("if") === spark.table("reference_data")("ef")
        )

        // Write the result to ORC or perform other processing
        refJoin.write
          .format("orc")
          .option("path", outputLocation)
          .mode("append")
          .save()
      }
      .start()
      .awaitTermination()
  }
}
