import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.streaming.Trigger

object ENBDAssignment extends App {

	Logger.getLogger("org").setLevel(Level.ERROR)

	val spark = SparkSession.builder()
	.appName("ENBD Banking Account & Loan Streaming application")
	.master("local[*]")
	.config("spark.streaming.stopGracefullyOnShutdown", true)
	.config("spark.sql.shuffle.partitions", 3)/*Setting number of partitions after Shuffle/Join to 3 for better performance in this case of small dataset,
	default is 200*/
	.getOrCreate()

	//Defining Account Schema
	val accountSchema = StructType(List(
			StructField("AccountOpeningTime", TimestampType),
			StructField("Type", StringType),//Account
			StructField("AccountId", LongType),
			StructField("AccountType", IntegerType)))

	//Defining Loan Schema
	val loanSchema = StructType(List(
			StructField("LoanOpeningTime", TimestampType),
			StructField("Type", StringType),//Loan
			StructField("LoanId", LongType),
			StructField("AccountId_Lt", LongType),
			StructField("Amount", FloatType)))

	//read accounts streaming data from socket
	val accountDf = spark.readStream
	.format("socket")
	.option("host", "localhost")
	.option("port", "12341")
	.load()

        //read loan streaming data from socket
	val loanDf = spark.readStream
	.format("socket")
	.option("host", "localhost")
	.option("port", "12342")
	.load()

	//Structure the data based on the schema defined for Account Data
	val valueDF1 = accountDf.select(from_json(col("value"), accountSchema).alias("value"))
	val accountDfNew = valueDF1.select("value.*").withWatermark("AccountOpeningTime", "1 minute")/*Spark doesnt support two streaming dataset Joins 
	in complete/update mode, for using append mode we have to watermark the dataset*/

	//Structure the data based on the schema defined for Loan Data
	val valueDF2 = loanDf.select(from_json(col("value"), loanSchema).alias("value"))
	val loanDfNew = valueDF2.select("value.*").withWatermark("LoanOpeningTime", "1 minute")/*Spark doesnt support two streaming dataset Joins 
	in complete/update mode, for using append mode we have to watermark the dataset*/


	//Joining both Streams
	val joinExpr = expr("AccountId == AccountId_Lt AND LoanOpeningTime BETWEEN AccountOpeningTime AND AccountOpeningTime + interval 30 second")/*Setting the maximum 
	latency between the loan and account records as 30 seconds as per the problem statement*/
	val joinType = "inner"
	val joinedDF = accountDfNew.join(loanDfNew, joinExpr, joinType)
	.drop(loanDfNew.col("AccountId_Lt"))//Dropping duplicate column ie AccountId_Lt

	//Window Aggregations
	val windowAggDf = joinedDF.groupBy(window(col("AccountOpeningTime"), "1 minute"), col("AccountType"))/*Using tumbling window of 1 minute to calculate 
	output in every minute as per the problem statement*/
	.agg(count("LoanId").alias("TotalCount"), (sum("Amount").alias("TotalAmount")))/*Calculated last minute total count and amount. 
	Complete output mode is not supported by spark in two streaming dataset joins, hence not calculating total amount and total count from beginning*/

	val outputDf = windowAggDf.select("window.end", "AccountType", "TotalCount", "TotalAmount")
	//.orderBy("window.end")

	//write final output to sink
	val finalDF = outputDf.writeStream
	.format("console")
	.outputMode("append")/*As per the problem statement we have to use complete mode, to get anount and count from application start but 
	 Spark doesnt support two streaming dataset Joins in complete/update mode, hence using append mode*/
	.option("checkpointLocation", "checkpoint-location1")//Setting the checkpoint folder location for keeping the state information
	.trigger(Trigger.ProcessingTime("1 minute"))//Output batches will be generated in every minute
	.start()

	finalDF.awaitTermination()

}
