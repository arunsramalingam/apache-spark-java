/**
 * Illustrates a sparksql operations in java
 */
package com.edu.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author sathiyarajan.m
 * Saprk SQL Word count example
 */
public class FileBatchProcessingSparkSQL {
	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "C:\\winutil\\");
		SparkConf driverConfigurations = new SparkConf().setMaster("local").setAppName("spark-word-count")
				.set("spark.driver.allowMultipleContexts", "true");
		@SuppressWarnings("resource")
		JavaSparkContext sparkContext = new JavaSparkContext(driverConfigurations);
		SparkSession sparkSession = SparkSession.builder().master("local").appName("spark-word-count").getOrCreate();
//		sparkSession.read().text("2017-07-17.log.txt").filter($"value").withColumn("words", split($"value", "\\s+")).
//		  select(explode($"words") as "word").
//		  groupBy("word").
//		  count.
//		  orderBy($"count".desc);
		JavaRDD<String> inputData = sparkContext.textFile("2017-07-17.log.txt");

		JavaRDD<String> filteredData = inputData.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = -4438640257249553509L;

			public Boolean call(String s) {
				return true;
			}
		});
		JavaRDD<CustomerData> inputBean = filteredData.map(new Function<String, CustomerData>() {
			public CustomerData call(String record) throws Exception {
				String[] plandata = record.split(";");
				CustomerData customerData = new CustomerData();
				customerData.setPlanName(plandata[0]);
				customerData.setUsage(plandata[1]);
				customerData.setCustomerName(plandata[2]);
				customerData.setMobile(plandata[3]);
				customerData.setCharge(plandata[4]);
				customerData.setTime(plandata[5]);
				customerData.setDate(plandata[6]);
				return customerData;
			}
		});
		Dataset<Row> inputsql = sparkSession.createDataFrame(inputBean, CustomerData.class);
		inputsql.createOrReplaceTempView("wordcounttable");
		Dataset<Row> sampleWords = sparkSession.sql("select * from wordcounttable");
		sampleWords.show();
		//word count using sparksql
		Dataset<Row> countWord = sparkSession.sql(
				"SELECT count(charge) + count(customerName) + count(date) + count(mobile)+ count(planName) + count(time) + count(usage) as totalcount FROM wordcounttable");
		countWord.show();
	}
}
