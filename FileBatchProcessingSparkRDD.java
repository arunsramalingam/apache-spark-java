package com.edu.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class FileBatchProcessingSparkRDD {

	@SuppressWarnings({ "serial", "resource" })
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\winutil\\");
		String inputFile = "2017-07-17.log.txt";
		String outputFile = "output.txt";

		// Create a Java Spark Context.
		SparkConf conf = new SparkConf().setAppName("CRD_Manipulation").setMaster("local")
				.set("spark.executor.memory", "1g");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Laod Data
		JavaRDD<String> input = sc.textFile(inputFile);

//		JavaRDD<String> requiredLines = input.filter(new Function<String, Boolean>() {
//			public Boolean call(String line) throws Exception {
//				boolean postpaid = false;
//				if (line.contains("SMS") || line.contains("UPdaya")) {
//					postpaid = true;
//				}
//				return postpaid;
//			}
//		});

		JavaRDD<String> addedrecord = input.map(new Function<String, String>() {
			public String call(String record) throws Exception {
				String iD = "";
				String rcd[] = record.split(";");
				String mobile = rcd[3];
				String month = rcd[1];
				iD = mobile + "-" + month;
				return record + ";" + iD;
			}
		});

		JavaPairRDD<String, Integer> counts = addedrecord.mapToPair(new PairFunction<String, String, Integer>() {
			@SuppressWarnings({ "unchecked", "rawtypes" })
			public Tuple2<String, Integer> call(String x) {
				String rcd[] = x.split(";");
				String key = rcd[0];
				int value = Integer.parseInt(rcd[3]);
				return new Tuple2(key, value);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer x, Integer y) {
				return x + y;
			}
		});
		System.out.println("Result : " + counts);
		counts.saveAsTextFile(outputFile);

	}
}
