package com.akhil.pilot.spark.worker;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * WordCount class, we will call this class with the test WordCountTest.
 */
public class WordCount {

	public void doRun(String inputFilePath) {
		run(inputFilePath);
	}

	/**
	 * The task body
	 */
	private void run(String inputFilePath) {
		/*
		 * This is the address of the Spark cluster. We will call the task from
		 * WordCountTest and we use a local stand alone cluster. [*] means use
		 * all the cores available. See {@see
		 * http://spark.apache.org/docs/latest/submitting-applications.html#
		 * master-urls}.
		 */
		String master = "local[*]";

		System.out.println("Input file is ::::::::::::::::::::::::: " + inputFilePath);

		/*
		 * Initializes a Spark context.
		 */
		SparkConf conf = new SparkConf().setAppName(WordCount.class.getName()).setMaster(master);
		JavaSparkContext context = new JavaSparkContext(conf);

		/*
		 * Performs a work count sequence of tasks and prints the output to the
		 * console.
		 */
		context.textFile(inputFilePath).flatMap(text -> Arrays.asList(text.split(" ")).iterator())
				.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((a, b) -> a + b)
				.foreach(result -> System.out.println(String.format("Word [%s] count [%d].", result._1(), result._2)));

		// Closes the spark context
		context.close();
	}
}
