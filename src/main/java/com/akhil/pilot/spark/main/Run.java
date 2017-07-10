package com.akhil.pilot.spark.main;

import com.akhil.pilot.spark.worker.WordCount;

public class Run {

	/**
	 * This is the entry point when the task is called from command line with
	 * spark-submit.sh script.
	 */
	public static void main(String[] args) {
		if(null != args && args.length > 0) {
			new WordCount().doRun(args[0]);
		} else {
			System.out.println("Please provide the path of input file ");
		}
	}
}
