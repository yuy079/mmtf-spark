/**
 * 
 */
package edu.sdsc.mmtf.spark.ml;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 
 */
public class parquetReader {

	/**
	 * 
	 */
	public static void main(String[] args) throws IOException {

		if (args.length != 1) {
			System.err.println("Usage: " + parquetReader.class.getSimpleName() + " <parquet file> ");
			System.exit(1);
		}

		long start = System.nanoTime();

		SparkSession spark = SparkSession
				.builder()
				.master("local[*]")
				.appName(parquetReader.class.getSimpleName())
				.getOrCreate();

		Dataset<Row> data = spark.read().parquet(args[0]);
		System.out.println(data.count());
		data.show(10, false);
		long end = System.nanoTime();

		System.out.println((end-start)/1E9 + " sec");

		
	}
}
