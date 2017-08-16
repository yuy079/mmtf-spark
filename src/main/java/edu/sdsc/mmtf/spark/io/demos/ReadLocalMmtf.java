package edu.sdsc.mmtf.spark.io.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.analysis.TraverseStructureHierarchy;
import edu.sdsc.mmtf.spark.datasets.SecondaryStructureExtractor;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;

/**
 * Example reading a list of PDB IDs from a local 
 * reduced MMTF Hadoop sequence file into a JavaPairRDD.
 * 
 * @author Peter Rose
 *
 */
public class ReadLocalMmtf {

	public static void main(String[] args) {  
		
		String path = "D://bioAss";
//	    if (path == null) {
//	    	    System.err.println("Environment variable for Hadoop sequence file has not been set");
//	        System.exit(-1);
//	    }
	    
	    // instantiate Spark. Each Spark application needs these two lines of code.
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(ReadLocalMmtf.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // read list of PDB entries from a local Hadoop sequence file

	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readMmtfFiles(path, sc);
	    
	    pdb.foreach(t -> TraverseStructureHierarchy.demo(t._2));
	    System.out.println("# structures: " + pdb.count());
	    
	    // close Spark
	    sc.close();
	}
}
