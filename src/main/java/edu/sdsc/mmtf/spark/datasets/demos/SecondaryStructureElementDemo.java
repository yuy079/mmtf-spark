package edu.sdsc.mmtf.spark.datasets.demos;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.datasets.SecondaryStructureElementExtractor;
import edu.sdsc.mmtf.spark.filters.ContainsLProteinChain;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;
import edu.sdsc.mmtf.spark.ml.ProteinSequenceEncoder;
import edu.sdsc.mmtf.spark.rcsbfilters.Pisces;

public class SecondaryStructureElementDemo {
	public static void main(String[] args) throws IOException {    
	    long start = System.nanoTime();
		String path = System.getProperty("MMTF_REDUCED");
  
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(CustomReportDemo.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    int sequenceIdentity = 20;
		double resolution = 2.0;
		double fraction = 1.0;
		long seed = 123;
		
		JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader
				.readSequenceFile(path, sc)
				.flatMapToPair(new StructureToPolymerChains())
				.filter(new Pisces(sequenceIdentity, resolution)) // The pisces filter
				.filter(new ContainsLProteinChain()) // filter out for example D-proteins
                .sample(false,  fraction, seed);
		
	    Dataset<Row> ds = SecondaryStructureElementExtractor.getDataset(pdb, "H", 6);
		ProteinSequenceEncoder encoder = new ProteinSequenceEncoder(ds);
		int n = 2;
		ds = encoder.overlappingNgramWord2VecEncode("D://w2vModel", n).cache();
	    // show the top 50 rows of this dataset
	    ds.show(50, false);
	    ds = ds.coalesce(1);

	    ds.write().mode("overwrite").format("json").save("D://pure_h");
	    long end = System.nanoTime();
	    
	    System.out.println("Time: " + TimeUnit.NANOSECONDS.toSeconds(end-start) + " sec.");
	    
	    sc.close();
	}
}