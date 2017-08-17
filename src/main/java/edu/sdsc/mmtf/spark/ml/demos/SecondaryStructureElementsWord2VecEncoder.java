package edu.sdsc.mmtf.spark.ml.demos;

import java.io.IOException;
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

/**
 * 
 */
public class SecondaryStructureElementsWord2VecEncoder {
	public static void main(String[] args) throws IOException {

		String path = System.getProperty("MMTF_REDUCED");
	    if (path == null) {
	    	    System.err.println("Path for Hadoop sequence file has not been set");
	        System.exit(-1);
	    }
	    
		if (args.length != 2) {
			System.err.println("Usage: " + SecondaryStructureWord2VecEncoder.class.getSimpleName() + " <outputFilePath> + <fileFormat>");
			System.exit(1);
		}


		long start = System.nanoTime();

		SparkConf conf = new SparkConf()
				.setMaster("local[*]")
				.setAppName(SecondaryStructureWord2VecEncoder.class.getSimpleName());
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
			
		Dataset<Row> data = SecondaryStructureElementExtractor.getDataset(pdb, "E");
		System.out.println(data.count());
		data.show(10,false);
		

		// add Word2Vec encoded feature vector
		ProteinSequenceEncoder encoder = new ProteinSequenceEncoder(data);
		data = encoder.oneHotEncode();
		
		if (args[1].equals("json")) {
			// coalesce data into a single file
		    data = data.coalesce(1);
		}
		
		data.show(50,false);
		
		data.write().mode("overwrite").format(args[1]).save(args[0]);
		
		long end = System.nanoTime();

		System.out.println(TimeUnit.NANOSECONDS.toSeconds(end-start) + " sec.");
	}
}
