/**
 * 
 */
package edu.sdsc.mmtf.spark.ml.demos;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.biojava.nbio.structure.StructureException;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.datasets.SecondaryStructureSegmentExtractor;
import edu.sdsc.mmtf.spark.filters.ContainsLProteinChain;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;
import edu.sdsc.mmtf.spark.ml.SequenceWord2Vector;
import edu.sdsc.mmtf.spark.rcsbfilters.Pisces;

/**
 * This class creates a dataset of sequence segments derived
 * from a non-redundant. The dataset contains the sequence segment,
 * the DSSP Q8 and DSSP Q3 code of the center residue in a sequence
 * segment, and a Word2Vec encoding of the sequence segment.
 * The dataset is saved in JSON file specified by the user.
 * 
 * @author Yue Yu
 */
public class SecondaryStructureWord2VecEncoder {

	/**
	 * @param args args[0] path of the output file
	 * @throws IOException 
	 * @throws StructureException 
	 */
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
		
		// read MMTF Hadoop sequence file and create a non-redundant set (<=20% seq. identity)
		// of L-protein chains
		int sequenceIdentity = 20;
		double resolution = 2.0;
		double fraction = 0.1;
		long seed = 123;
		
		JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader
				.readSequenceFile(path, sc)
				.flatMapToPair(new StructureToPolymerChains())
				.filter(new Pisces(sequenceIdentity, resolution)) // The pisces filter
				.filter(new ContainsLProteinChain()) // filter out for example D-proteins
                .sample(false,  fraction, seed);
		
		// get content
		int segmentLength = 25; 
		Dataset<Row> data = SecondaryStructureSegmentExtractor.getDataset(pdb, segmentLength);
	
		// create a Word2Vector representation of the protein sequences
		int n = 2; // create 2-grams
		int windowSize = segmentLength;
		int vectorSize = 50; // dimension of feature vector	(50)
		
		data = SequenceWord2Vector.addFeatureVector(data, n, windowSize, vectorSize).cache();
		data.show(25, false);
		
		// coalesce data to write a single json file
		data = data.coalesce(1);
		data.write().mode("overwrite").format("json").save(args[0]);
		
		long end = System.nanoTime();

		System.out.println(TimeUnit.NANOSECONDS.toSeconds(end-start) + " sec.");
	}
}