package classify;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import utils.StringDoubleList;
import utils.StringDoubleList.StringDouble;
import utils.StringIntegerList;
import utils.StringIntegerList.StringInteger;


public class ClassifyMapred {

	public static class ClassifyMapper extends Mapper<Text, Text, Text, Text> {

		public static Set<String> peopleArticlesTitles = new HashSet<String>();
		public static Integer trainingArticleCount = 673988; //for normalization
		
		HashMap<String, HashMap<String, Double>> labelScores = new HashMap<String, HashMap<String, Double>>();
		
		protected void setup(Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//read probability file
			URI titleFile = context.getCacheFiles()[0];
            BufferedReader br = new BufferedReader(new FileReader(titleFile.getPath()));			
			String line;
            while ((line = br.readLine()) != null){
	        	String[] labelIndices = line.split("\t");
            	StringDoubleList list = new StringDoubleList();
            	list.readFromString(labelIndices[1].toString());
    			
    			HashMap<String, Double> temp = new HashMap<String, Double>();
    			for (StringDouble index : list.getIndices()) {    			
    				String lemma = index.getString();
    				Double prob = index.getValue();
    				temp.put(lemma, prob);
    			}
    			labelScores.put(labelIndices[0], temp);
            }
			br.close();
			super.setup(context);
		}
		
		public void map(Text articleId, Text indices, Context context)
				throws IOException, InterruptedException {
			// TODO: build: 
			// < Title,  <top 3 predictions above threshold> ] > 

			//need to normalize over all terms. otherwise any label 
			//with no observed tags in current doc will get best score
			Double normalizedGuess = 1.0 / trainingArticleCount; 
			
			StringIntegerList list = new StringIntegerList();	
			list.readFromString(indices.toString());
	        TreeMap<Double,String> guesses = new TreeMap<Double,String>(Collections.reverseOrder());			
	        for (String label : labelScores.keySet()) {
	        	HashMap<String, Double> scores = labelScores.get(label);
	        	//Double prob = Math.log( scores.get("__LABEL__") ); // include label weight
	        	Double prob = 0.0;
	        	for (StringInteger index : list.getIndices()) {	
					String lemma = index.getString();
					int freq = index.getValue();
					if (scores.containsKey(lemma)) {
						prob += freq * Math.log(scores.get(lemma));
					} else{
						prob += freq * Math.log(normalizedGuess);
						// need to normalize over all terms
						// otherwise any label with no features in 
						// current doc will get best score
					}
	        	}
	        	guesses.put(prob, label);
	        }
			String[] top3 = new String[3];
			Integer i = 0;
	        for (Double prob: guesses.keySet()){
	        	if (i < 3) {
	        		top3[i] = guesses.get(prob);
	        		i++;
	        	} else {
	        		context.write(articleId, new Text(": " + StringUtils.join(top3, ", ")));
	        		break;
	        	}
	        }
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 3) {
	      System.err.println("Usage: ClassifyMapred <input-filepath> <output-filepath> <probabilities-path>");
	      System.exit(2);
	    }
		Job job = Job.getInstance(conf, "calculate top 3 professions per <title, lemmaList>");
		job.setJarByClass(ClassifyMapred.class);
		job.setMapperClass(ClassifyMapper.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.addCacheFile(new Path(otherArgs[2]).toUri());

		job.getConfiguration().set("mapreduce.job.queuename", "hadoop14");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}		
}