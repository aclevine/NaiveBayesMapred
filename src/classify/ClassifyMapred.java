package classify;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
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

import java.lang.*;

public class ClassifyMapred {

	public static class ClassifyMapper extends Mapper<Text, Text, Text, Text> {
	
		HashMap<String, HashMap<String, Double>> labelScores = new HashMap<String, HashMap<String, Double>>();
		public static Double threshold = 0.0; // if you wanted to try and get exact number of labels instead of top 3 guesses
		
		protected void setup(Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO: read profession probabilities, feature given profession probabilites
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
			super.setup(context);
		}
		
		public void map(Text articleId, Text indices, Context context)
				throws IOException, InterruptedException {
			// TODO: build: 
			// < Title,  <top 3 predictions above threshold> ] > 
			StringIntegerList list = new StringIntegerList();	
			list.readFromString(indices.toString());
	        TreeMap<Double,String> guesses = new TreeMap<Double,String>(Collections.reverseOrder());			

	        for (String label : labelScores.keySet()) {
	        	HashMap<String, Double> scores = labelScores.get(label);
	        	Double prob = Math.log( scores.get("__LABEL__") );
	        	for (StringInteger index : list.getIndices()) {	
					String lemma = index.getString();
					int freq = index.getValue();
					if (scores.containsKey(lemma)) {
						prob += freq * Math.log(scores.get(lemma));
					}	
	        	}    	
	        	guesses.put(prob, label);
	        }
	         
			String[] top3 = new String[3];
			Integer i = 0;
	        for (Double prob: guesses.keySet()){
	        	if (i < 3 &&  prob >= threshold) {
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
	      System.err.println("Usage: ClassifyMapred <input-filepath> <output-filepath> <probabilities-filepath>");
	      System.exit(2);
	    }
		Job job = Job.getInstance(conf, "calculate top 3 professions per <title, lemmaList>");
		job.setJarByClass(ClassifyMapred.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(ClassifyMapper.class);
		//job.setCombinerClass(InvertedIndexReducer.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.addCacheFile(new Path(otherArgs[2]).toUri());

		job.getConfiguration().set("mapreduce.job.queuename", "hadoop14");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		job.addCacheFile(new Path("file").toUri());

	}		
}