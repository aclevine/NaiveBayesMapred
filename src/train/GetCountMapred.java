package train;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import utils.StringIntegerList;
import utils.StringIntegerList.StringInteger;

public class GetCountMapred {

	public static class GetCountMapper extends
		Mapper<Text, Text, Text, StringIntegerList> {
		
		public static HashMap<String, String> titleprofession = new HashMap<String, String>();
		
		protected void setup(Mapper<Text, Text, Text, StringIntegerList>.Context context)
				throws IOException, InterruptedException {
			InputStream is = this.getClass().getResourceAsStream("profession_train.txt");
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			String line;
	        while ((line = br.readLine()) != null) {
	        	//split by " : "
	        	String[] keyvalue = line.split(" : ");
	        	String[] professions = keyvalue[1].split(", ");
	        	//decode url encoding
	        	//URLDecoder dc = new URLDecoder();
	    		//String title = dc.decode(keyvalue[0], "US-ASCII");
	        	String title = keyvalue[0];
	        	//convert to hashmap
	    		for (int i=0; i<professions.length; i++) {
	    			titleprofession.put(title, professions[i]);
	    		}
	        }
			super.setup(context);
		}

		public void map(Text articleId, Text indices, Context context)
				throws IOException, InterruptedException {	
			//get title
			String title = articleId.toString();
			String profession = titleprofession.get(title.toString());
			//translate list indices to StringIntegerList
			StringIntegerList list = new StringIntegerList();
			list.readFromString(indices.toString());
			//write profession and StringIntegerList to context
			if(profession != null) {
				context.write(new Text(profession), list);
			}
		}
	}
	
	
	public static class GetCountReducer extends 
		Reducer<Text, StringIntegerList, Text, StringIntegerList> {
		
		public static int articleCount = 0;
		public static HashMap<String, Integer> articlePerProfessionCount = new HashMap<String, Integer>();
		MultipleOutputs<Text,Integer> mos;
		
		protected void setup(Reducer<Text, StringIntegerList, Text, StringIntegerList>.Context context)
				throws IOException, InterruptedException {
	    	mos = new MultipleOutputs(context);
			super.setup(context);
		}
		
	    public void reduce(Text Profession, Iterable<StringIntegerList> lemmaCounts, Context context) 
	    		throws IOException, InterruptedException {
		    HashMap<String, Integer> wordcount = new HashMap();
	    	Integer professionCount = 0;
	    	for (StringIntegerList lc : lemmaCounts) {
	    		articleCount++;
	    		professionCount++;
	    		List<StringInteger> l = lc.getIndices();
	    		for(StringInteger si : l) {
	    			String lemma = si.getString();
	    			if(wordcount.containsKey(si.getString())) {
	                    wordcount.put(lemma,wordcount.get(lemma)+1);
	                } else {
	                    wordcount.put(lemma, 1);
	                }
	    		}
	    	}
	    	articlePerProfessionCount.put(Profession.toString(), professionCount);
	    	mos.write("ArticleCounts", Profession, professionCount);
	    	context.write(Profession, new StringIntegerList(wordcount));
	    }
	    
	    protected void cleanup(Reducer<Text, StringIntegerList, Text, StringIntegerList>.Context context)
				throws IOException, InterruptedException {
	    	mos.close();
			super.cleanup(context);
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: InvertedIndexMapred <in> <out>");
	      System.exit(2);
	    }
		Job job = Job.getInstance(conf, "get lemma counts per profession");
		job.setJarByClass(GetCountMapred.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringIntegerList.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringIntegerList.class);
		job.setMapperClass(GetCountMapper.class);
		job.setReducerClass(GetCountReducer.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		// Defines additional single text based output 'text' for the job
		 MultipleOutputs.addNamedOutput(job, "ArticleCounts", TextOutputFormat.class,
		 Text.class, Integer.class);
		job.getConfiguration().set("mapreduce.job.queuename", "hadoop14");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}	
}