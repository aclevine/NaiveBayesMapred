package train;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import utils.StringIntegerList;
import utils.StringIntegerList.StringInteger;

public class GetCountMapred {

	public static class GetCountMapper extends
		Mapper<Text, Text, Text, StringIntegerList> {
		
		protected void setup(Mapper<Text, Text, Text, StringIntegerList>.Context context)
				throws IOException, InterruptedException {
			// TODO: read profession_train, create hashmap article_name -> professions
		
			InputStream is = this.getClass().getResourceAsStream("profession_train.txt");
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			String line;
	        while ((line = br.readLine()) != null) {
	        
	        	//convert to hashmap
	        
	        }
			super.setup(context);
		}
		
		public void map(Text articleId, Text indices, Context context)
				throws IOException, InterruptedException {	
			// TODO: build: 
			// < String profession,  StringIntegerList [<lemma1, count1>, <lemma2, count2> ...] > 		
		}
	}
	
	
	public static class GetCountReducer extends 
		Reducer<Text, StringIntegerList, Text, StringIntegerList> {

		// TODO: initiate sums for articleCount, articlePerProfessionCounts
		private IntWritable articleCount = new IntWritable();
	    // articlePerProfessionCounts = ?
		
	    public void reduce(Text Profession, Iterable<StringIntegerList> lemmaCounts, Context context) 
	    		throws IOException, InterruptedException {
		    // TODO: get sums for article counts, article per profession counts, lemma per profession counts.
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
		//job.setCombinerClass(InvertedIndexReducer.class);
		job.setReducerClass(GetCountReducer.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		job.getConfiguration().set("mapreduce.job.queuename", "hadoop14");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}	
}