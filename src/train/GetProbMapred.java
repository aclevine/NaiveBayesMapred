package train;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import utils.StringIntegerList;
import utils.StringIntegerList.StringInteger;
import utils.StringDoubleList;
import utils.StringDoubleList.StringDouble;

public class GetProbMapred {

	public static class GetCountMapper extends
		Mapper<Text, Text, String, StringDoubleList> {

		// Int article_counts
		// Hashmap article_per_profession_counts
		
		protected void setup(Mapper<Text, Text, String, StringDoubleList>.Context context)
				throws IOException, InterruptedException {
			// TODO: load articleCount, articlePerProfessionCounts

			// build articlePerProfessionProbs?
			
			super.setup(context);
		}

		
		public void map(Text articleId, Text LemmaList, Context context)
				throws IOException, InterruptedException {	
			// TODO: build: 
			//< String profession,  StringDoubleList [<lemma1, prob1>, <lemma2, prob2> ...] >
		}
	}

	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
		  System.err.println("Usage: InvertedIndexMapred <in> <out>");
		  System.exit(2);
		}
		Job job = Job.getInstance(conf, "get lemma probabilities per profession");
		job.setJarByClass(GetProbMapred.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringDoubleList.class);
		
		job.setMapperClass(GetCountMapper.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		job.getConfiguration().set("mapreduce.job.queuename", "hadoop14");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}	
}