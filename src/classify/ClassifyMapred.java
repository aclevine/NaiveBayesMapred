package classify;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import train.GetCountMapred;
import train.GetCountMapred.GetCountMapper;
import train.GetCountMapred.GetCountReducer;
import utils.StringIntegerList;

public class ClassifyMapred {

	public static class ClassifyMapper extends
		Mapper<Text, Text, Text, Text> {
	
		protected void setup(Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO: read profession probabilities, feature given profession proabilties

			
			super.setup(context);
					
		}
		
		public void map(Text articleId, Text indices, Context context)
				throws IOException, InterruptedException {	
			// TODO: build: 
			// < Title,  <top 3 predictions above threshold> ] > 		
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
		job.getConfiguration().set("mapreduce.job.queuename", "hadoop14");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}		
}
