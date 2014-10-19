package classify;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * This class is used for Section A of assignment 1. You are supposed to
 * implement a main method that has first argument to be the dump wikipedia
 * input filename , and second argument being an output filename that only
 * contains articles of people as mentioned in the people auxiliary file.
 */
public class GetTestMapred {

	//@formatter:off
	/**
	 * Input:
	 * 		articleTitle 	lemmaCounts
	 * Output
	 * 		articleTitle 	lemmaCounts
	 * @author Aaron
	 *
	 */
	//@formatter:on
	public static class GetTestMapper extends Mapper<Text, Text, Text, Text> {
		
		public static Set<String> peopleArticlesTitles = new HashSet<String>();

		@Override
		protected void setup(Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//read file
			InputStream is = this.getClass().getResourceAsStream("profession_test.txt");
			BufferedReader br = new BufferedReader(new InputStreamReader(is)); //Open text				
			String line;
            while ((line = br.readLine()) != null) {
                peopleArticlesTitles.add(line);
            }
			super.setup(context);
		}

		@Override
		public void map(Text articleId, Text indices, Context context)
				throws IOException, InterruptedException {
			String title = articleId.toString();
			if(peopleArticlesTitles.contains(title)) {
				context.write(new Text(title), new Text(indices.toString()));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		//Get Articles
		Configuration conf = new Configuration();        
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: GetArticlesMapred <input-filepath> <output-filepath>");
	      System.exit(2);
	    }		
		Job job = Job.getInstance(conf, "filter down to test data");
        job.setJarByClass(GetTestMapred.class);
        job.setMapperClass(GetTestMapper.class);
		
        job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);        

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
		job.getConfiguration().set("mapreduce.job.queuename", "hadoop14");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
