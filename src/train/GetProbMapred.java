package train;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

	public static class GetProbMapper extends
		Mapper<Text, Text, Text, Text> {

        public static Map<String, Integer> professionCount = new HashMap<String, Integer>();
		public static Integer totalCount = 0;
		protected void setup(Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO: load articleCount, articlePerProfessionCounts
			InputStream is = this.getClass().getResourceAsStream("ArticleCounts");
			BufferedReader br = new BufferedReader(new InputStreamReader(is)); //Open text				

            String line;
			while ((line = br.readLine()) != null) {
				String[] splitter = line.split("\t");		
				professionCount.put(splitter[0],  Integer.parseInt(splitter[1])); 
				totalCount += Integer.parseInt(splitter[1]);
			}
			super.setup(context);
		}

		public void map(Text profession, Text LemmaList, Context context)
				throws IOException, InterruptedException {		
			StringIntegerList list = new StringIntegerList();
			list.readFromString(LemmaList.toString());
			Map<String, Double> lemmaProb = new HashMap<String, Double>();
			lemmaProb.put("__LABEL__", (double)totalCount/professionCount.get(profession.toString()));
			for (StringInteger stringInt : list.getIndices()) {
				String lemma = stringInt.getString();
				Integer count = stringInt.getValue();	
				double probability = (double)count / professionCount.get(profession.toString());
				lemmaProb.put(lemma, probability);
			}
			StringDoubleList sdl = new StringDoubleList(lemmaProb);
			context.write(new Text(profession.toString()), new Text(sdl.toString()));
		}
	}

	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: GetProbMapred <input-filepath> <output-filepath>");
	      System.exit(2);
	    }
		Job job = Job.getInstance(conf, "get lemma probabilities per profession");
		job.setJarByClass(GetProbMapred.class);
		job.setMapperClass(GetProbMapper.class);
        //job.setNumReduceTasks(0);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.getConfiguration().set("mapreduce.job.queuename", "hadoop14");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}