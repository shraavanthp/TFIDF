package project2;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TFIDF {

	public static class TFIDFMapper extends Mapper<LongWritable,Text,Text,Text> {
		public void map(LongWritable key, Text value, Context context) {
			try {
			String[] wordAndCounters = value.toString().split("\t");
	        String[] wordAndDoc = wordAndCounters[0].split("@");
				context.write(new Text(wordAndDoc[0]), new Text(wordAndDoc[1] + "=" + wordAndCounters[1]));
			} catch (Exception e) {				
				e.printStackTrace();
			}
		}
	}
	
	public static class TFIDFReducer extends Reducer<Text,Text,Text,Text>{
		private static final DecimalFormat DF = new DecimalFormat("###.########");
		public void reduce(Text key, Iterable<Text> values, Context context){
			try {
				int numDocsInCorpus = Integer.parseInt(context.getJobName());
				int numOfDocsInCorpusWithKey = 0;
				Map<String, String> tempFrequencies = new HashMap<String, String>();
				for (Text val : values) {
					String[] documentAndFrequencies = val.toString().split("=");
					numOfDocsInCorpusWithKey++;
					tempFrequencies.put(documentAndFrequencies[0], documentAndFrequencies[1]);
				}
				for (String document : tempFrequencies.keySet()) {
					String[] wordFrequenceAndTotalWords = tempFrequencies.get(document).split("/");
					double tf = Double.valueOf(Double.valueOf(wordFrequenceAndTotalWords[0])/ Double.valueOf(wordFrequenceAndTotalWords[1]));
					double idf = (double) numDocsInCorpus / (double) numOfDocsInCorpusWithKey;
					double tfIdf = new double();
					if(numDocsInCorpus == numOfDocsInCorpusWithKey)
						tfIdf = tf * Math.log10(idf);	            
					else
						tfIdf = tf * Math.log10(idf);
					context.write(new Text(key + "@" + document), new Text("[" + numOfDocsInCorpusWithKey + "/"+ numDocsInCorpus + " , " + wordFrequenceAndTotalWords[0] + "/"+ wordFrequenceAndTotalWords[1] + " , " + DF.format(tfIdf) + "]"));
				}
	        }catch (Exception e) {
				e.printStackTrace();
			} 
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration config = new Configuration();
		Job job = Job.getInstance(config, "TFIDF Computation");
		job.setJarByClass(TFIDF.class);
		job.setMapperClass(TFIDFMapper.class);
		job.setReducerClass(TFIDFReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("tf-idf-2"));
		FileOutputFormat.setOutputPath(job, new Path("tf-idf-3"));
		Path inputPath = new Path("input");
		FileSystem fs = inputPath.getFileSystem(config);
		FileStatus[] stats = fs.listStatus(inputPath);
		job.setJobName(String.valueOf(stats.length));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
