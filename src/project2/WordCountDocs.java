package project2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDocs {

	public static class WordCountDocsMapper extends Mapper<Object,Text,Text, Text> {
		public void map(Object key, Text value, Context context){
			try{
				String[] wordAndDocCounter = value.toString().split("\t");
		        String[] wordAndDoc = wordAndDocCounter[0].split("@");
		        context.write(new Text(wordAndDoc[1]), new Text(wordAndDoc[0] + "=" + wordAndDocCounter[1]));
		        
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
	
	public static class WordCountDocsReducer extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values, Context context){
			int wordsSum = 0;
			Map<String, Integer> tempCounter = new HashMap<String, Integer>();
			try {
				for(Text val:values){
					String[] wordCounter = val.toString().split("=");
		            tempCounter.put(wordCounter[0], Integer.valueOf(wordCounter[1]));
		            wordsSum += Integer.parseInt(val.toString().split("=")[1]);
				}
				for (String wordKey : tempCounter.keySet()) {
		            context.write(new Text(wordKey + "@" + key.toString()), new Text(tempCounter.get(wordKey) + "/"
		                    + wordsSum));
		        }
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration config = new Configuration();
		Job job = Job.getInstance(config, "Word Counts for Docs");
		job.setJarByClass(WordCountDocs.class);
		job.setMapperClass(WordCountDocsMapper.class);
		job.setReducerClass(WordCountDocsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("tf-idf-1"));
		FileOutputFormat.setOutputPath(job, new Path("tf-idf-2"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}