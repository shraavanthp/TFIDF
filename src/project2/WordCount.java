package project2;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.util.HashSet;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCount {

	private static HashSet<String> commonWords = new HashSet<String>();
	static {
	commonWords.add("a");
	commonWords.add("the");
	commonWords.add("are");
	commonWords.add("is");
	commonWords.add("was");
	commonWords.add("were");
	commonWords.add("i");
	commonWords.add("if");
	commonWords.add("by");
	commonWords.add("and");
	commonWords.add("will");
	commonWords.add("to");
	commonWords.add("or");
	commonWords.add("of");
	commonWords.add("from");
	commonWords.add("be");
	commonWords.add("as");
	commonWords.add("for");
	commonWords.add("what");
	commonWords.add("with");
	}

	public static class WordCountMapper extends Mapper<LongWritable,Text,Text, IntWritable>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{			
				Pattern p = Pattern.compile("\\w+");
		        Matcher m = p.matcher(value.toString());
		        
		        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();		       		       
		        
		        while(m.find()){
		        	StringBuilder keyString = new StringBuilder();
		        	String match = m.group().toLowerCase();
		        	if (!Character.isLetter(match.charAt(0)) || Character.isDigit(match.charAt(0)) || commonWords.contains(match)
		                    || match.contains("_"))
		        		continue;		        
		        	keyString.append(match);
		        	keyString.append("@");
		        	keyString.append(fileName);
				
		        	context.write(new Text(keyString.toString()), new IntWritable(1));
				} 
		}
	}
	
	public static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		public void reduce(Text keyString, Iterable<IntWritable> values, Context context){
			int totalCount = 0;
			try {
				for(IntWritable val:values){
					totalCount+=val.get();
				}
				context.write(keyString,new IntWritable(totalCount));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration config = new Configuration();
		Job job = Job.getInstance(config, "Word Frequency in Documents");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("input"));
		FileOutputFormat.setOutputPath(job, new Path("tf-idf-1"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
