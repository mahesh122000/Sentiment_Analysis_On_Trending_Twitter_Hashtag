package hadoop;

import java.util.*;

import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.util.StringTokenizer;


public class TrendCount {
    
    
	
	public static class CountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	    
    @Override
	public void map(LongWritable key, Text value,OutputCollector<Text,IntWritable> output,Reporter reporter ) throws IOException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {        	
        	String token = tokenizer.nextToken();
			if(token.startsWith("#")){
        		output.collect(new Text(token), new IntWritable(1));
        	}
        }
    }
}

	
	
	
	public static class CountReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    
    @Override
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text,IntWritable> output,Reporter reporter) throws IOException {
        int sum = 0;
        while(values.hasNext()) {
            sum += values.next().get();
        }
        output.collect(key, new IntWritable(sum));
    }
}
	

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(TrendCount.class);
        conf.setJobName("Hashtags_Counter");     
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        
        conf.setMapperClass(CountMapper.class);
        conf.setCombinerClass(CountReducer.class);
        conf.setReducerClass(CountReducer.class);
        
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}