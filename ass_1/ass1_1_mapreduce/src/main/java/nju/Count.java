package nju;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.io.IntWritable.Comparator;
import org.apache.hadoop.io.WritableComparable;

public class Count {

  public static class StepOneMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    static enum CountersEnum { INPUT_WORDS }

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    private Configuration conf;
    private BufferedReader fis;

    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      if (key.toString().equals("0")) {
        return;
      }

      String line = value.toString();
      String item[] = line.split(",");
      String action_type = item[item.length - 1];
      if(action_type.equals("1") || action_type.equals("2") || action_type.equals("3")) {
        word.set(item[1]);
        context.write(word,one);
      }
    }
  }

  public static class StepOneReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
    public int compare(WritableComparable a,WritableComparable b){
        return -super.compare(a, b);
      }
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return -super.compare(b1, s1, l1, b2, s2, l2);
      }     
  }

	public static class StepTwoReducer extends Reducer <IntWritable,Text,Text,IntWritable>{
		private int cnt=0;
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val:values){
				cnt = cnt+1;
				if(cnt > 100)
					return;
				else{
					String t = cnt + ":" + val.toString() + ",";
					Text WORD = new Text(t);
					context.write(WORD, key);
				}
			}
		}
	}



  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "count");
    job.setJarByClass(Count.class);
    job.setMapperClass(StepOneMapper.class);
    job.setCombinerClass(StepOneReducer.class);
    job.setReducerClass(StepOneReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);



    Path intermediatePath = new Path("IntermediateOutput");
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, intermediatePath);
    job.waitForCompletion(true);

    System.out.println("开始第二个任务");

    Job sortjob = Job.getInstance(conf, "count sort");
    sortjob.setJarByClass(Count.class);
    FileInputFormat.addInputPath(sortjob, intermediatePath);
    FileOutputFormat.setOutputPath(sortjob, new Path(args[1]));
    sortjob.setInputFormatClass(SequenceFileInputFormat.class);
    sortjob.setMapperClass(InverseMapper.class);
    sortjob.setReducerClass(StepTwoReducer.class);
    sortjob.setSortComparatorClass(IntWritableDecreasingComparator.class);
    //sortjob.setOutputKeyClass(IntWritable.class);
    //sortjob.setOutputValueClass(Text.class);
    sortjob.setOutputKeyClass(Text.class);
    sortjob.setOutputValueClass(IntWritable.class);
    sortjob.setMapOutputKeyClass(IntWritable.class);
    sortjob.setMapOutputValueClass(Text.class);

    sortjob.waitForCompletion(true);

    FileSystem.get(conf).delete(intermediatePath);
    System.exit(sortjob.waitForCompletion(true) ? 0 : 1);
  }
}