
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LongestWord {


    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        private Text word = new Text();
        private int maxLength = 0;

        public void map(Object key, Text value,  Context context) throws IOException, InterruptedException
        {
            StringTokenizer s = new StringTokenizer(value.toString());
            while (s.hasMoreTokens())
            {
                if (maxLength < s.nextToken().length())
                {
                    maxLength = s.nextToken().length();
                }
            }
            while (s.hasMoreTokens())
            {
                word.set(s.nextToken());
                context.write(word, new IntWritable(maxLength));
            }

        }
    }

    public static class IntSumReducer extends Reducer<IntWritable, Text, Text, IntWritable>
    {
        public void reduce(Text key, Iterator <IntWritable> values, Context context) throws IOException, InterruptedException
        {
            while (values.hasNext())
            {
                context.write(key, new IntWritable(values.next().get()));
            }



        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Longest Word");
        job.setJarByClass(LongestWord.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass (IntSumReducer. class );
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}