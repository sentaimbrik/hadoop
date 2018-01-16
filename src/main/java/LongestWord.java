
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LongestWord
{

    public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, Text>
    {
        private Text word = new Text();

        public void map(Object key, Text value,  Context context) throws IOException, InterruptedException
        {
            StringTokenizer s = new StringTokenizer(value.toString());
            while (s.hasMoreTokens())
            {
                word.set(s.nextToken());
                context.write(new IntWritable(word.getLength()), word);
            }
        }
    }

    public static class IntSumReducer extends Reducer<IntWritable, Text, Text, IntWritable>
    {
        private Map<IntWritable, Text> count = new HashMap<IntWritable, Text>();
        private int max = 0;

        public void reduce(IntWritable key, Iterator<Text> values, Context context) throws IOException, InterruptedException
        {
            while (values.hasNext())
            {
                count.put(key, values.next());
                if (Integer.parseInt(key.toString()) > max)
                {
                    max = Integer.parseInt(key.toString());
                }
            }
        }

        @Override
        protected void cleanup (Context context) throws IOException, InterruptedException
        {
            for(IntWritable k : count.keySet())
            {
                if (Integer.parseInt(k.toString()) == max)
                {
                    context.write(count.get(k), k);
                }
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
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}