
import java.io.IOException;
import java.util.*;

import org.apache.commons.collections.map.LinkedMap;
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
    public static int max = 0;
    public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, Text>
    {
        private Text word = new Text();

        @Override
        public void map(Object key, Text value,  Context context) throws IOException, InterruptedException
        {
            String str = value.toString().replaceAll("\\n", " ");
            StringTokenizer s = new StringTokenizer(str);

            while (s.hasMoreTokens())
            {
                word.set(s.nextToken());
                context.write(new IntWritable(word.getLength()), word);
            }
        }
    }

    public static class Combiner extends Reducer<IntWritable, Text, Text, IntWritable>
    {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            if (Integer.parseInt(key.toString()) > max) max = Integer.parseInt(key.toString());
            StringBuilder sb = new StringBuilder();
            for (Text v : values) {
                sb.append(v + ";");
            }
            Text txt = new Text();
            txt.set(sb.toString());
            context.write(txt, key);
        }
    }

    public static class IntSumReducer extends Reducer<IntWritable, Text, Text, IntWritable>
    {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
                if (Integer.parseInt(key.toString()) == max)
                {
                    for (Text t : values)
                    {
                        context.write(t, key);
                    }
                }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Longest Word");
        job.setJarByClass(LongestWord.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass (IntSumReducer.class );
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}