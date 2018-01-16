
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

    public static class IntSumReducer extends Reducer<IntWritable, Text, Text, IntWritable>
    {
        private Map<Text, Integer> count = new HashMap<Text, Integer>();
        private int max = 0;
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            StringBuilder sb = new StringBuilder();
            for (Text v : values)
            {
                sb.append(v + ";");
               // count.put(v, Integer.parseInt(key.toString()));
            }
            Text txt = new Text();
            txt.set(sb.toString());
            context.write(txt, key);
            //if (Integer.parseInt(max.toString()) < Integer.parseInt(key.toString())) max = key;
        }

        /*@Override
        public void cleanup(Context context) throws IOException, InterruptedException
        {
            Set<Text> set = count.keySet();

            for (Text t : set)
            {
                if (t.toString().length() > max) max = t.toString().length();
            }
            for (Map.Entry<Text, Integer> e : count.entrySet())
            {

                    context.write(e.getKey(), new IntWritable(max));

            }

        }*/
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Longest Word");
        job.setJarByClass(LongestWord.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
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