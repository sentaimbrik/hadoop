import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
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

public class BytesCount
{
    public static class BytesMapper extends Mapper<Object, Text, Text, Text>
    {
        private Pattern patternIP = Pattern.compile("^[A-Za-z]*[0-9]*");
        private Pattern patternBytes = Pattern.compile("(200).([0-9]*)");

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            Matcher matcherIP = patternIP.matcher(value.toString());
            Matcher matcherBytes = patternBytes.matcher(value.toString());
            matcherIP.find();
            //matcherBytes.find();

            context.write(new Text(matcherIP.group(0)), new Text("1"));
        }
    }

    /*public static class BytesReducer extends Reducer<Text, Text, Text, Text>
    {

        private Text txt = new Text();
        private IntWritable wordLength = new IntWritable();
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            StringBuilder sb = new StringBuilder();
            for (Text v : values)
            {
                sb.append(v + ";");
            }
            txt.set(sb.toString());
            wordLength = key;
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException
        {
            context.write(txt, wordLength);
        }
    }*/

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Count bytes by IP");
        job.setJarByClass(BytesCount.class);
        job.setMapperClass(BytesMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        //job.setReducerClass (BytesReducer.class );
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
