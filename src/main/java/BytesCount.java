import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BytesCount
{
    public static class BytesMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        private Pattern patternIP = Pattern.compile("^([A-Za-z]*)([0-9]*)");
        private Pattern patternBytes = Pattern.compile("([0-9]{1,}\\ \\\")|([0-9]{1,}\\ \\- \\\")");
        private Pattern patternDigits = Pattern.compile("([0-9]*)");

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            Matcher matcherIP = patternIP.matcher(value.toString());
            Matcher matcherBytes = patternBytes.matcher(value.toString());
            matcherIP.find();
            StringBuilder IPstr = new StringBuilder();
            if(matcherIP.group(2).length() == 1)
            {
                IPstr.append("00" + matcherIP.group(2));
            }
            else if(matcherIP.group(2).length() == 2)
            {
                IPstr.append("0" + matcherIP.group(2));
            }
            else
            {
                IPstr.append(matcherIP.group(2));
            }

            matcherBytes.find();
            Matcher matcherDigits = patternDigits.matcher(matcherBytes.group(0));
            matcherDigits.find();
            context.write(new Text(matcherIP.group(1).toUpperCase() + "," + IPstr), new IntWritable(Integer.parseInt(matcherDigits.group(0))));
        }
    }

    public static class BytesCombiner extends Reducer<Text, IntWritable, Text, NullWritable>
    {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int bytesSum = 0;
            for (IntWritable i : values)
            {
                bytesSum += Integer.parseInt(i.toString());
            }
            context.write(new Text(key.toString() + "," + bytesSum), NullWritable.get());
        }
    }

    /*public static class BytesReducer extends Reducer<Text, IntWritable, Text, Text>
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
        job.setCombinerClass(BytesCombiner.class);
        //job.setReducerClass (BytesReducer.class );
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
