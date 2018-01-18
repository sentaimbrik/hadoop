import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class BytesCount
{
    public static enum AGENTS {
        //Mozilla, Opera, Safari, Microsoft, Googlebot, Other
        MOZILLA, OPERA, SAFARI, IE, GOOGLE, OTHER
    }
    public static class BytesMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        private Pattern patternIP = Pattern.compile("^([A-Za-z]*)([0-9]*)");
        private Pattern patternBytes = Pattern.compile("([0-9]{1,}\\ \\\")|([0-9]{1,}\\ \\- \\\")");
        private Pattern patternDigits = Pattern.compile("([0-9]*)");
        private Pattern patternAgent = Pattern.compile("^[A-Za-z]+");

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

            String[] str = value.toString().split("\"");
            String[] agent = str[str.length - 1].split(" ");
            String ag = agent[0];
            Matcher matcherAgent = patternAgent.matcher(ag);

            if ( matcherAgent.find())
            {
                ag = matcherAgent.group(0);
               /* if (ag.equals("Mozilla"))
                {
                    context.getCounter(AGENTS.MOZILLA).increment(1);
                }
                else
                {
                    context.getCounter(AGENTS.OTHER).increment(1);
                }*/
            }
            context.write(new Text(ag + "" + matcherIP.group(1).toUpperCase() + IPstr), new IntWritable(Integer.parseInt(matcherDigits.group(0))));


        }
    }
/*
    public static class BytesCombiner extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int count = 0;
            int bytesSum = 0;
            for (IntWritable i : values)
            {
                bytesSum += Integer.parseInt(i.toString());
                count++;
            }
            context.write(new Text(key + ";" + count), new IntWritable(bytesSum));
        }
    }

    public static class BytesReducer extends Reducer<Text, IntWritable, Text, CustomData>
    {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {

            int bytes = 0;
            CustomData customData = new CustomData();
            String[] strs = key.toString().split(";");
            int count = Integer.parseInt(strs[strs.length - 1]);
            String newKey = key.toString().substring(0, key.toString().indexOf(";"));

            for (IntWritable i : values)
            {
                bytes +=Integer.parseInt(i.toString());
            }

            customData.setAvg(bytes / count);
            customData.setTotal(bytes);
            context.write(new Text(newKey), customData);
        }

    }*/

    static class CustomData
    {
        private int total;
        private int avg;

        public CustomData() {
        }

        public int getTotal() {
            return total;
        }

        public void setTotal(int total) {
            this.total = total;
        }

        public int getAvg() {
            return avg;
        }

        public void setAvg(int avg) {
            this.avg = avg;
        }

        @Override
        public String toString() {
            return avg + "," + total;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Count bytes by IP");
        job.setJarByClass(BytesCount.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(BytesMapper.class);
        //job.setCombinerClass(BytesCombiner.class);
        //job.setReducerClass (BytesReducer.class );
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CustomData.class);
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
       /* FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);*/
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
