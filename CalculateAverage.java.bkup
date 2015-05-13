package com.oliveirf.hadooptraining.com.oliveirf.hadooptraining;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CalculateAverage {

    public static class WikiMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text page = new Text();
        private double views;
        private double total_bytes;
        private double num_items;
        private double sum;
        private DoubleWritable resultado = new DoubleWritable();

        @Override
        public void setup(Context context) {
            num_items = 0;
            sum = 0;
        }
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] valor = value.toString().split(" ");
            page.set(valor[1]);
            views = Long.valueOf(valor[2]).longValue();
            total_bytes = Long.valueOf(valor[3]).longValue();
            sum += total_bytes / views;
            num_items += 1;
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            resultado.set(sum);
            context.write(new Text(Double.toString(num_items)), resultado);
        }
    }

    public static class CalculateAverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException,
                InterruptedException {
            double number_of_items = 0;
            double total = 0;
	    System.err.println("num of times inside reducer =");
            for (DoubleWritable val : values) {
                number_of_items += 1;
                total += val.get();
            }
            double value = Double.parseDouble(key.toString());
            double myitems=value*number_of_items;
            DoubleWritable outValue = new DoubleWritable(total / myitems);
            context.write(new Text("1"), outValue);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Path wikistats = new Path(otherArgs[0]);
        Path join_result = new Path(otherArgs[1]);

        Job job = Job.getInstance(conf);
        job.setJarByClass(CalculateAverage.class);
        job.setJobName("CalculateAverageInMapAggregation");

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setMapperClass(WikiMapper.class);
        job.setCombinerClass(CalculateAverageReducer.class);
        FileInputFormat.addInputPath(job, wikistats);

        job.setReducerClass(CalculateAverageReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
	//job.setNumReduceTasks(1);
        TextOutputFormat.setOutputPath(job, join_result);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
