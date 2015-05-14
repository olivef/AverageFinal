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
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Writable;

public class CalculateAverageC {

    public static class WikiMapper extends Mapper<LongWritable, Text, Text, OutTuple> {
        private Text page = new Text();
        private double views;
        private double total_bytes;
        private double num_items;
        private double sum;
        private OutTuple outTuple = new OutTuple();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] valor = value.toString().split(" ");
            page.set(valor[1]);
            views = Long.valueOf(valor[2]).longValue();
            total_bytes = Long.valueOf(valor[3]).longValue();
            sum = total_bytes / views;
            num_items = 1;
            outTuple.setSum(sum);
            outTuple.setTotal(1);
            context.write(new Text("Average"), outTuple);
        }
    }

    public static class CalculateAverageReducer extends Reducer<Text, OutTuple, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<OutTuple> values, Context context) throws IOException,
                InterruptedException {
            double sum = 0;
            double total = 0;
            double num_items = 0;
            for (OutTuple val : values) {
                sum += val.getSum();
                total += val.getTotal();
                num_items += 1;
            }
            DoubleWritable outValue = new DoubleWritable(sum / num_items);
            context.write(new Text("1"), outValue);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Path wikistats = new Path(otherArgs[0]);
        Path join_result = new Path(otherArgs[1]);

        Job job = Job.getInstance(conf);
        job.setJarByClass(CalculateAverageC.class);
        job.setJobName("CalculateAverageInMapAggregation");

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OutTuple.class);

        job.setMapperClass(WikiMapper.class);
        FileInputFormat.addInputPath(job, wikistats);

        job.setReducerClass(CalculateAverageReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        TextOutputFormat.setOutputPath(job, join_result);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class OutTuple implements Writable {
        private double sum;
        private double total;

        public double getSum() {
            return sum;
        }

        public void setSum(double s) {
            this.sum = s;
        }

        public double getTotal() {
            return total;
        }

        public void setTotal(double t) {
            this.total = t;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            sum = in.readDouble();
            total = in.readDouble();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeDouble(sum);
            out.writeDouble(total);
        }

    }

}

