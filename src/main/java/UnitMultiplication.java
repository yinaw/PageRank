import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UnitMultiplication {

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] fromTo = line.split("\t");

            //a	b,c,d

            if(fromTo.length == 1 || fromTo[1].trim().equals("")) {
                return;
            }
            String from = fromTo[0];
            String[] tos = fromTo[1].split(",");
            for (String to: tos) {
                context.write(new Text(from), new Text(to + "=" + (double)1/tos.length));
                // a    b=1/3      a   c=1/3     a   d=1/3
                // b    a=1/2      b   d=1/2
                // c    a=1
                // d    b=1/2      d   c=1/2
            }
        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] pr = value.toString().trim().split("\t");

            String beta = String.valueOf(Double.parseDouble(pr[1]) * (1 - 1/5));  // pr * (1 - b)  b = 1/5 here.
            context.write(new Text(pr[0]), new Text(beta));

            //a  1/4
            //b   1/4
            //c  1/4
            //d   1/4
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> transitionUnit = new ArrayList<String>();
            double prUnit = 0;
            for (Text value: values) {
                if(value.toString().contains("=")) {
                    transitionUnit.add(value.toString());
                }
                else {
                    prUnit = Double.parseDouble(value.toString());
                }
            }
            for (String unit: transitionUnit) {
                String outputKey = unit.split("=")[0];
                double relation = Double.parseDouble(unit.split("=")[1]);
                String outputValue = String.valueOf(relation * prUnit);
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, PRMapper.class, Object.class, Text.class, Text.class, Text.class, conf);

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
