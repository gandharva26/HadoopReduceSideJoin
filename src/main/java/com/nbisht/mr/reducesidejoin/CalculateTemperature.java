package com.nbisht.mr.reducesidejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author nbisht7.
 *         Purpose - Java MR reduce-side join driver code to find total sales by states.
 */

public class CalculateTemperature {

    public static void main(String[] args) throws Exception {

       Configuration config = new Configuration();

        // Configuration settings for joining customers and sales files
        Job mr1 = new Job(config, "calculate temperature");

        mr1.setJarByClass(CalculateTemperature.class);
        mr1.setReducerClass(TemperatureWeatherJoinReducer.class);
        mr1.setOutputKeyClass(IntWritable.class);
        mr1.setOutputValueClass(Text.class);

        String weatherFilePath = args[0];
        String temperatureFilePath = args[1];

        MultipleInputs.addInputPath(mr1, new Path(weatherFilePath), TextInputFormat.class, WeatherMapper.class);
        MultipleInputs.addInputPath(mr1, new Path(temperatureFilePath), TextInputFormat.class, TemperatureMapper.class);

        // Specifying temporary directory name for storing mapreduce 1's output
       final String TMP_DIR = "/output1/";

        Path res = new Path(TMP_DIR);
        FileOutputFormat.setOutputPath(mr1, res);


     int flag = 1;

        if (mr1.waitForCompletion(true)) {

            // Configuration settings for aggregating sales by state
          Job mr2 = new Job(config, "Sales aggregation by State");
            mr2.setJarByClass(CalculateTemperature.class);
            mr2.setMapperClass(StateTemperatureMapper.class);
            mr2.setReducerClass(SalesAggregatorReducer.class);
            mr2.setOutputKeyClass(Text.class);
            mr2.setOutputValueClass(DoubleWritable.class);

            TextInputFormat.addInputPath(mr2, new Path(TMP_DIR));

            String finalFilePath = args[2];
            Path finalOutputPath = new Path(finalFilePath);
            FileOutputFormat.setOutputPath(mr2, finalOutputPath);

           if (mr2.waitForCompletion(true)) {
              //  res.getFileSystem(config).deleteOnExit(res);
                flag = 0;
            }
            System.exit(flag);
        }
    }
}