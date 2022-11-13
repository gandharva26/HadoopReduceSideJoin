package com.gandharva.mr.reducesidejoin;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MinMaxCalculator extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();
       //state month average-temperature
        data = data.replaceAll(" ", "");

        String state = data.substring(0, 2);
        String month = data.substring(2, 5);
        String averageTemperature = data.substring(5);

        context.write(new Text(state), new Text(month + ":" + averageTemperature));

    }
}
