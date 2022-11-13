package com.gandharva.mr.reducesidejoin;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class MinMaxReducer  extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String max = "";
        double maxTemp = 0.0;
        String min = "";
        double minTemp = 0.0;
        for (Text value: values) {
            String[] monthAndTemp = value.toString().split(":");
            double temp = Double.valueOf(monthAndTemp[1]);
            if (temp > maxTemp || max.equals("")) {
                max = monthAndTemp[0];
                maxTemp = temp;
            }
            if (temp < minTemp || min.equals("")) {
                min = monthAndTemp[0];
                minTemp = temp;
            }
         }
        context.write(key, new Text(min + " " + minTemp + " " + max + " " + maxTemp));
    }
}
