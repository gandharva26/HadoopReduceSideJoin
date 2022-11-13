package com.nbisht.mr.reducesidejoin;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author nbisht7.
 * Purpose - Joining customers and sales files based on CustomerID
 * Input Schema - Output from Customer Mapper : CustomerID, state and
 *                Output from Sales Mapper : CustomerID, salesPrice.
 * Output Schema - state, salesPrice without aggregation.
 */

public class TemperatureWeatherJoinReducer extends Reducer<IntWritable, Text, Text, Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String stateName = "";
        String tempAndMonth = "";

       for (Text val : values) {
            String data[] = val.toString().split("~");
            if (data[0].equals("weather")) {
              stateName = data[1];
            } else if (data[0].equals("temp")) {
              //  salesData = Double.parseDouble(data[1]);
                tempAndMonth = data[1];
            }
        }


       // DoubleWritable val = new DoubleWritable(Double.valueOf(tempAndMonth));
        Text val = new Text(tempAndMonth);
        // Validating the data before writing
        if (!(stateName.toString().isEmpty()) && !(tempAndMonth.equals("")))
            context.write(new Text(stateName), val);
    }
}