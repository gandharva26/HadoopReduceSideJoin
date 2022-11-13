package com.nbisht.mr.reducesidejoin;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author nbisht7.
 * Purpose - States Mapper to fetch the data from intermediate directory in hadoop.
 * Input Schema - state, salesPrice
 * Output Schema - state, salesPrice
 * sales - temperature
 */

public class StateTemperatureMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();
        data = data.replaceAll(" ", "");
        String state = data.substring(0, 2);
        String[] temperatureAndMonth = data.substring(2).split(":");
        String params = state + ":" + temperatureAndMonth[1];
        DoubleWritable temperature = new DoubleWritable(Double.parseDouble(temperatureAndMonth[0]));
        Text stateName = new Text(params);
        if (!(stateName.toString().isEmpty()) && !(temperature == null))
            context.write(stateName, temperature);
    }
}