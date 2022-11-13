package com.nbisht.mr.reducesidejoin;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author nbisht7.
 * Purpose - Customer Mapper to fetch only customer ID and state information.
 * Input Schema - customerId, name, street, city, state, zipCode.
 * Output Schema - customerId, state.
 *
 * customer - weatherstation
 * sales - 2006.txt
 * customerid - primary key - USF/station
 */

public class WeatherMapper extends Mapper<Object, Text, IntWritable, Text> {

    private static final String fileTag = "weather~";

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String inputData = value.toString();
        String customerData[] = inputData.split(",");
        if (customerData.length == 10 && !customerData[0].equals("") && customerData[3].equals("US") && !customerData[4].equals("")) {
            IntWritable customerId = new IntWritable(Integer.parseInt(customerData[0]));
            Text state = new Text(customerData[4]);
            // Tagging the Mapper output to show that it is coming from Customer file
            Text stateName = new Text(fileTag + state); // format - weather~state

            // Validating the data before writing
            if (!(customerId.toString().isEmpty()) && !(stateName.toString().isEmpty())) {
                context.write(customerId, stateName);
            }
        }
    }
}