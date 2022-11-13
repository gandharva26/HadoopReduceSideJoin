package com.nbisht.mr.reducesidejoin;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author nbisht7.
 * Purpose - Sales Mapper to fetch only customer ID and sales price information.
 * Input Schema - timestamp, customerId,salesPrice
 * Output Schema - customerId, salesPrice.
 * sales is temperature
 */

public class TemperatureMapper extends Mapper<Object, Text, IntWritable, Text> {

    private static final String fileTag = "temp~";
    // 2006 - temp.csv

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String inputData = value.toString();
        String salesData[] = inputData.split(",");
        if (salesData.length == 4 && !salesData[0].equals("") &&!salesData[2].equals("") && !salesData[3].equals("")) {
            IntWritable customerId = new IntWritable(Integer.parseInt(salesData[0])); // ID
            Text month = new Text(salesData[2].substring(4, 6)); // get month from the date string
            Text temperature = new Text(salesData[3]); // temperature

            // Tagging the Mapper output to show that it is coming from Sales file
            Text salesPrice = new Text(fileTag + temperature); // format temp~ temperature
            String temp = salesPrice + ":" + month; // temp~temperature,month
            Text valueOutData = new Text(temp);
            // Validating the data before writing
            if (!(customerId.toString().isEmpty()) && !(salesPrice.toString().isEmpty()) && !(month.toString().isEmpty())) {
                context.write(customerId, valueOutData);
            }
        }
    }
}