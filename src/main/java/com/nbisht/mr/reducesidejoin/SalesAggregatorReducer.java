package com.nbisht.mr.reducesidejoin;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author nbisht7.
 * Purpose - Final reducer to calculate aggregate sales price for each state.
 * Input Schema - state, salesPrice
 * Output Schema - state, totalSalesPrice
 */

public class SalesAggregatorReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double totalSalesPrice = 0.0;
        int n = 0;
        for (DoubleWritable val : values) {
            totalSalesPrice = totalSalesPrice + Double.parseDouble(val.toString());
            n ++;
        }
        double average = totalSalesPrice/ n;
        DoubleWritable finalAverage = new DoubleWritable(average);
        HashMap<String, String> months = new HashMap<>();
        months.put("01", "JAN");
        months.put("02", "FEB");
        months.put("03", "MAR");
        months.put("04", "APR");
        months.put("05", "MAY");
        months.put("06", "JUN");
        months.put("07", "JUL");
        months.put("08", "AUG");
        months.put("09", "SEP");
        months.put("10", "OCT");
        months.put("11", "NOV");
        months.put("12", "DEC");
        String[] stateAndMonthArray = key.toString().split(":");

        String state = stateAndMonthArray[0];
        String month = months.get(stateAndMonthArray[1]);
        String keyForResults = state + " " + month;
        // Validating the data before writing
        if (!(key.toString().isEmpty()) && !(finalAverage == null))
            context.write(new Text(keyForResults), finalAverage);
    }
}