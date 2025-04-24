/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package edu.neu.crimeanalysis;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author ingale.r
 */
public class CrimeTimeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable one = new IntWritable(1);
    private boolean skipHeader = true;
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
    private static final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("h:mm a");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",", -1);
        if (skipHeader) {
            skipHeader = false;
            return;
        }

        try {
            String dateStr = fields[2]; // date_of_occurrence
            String timeStr = fields[3]; // time_of_occurrence
            String part = fields[6];    // Part_1_2

            if (!"1".equals(part)) return;

            LocalDate date = LocalDate.parse(dateStr, dateFormatter);
            LocalTime time = LocalTime.parse(timeStr, timeFormatter);

            String hourKey = "Hour_" + time.getHour();
            String dayKey = "Day_" + date.getDayOfWeek().toString();
            String monthKey = "Month_" + date.getMonth().toString();

            context.write(new Text(hourKey), one);
            context.write(new Text(dayKey), one);
            context.write(new Text(monthKey), one);

        } catch (Exception e) {
           
        }
    }
}
