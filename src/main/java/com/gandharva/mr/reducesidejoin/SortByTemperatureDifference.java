package com.gandharva.mr.reducesidejoin;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class SortByTemperatureDifference {
    public static void main(String[] args) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get("/Users/gandharvadeshpande/Downloads/joinProject/InputOutputDir/minMaxtemperature"), Charset.forName("UTF-8"));
        List<String[]> states = new ArrayList<>();
        for (String line: lines) {
            line = line.replaceAll(" ", "");
            String diff = line.substring(18);
            String state = line.substring(0, 2);
            String[] stateData = new String[2];
            stateData[0] = state;
            stateData[1] = diff;
            states.add(stateData);
        }
        Collections.sort(states,new Comparator<String[]>() {
            public int compare(String[] strings, String[] otherStrings) {
                return Double.valueOf(strings[1]).compareTo(Double.valueOf(otherStrings[1]));
            }
        });
        List<String> finalSortedList = new ArrayList<>();
        for (int i = 0; i < states.size(); i++) {
            finalSortedList.add(states.get(i)[0] + " " + states.get(i)[1]);
        }
        Files.write(Paths.get("/Users/gandharvadeshpande/Downloads/joinProject/InputOutputDir/sortedStates"), finalSortedList, Charset.forName("UTF-8"));
    }
}
