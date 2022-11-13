package com.gandharva.mr.reducesidejoin;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CombineFiles {
    public static void combineAllTemperatureFiles(String path1, String path2, String path3, String path4, String outputFilePath) throws IOException {
        List<Path> paths = Arrays.asList(Paths.get(path1), Paths.get(path2), Paths.get(path3), Paths.get(path4));
        List<String> mergedLines = getMergedLines(paths);
        Path target = Paths.get(outputFilePath);
        Files.write(target, mergedLines, Charset.forName("UTF-8"));
    }
    private static List<String> getMergedLines(List<Path> paths) throws IOException {
        List<String> mergedLines = new ArrayList<>();
        for (Path p : paths){
            List<String> lines = Files.readAllLines(p, Charset.forName("UTF-8"));
            if (!lines.isEmpty()) {
                mergedLines.addAll(lines.subList(1, lines.size()));
            }
        }
        List<String> result = new ArrayList<>();
        for (String line: mergedLines) {
            if (!line.contains("STN---")) {
                result.add(line);
            }
        }
        return result;
    }

    public static void main(String[] args) throws IOException {
        String temperatureFilePath1 = "/Users/gandharvadeshpande/Downloads/joinProject/InputOutputDir/2006-resolved.csv";
        String temperatureFilePath2 = "/Users/gandharvadeshpande/Downloads/joinProject/InputOutputDir/2007-resolved.csv";
        String temperatureFilePath3 = "/Users/gandharvadeshpande/Downloads/joinProject/InputOutputDir/2008-resolved.csv";
        String temperatureFilePath4 = "/Users/gandharvadeshpande/Downloads/joinProject/InputOutputDir/2009-resolved.csv";
        String outPutFilePath = "/Users/gandharvadeshpande/Downloads/joinProject/InputOutputDir/temperature.csv";
        combineAllTemperatureFiles(temperatureFilePath1, temperatureFilePath2, temperatureFilePath3, temperatureFilePath4, outPutFilePath);
    }
}
