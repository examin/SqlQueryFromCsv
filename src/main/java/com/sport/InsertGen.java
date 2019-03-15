package com.sport;

import au.com.bytecode.opencsv.CSVReader;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

/**
 * @author examin
 */
public class InsertGen {

    public static void main(String[] args) throws IOException, IllegalAccessException {
        String filePath = "/home/examin/Videos/final2.csv";
        String tableName = "FACT_CC_ACQUISITION";

        String schemaPath = "/home/examin/Videos/schema.sql";
        Set<Integer> IndexOfNumericValues = getNumericIndex(schemaPath, tableName);


        final int groupedQuery = 5;


        String firstPartLine = "INSERT INTO " + tableName + " ";
        File file = createFile(filePath);
        CSVReader csvReader = new CSVReader(new FileReader(filePath), ',');


        String[] currentLine;
        int counter = 0;
        String headerToInsert = buildTableHeader(csvReader);
        FileWriter writer = new FileWriter(file);
        BufferedWriter bf = new BufferedWriter(writer);

        while ((currentLine = csvReader.readNext()) != null) {
            String values;
            switch (counter % groupedQuery) {
                case 0:
                    values = buildValues(currentLine, true, IndexOfNumericValues);
                    values = firstPartLine + headerToInsert + " VALUES " + values;
                    break;
                case groupedQuery - 1:
                    values = buildValues(currentLine, true, IndexOfNumericValues);
                    break;
                default:
                    values = buildValues(currentLine, false, IndexOfNumericValues);
            }
            bf.write(values);

            System.out.print("rows processed: " + (++counter) + "\r");
        }
        bf.close();
        replaceLastChar(file.getName());

    }

    private static Set<Integer> getNumericIndex(String schemaPath, String tableName) throws IllegalAccessException {
        Set<Integer> setOfIndex = new HashSet<Integer>();
        try {
            FileReader fr = new FileReader(schemaPath);
            BufferedReader bufr = new BufferedReader(fr);
            String line;
            Boolean isRight = false;
            int lineCount = 0;
            while ((line = bufr.readLine()) != null) {
                if (line.matches("CREATE.*" + tableName + ".*")) {
                    isRight = true;
                    break;
                }

            }
            if (isRight == true) {
                while ((line = bufr.readLine()) != null) {
                    System.out.println(line);
                    line = line.trim();
                    String[] words = line.split("\\s+");
                    if (words.length > 2) {
                        lineCount++;
                        if (words[1].contains("varchar") || words[1].contains("VARCHAR"))
                            setOfIndex.add(lineCount);
                    }
                }
            } else {
                throw new IllegalAccessException();
            }
            bufr.close();
        } catch (IOException io) {
            System.exit(0);
        }
        return setOfIndex;
    }

    private static File createFile(String filePath) throws IOException {
        String[] splitedPath = filePath.split("/");
        String[] completeName = (splitedPath[splitedPath.length - 1]).split("\\.");
        File file = new File(completeName[0] + ".sql");
        if (!file.exists()) {
            file.createNewFile();
        }
        return file;
    }

    private static String buildTableHeader(CSVReader reader) throws IOException {
        String headerToInsert = "(";
        String[] header;

        if ((header = reader.readNext()) != null) {
            for (String title : header) {
                headerToInsert += (title + ",");
            }
            headerToInsert = headerToInsert.substring(0, headerToInsert.length() - 1);
            headerToInsert += ") ";
        }
        return headerToInsert;
    }

    private static String buildValues(String[] currentLine, boolean isEnd, Set<Integer> numericValueIndex) {
        String values = "(";
        int currIndex = 0;
        for (String element : currentLine) {
            String replacedElement = element.replaceAll("[\n\r]", "");
            replacedElement = replacedElement.replaceAll("'", "\\\\'");
            if (!numericValueIndex.contains(currIndex)) {
                values += (replacedElement + ",");
            } else {
                values += ("'" + replacedElement + "',");
            }
            currIndex++;
        }
        values = values.substring(0, values.length() - 1);
        if (isEnd) {
            values += ");";
        } else {
            values += "),";
        }
        return values;
    }

    private static void replaceLastChar(String filePath) throws IOException {

        RandomAccessFile file = new RandomAccessFile(filePath, "rw");
        file.seek(file.length() - 1);
        byte[] bytes = new byte[1];
        file.read(bytes);
        String lastChar = new String(bytes);
        if (lastChar.equals(",")) {
            file.seek(file.length() - 1);
            file.write(";".getBytes());
        }
        file.close();
    }
}
