package com.sport;

import au.com.bytecode.opencsv.CSVReader;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author examin
 */
public class InsertGen {

    public static void main(String[] args) throws IOException, IllegalArgumentException, ParseException {
        String filePath = "/home/examin/Videos/final3.csv";
        String tableName = "FACT_CC_ACQUISITION";

        String schemaPath = "/home/examin/Videos/schema.sql";
        TreeSet<Integer>[] allStringIndex = getNumericIndex(schemaPath, tableName);
        TreeSet<Integer> IndexOfStringValues = allStringIndex[0];
        TreeSet<Integer> IndexOfDateValues = allStringIndex[1];

        String firstPartLine = "INSERT INTO " + tableName + " ";
        File file = createFilePath(filePath);
        CSVReader csvReader = new CSVReader(new FileReader(filePath), ',');


        String[] currentLine;
        int counter = 0;
        String headerToInsert = buildQueryHeader(csvReader);
        FileWriter writer = new FileWriter(file);
        BufferedWriter bf = new BufferedWriter(writer);
        while ((currentLine = csvReader.readNext()) != null) {
            String values;
            values = getCsvRowValues(currentLine, true, IndexOfStringValues, IndexOfDateValues);
                    values = firstPartLine + headerToInsert + " VALUES " + values;

            bf.write(values);

            System.out.print("rows processed: " + (++counter) + "\r");
        }
        bf.close();
        replaceLastChar(file.getName());

    }


    private static TreeSet<Integer>[] getNumericIndex(String schemaPath, String tableName) throws IllegalArgumentException {
        TreeSet<Integer> setOfVarcharIndex = new TreeSet<Integer>();
        TreeSet<Integer> setOfDateIndex = new TreeSet<Integer>();
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
                    line = line.trim();
                    String[] words = line.split("\\s+");
                    if (words.length > 2) {
                        if (words[1].contains("date") || words[1].contains("DATE")) {
                            System.out.println(lineCount + " " + line);
                            setOfDateIndex.add(lineCount);
                        } else {
                            if (words[1].contains("varchar") || words[1].contains("VARCHAR")) {
                                System.out.println(lineCount + " " + line);
                                setOfVarcharIndex.add(lineCount);
                            }
                        }
                        lineCount++;
                    }
                }
            }
            bufr.close();
        } catch (IOException io) {
            System.exit(0);
        }
        TreeSet<Integer>[] tor = new TreeSet[]{setOfVarcharIndex, setOfDateIndex};
        return tor;
    }

    private static File createFilePath(String filePath) throws IOException {
        String[] splitedPath = filePath.split("/");
        String[] completeName = (splitedPath[splitedPath.length - 1]).split("\\.");
        File file = new File(completeName[0] + ".sql");
        if (!file.exists()) {
            file.createNewFile();
        }
        return file;
    }

    private static String buildQueryHeader(CSVReader reader) throws IOException {
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

    private static String getCsvRowValues(String[] currentLine, boolean isEnd, Set<Integer> numericValueIndex, Set<Integer> dateValueIndex) throws ParseException {
        String values = "(";
        int currIndex = 0;
        DateFormat srcDf = new SimpleDateFormat("dd-MM-yyyy");
        DateFormat desDf = new SimpleDateFormat("yyyy-MM-dd");
        for (String element : currentLine) {
            String replacedElement = element.replaceAll("[\n\r]", "");
            replacedElement = replacedElement.replaceAll("'", "\\\\'");
            if (numericValueIndex.contains(currIndex)) {
                values += ("'" + replacedElement + "',");

            } else if (dateValueIndex.contains(currIndex)) {
                Date sourceDf = srcDf.parse(replacedElement);
                values += ("'" + desDf.format(sourceDf) + "',");
            } else {
                values += (replacedElement + ",");
            }
            currIndex++;
        }
        values = values.substring(0, values.length() - 1);
        if (isEnd) {
            values += ");\n";
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
