package com.sport;

import au.com.bytecode.opencsv.CSVReader;

import java.io.*;

/**
 * @author examin
 */
public class InsertGen {

    public static void main(String[] args) {
        String filePath = "/home/examin/cc_acq_monthly_20181031_done.txt";
        String tableName = "FACT_CC_ACQUISITION";
        final int groupedQuery = 5;
        String firstPartLine = "INSERT INTO `" + tableName + "` ";

        try {
            File file = createFile(filePath);
            CSVReader reader = new CSVReader(new FileReader(filePath), '|');
            String[] currentLine;
            int counter = 0;
            String headerToInsert = buildTableHeader(reader);
            FileWriter writer = new FileWriter(file);
            BufferedWriter bf = new BufferedWriter(writer);
            int i = 10;
            while ( i > 0 && (currentLine = reader.readNext()) != null){
                i--;
                currentLine = reader.readNext();
                String values;
                switch (counter % groupedQuery) {
                    case 0:
                        values = buildValues(currentLine, true);
                        values = firstPartLine + headerToInsert + " VALUES " + values;
                        break;
                    case groupedQuery - 1:
                        values = buildValues(currentLine, true);
                        break;
                    default:
                        values = buildValues(currentLine, false);
                }
                bf.write(values);

                System.out.println(values);
            }
            bf.close();
            System.out.println();
            replaceLastChar(file.getName());
        } catch (Exception e) {
            e.printStackTrace();

        }

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
                headerToInsert += ("`" + title + "`,");
            }
            headerToInsert = headerToInsert.substring(0, headerToInsert.length() - 1);
            headerToInsert += ") ";
        }
        return headerToInsert;
    }

    private static String buildValues(String[] currentLine, boolean isEnd) {
        String values = "(";

        for (String element : currentLine) {
            String replacedElement = element.replaceAll("[\n\r]", "");
            replacedElement = replacedElement.replaceAll("'", "\\\\'");
            values += ("'" + replacedElement + "',");
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
