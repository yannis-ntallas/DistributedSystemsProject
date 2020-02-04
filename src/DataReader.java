package com;

/*
 * NATIONAL TECHNICAL UNIVERSITY OF ATHENS
 * SCHOOL OF ELECTRICAL AND COMPUTER ENGINEERING
 * Distributed Systems Project
 * @author: Ntallas Ioannis, 03111418
 * @email: ynts@outlook.com
 */

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class DataReader {

    public ArrayList<Data> readFile(String path) {

        ArrayList<Data> arr = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String sCurrentLine;

            while ((sCurrentLine = br.readLine()) != null) {
                String[] splitInput = sCurrentLine.split(", ");
                Data d = new Data(splitInput[0], Integer.parseInt(splitInput[1]));
                arr.add(d);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return arr;
    }
}
