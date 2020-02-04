package com;

/*
 * NATIONAL TECHNICAL UNIVERSITY OF ATHENS
 * SCHOOL OF ELECTRICAL AND COMPUTER ENGINEERING
 * Distributed Systems Project
 * @author: Ntallas Ioannis, 03111418
 * @email: ynts@outlook.com
 */

public class Tuple {
    private String command;
    private String key;
    private int value;

    public Tuple(String c, String k, int v){
        this.command = c;
        this.key = k;
        this.value = v;
    }

    public String getCommand(){
        return this.command;
    }
    public String getKey(){
        return this.key;
    }
    public int getValue(){
        return this.value;
    }
    public void setValue(int v){
        this.value = v;
    }
}
