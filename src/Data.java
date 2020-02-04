package com;

/*
 * NATIONAL TECHNICAL UNIVERSITY OF ATHENS
 * SCHOOL OF ELECTRICAL AND COMPUTER ENGINEERING
 * Distributed Systems Project
 * @author: Ntallas Ioannis, 03111418
 * @email: ynts@outlook.com
 */

public class Data {
    public int value;
    private String key;

    public Data(String k, int v){
        this.value = v;
        this.key = k;
    }

    public int getValue(){return this.value;}
    public String getKey(){return this.key;}
    public void setValue(int newValue){this.value = newValue;}
    public void setKey(String newKey){this.key = newKey;}
}
