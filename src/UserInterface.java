package com;

/*
 * NATIONAL TECHNICAL UNIVERSITY OF ATHENS
 * SCHOOL OF ELECTRICAL AND COMPUTER ENGINEERING
 * Distributed Systems Project
 * @author: Ntallas Ioannis, 03111418
 * @email: ynts@outlook.com
 */

public class UserInterface extends Thread {
    private String[] userInput;
    private Program mainProgram;

    public UserInterface(String[] s, Program p) {
       this.userInput = s;
       this.mainProgram = p;
    }

    public void run() {
        try{
            switch (userInput[0]){
                case "ADD":
                    this.mainProgram.addNode(Integer.parseInt(userInput[1]));
                    break;
                case "REMOVE":
                    this.mainProgram.removeNode(userInput[1]);
                    break;
                case "KILL":
                    this.mainProgram.terminate();
                    break;
                case "DUMP":
                    this.mainProgram.dumpNetwork();
                    break;
                case "INSERT":
                    this.mainProgram.insertData(userInput[1], Integer.parseInt(userInput[2]));
                    break;
                case "PRINT":
                    this.mainProgram.printData();
                    break;
                case "DELETE":
                    this.mainProgram.deleteData(userInput[1], Integer.parseInt(userInput[2]));
                    break;
                case "QUERY":
                    this.mainProgram.query(userInput[1]);
                    break;
                default:
                    break;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
