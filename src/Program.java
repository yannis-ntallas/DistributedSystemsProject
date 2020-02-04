package com;

/*
 * NATIONAL TECHNICAL UNIVERSITY OF ATHENS
 * SCHOOL OF ELECTRICAL AND COMPUTER ENGINEERING
 * Distributed Systems Project
 * @author: Ntallas Ioannis, 03111418
 * @email: ynts@outlook.com
 */

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Program {
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLACK = "\u001B[30m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_WHITE = "\u001B[37m";

    public static final int INIT_SOCKET = 40000;
    public static final int NUMBER_OF_NODES = 10;
    public static final long SECONDS = 2;
    public static final String CONSISTENCY = "eventual";
    public static final int REPLICATION_FACTOR = 5;

    private int socket;
    private int leadNodeSocket;
    private ArrayList<ProcessSignature<Process, Integer, Integer>> network;
    private ArrayList<Process> procList;

    /* Main program constructor initializes sockets and list. */
    public Program(){
        socket = INIT_SOCKET;
        leadNodeSocket = INIT_SOCKET + 1;
        network = new ArrayList<>();
        procList = new ArrayList<>();
    }


    public static void main(String args[]) throws IOException, InterruptedException {
        System.out.print("Inserting leader node... ");
        /* Create instance of the main class of the program. */
        Program prog = new Program();

        /* Create and add leader node first. Leader node doesn't need to be placed somewhere scpecific. */
        String java = System.getProperty("java.home")+"/bin/java";
        ProcessBuilder pb = new ProcessBuilder(java,"-cp","/home/yandall/src/DistrSysProject/out/production/DistrSysProject","com.Node",
                ""+prog.leadNodeSocket, "1", ""+prog.leadNodeSocket, ""+prog.socket, CONSISTENCY, ""+REPLICATION_FACTOR);

        /* Inherit the IO of the process and start it. */
        pb.inheritIO();
        Process p = pb.start();

        /* Add it to the network */
        prog.procList.add(p);
        prog.network.add(new ProcessSignature<>(p, 1, prog.leadNodeSocket));
        TimeUnit.SECONDS.sleep(SECONDS);    //Wait because fuck.

        System.out.println("Done!");

        /* Repeat for the rest of the nodes. */
        for (int i = 2; i <= NUMBER_OF_NODES; i++){
            prog.addNode(i);
        }

        System.out.print("TIME: ");
        System.out.println(System.currentTimeMillis());

        DataReader dr = new DataReader();
        ArrayList<Data> data = dr.readFile("/home/yandall/src/DistrSysProject/data/insert.txt");

        for(Data d : data){
            prog.insertData(d.getKey(), d.getValue());
            TimeUnit.MILLISECONDS.sleep(100);
        }

        System.out.print("TIME: ");
        System.out.println(System.currentTimeMillis());

        ArrayList<String> queries = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader("/home/yandall/src/DistrSysProject/data/query.txt"))) {
            String sCurrentLine;

            while ((sCurrentLine = br.readLine()) != null) {
                queries.add(sCurrentLine);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (String q : queries){
            prog.query(q);
            TimeUnit.MILLISECONDS.sleep(100);
        }

        System.out.print("TIME: ");
        System.out.println(System.currentTimeMillis());

        ArrayList<Tuple> requests = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader("/home/yandall/src/DistrSysProject/data/requests.txt"))) {
            String sCurrentLine;

            while ((sCurrentLine = br.readLine()) != null) {
                System.out.println(sCurrentLine);
                String[] splitCommand = sCurrentLine.split(", ");
                Tuple t = new Tuple(splitCommand[0], splitCommand[1], 0);
                int l = splitCommand.length;
                if (l > 2) {
                    t.setValue(Integer.parseInt(splitCommand[2]));
                }
                requests.add(t);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (Tuple r : requests){
            if (r.getCommand().equals("insert")){
                prog.insertData(r.getKey(), r.getValue());
                //TimeUnit.MILLISECONDS.sleep(100);
            }
            else{
                prog.query(r.getKey());
                //TimeUnit.MILLISECONDS.sleep(100);
            }
        }

        System.out.print("TIME: ");
        System.out.println(System.currentTimeMillis());

        /* Splash. */
        System.out.println(ANSI_PURPLE + "______ _     _       ______          _           _   " + ANSI_RESET);
        System.out.println(ANSI_PURPLE + "|  _  (_)   | |      | ___ \\        (_)         | |  " + ANSI_RESET);
        System.out.println(ANSI_PURPLE + "| | | |_ ___| |_ _ __| |_/ / __ ___  _  ___  ___| |_ " + ANSI_RESET);
        System.out.println(ANSI_PURPLE + "| | | | / __| __| '__|  __/ '__/ _ \\| |/ _ \\/ __| __|" + ANSI_RESET);
        System.out.println(ANSI_YELLOW + "| |/ /| \\__ \\ |_| |  | |  | | | (_) | |  __/ (__| |_ " + ANSI_RESET);
        System.out.println(ANSI_YELLOW + "|___/ |_|___/\\__|_|  \\_|  |_|  \\___/| |\\___|\\___|\\__|" + ANSI_RESET);
        System.out.println(ANSI_YELLOW + "                                   _/ |              " + ANSI_RESET);
        System.out.println(ANSI_YELLOW + "                                  |__/     ..::: 2017 :::..          " + ANSI_RESET);

        System.out.println(" ");
        System.out.println("System ready! Please enter a command... ");

        /* Wait for user input. */
        prog.listen();
        System.out.println("Error at main.");
    }


    /* BASIC NODE FUNCTIONS */
    /******************************************************************************************************************/

    /* Adds a node to the network. Called during initialization or by the user. */
    public void addNode(int nodeId) throws IOException, InterruptedException{
        System.out.print("Inserting new node... ");

        int newSocket = this.socket + nodeId;

        /* Create the process. */
        String java = System.getProperty("java.home")+"/bin/java";
        ProcessBuilder pb = new ProcessBuilder(java,"-cp","/home/yandall/src/DistrSysProject/out/production/DistrSysProject","com.Node",
                ""+newSocket, ""+nodeId, ""+this.leadNodeSocket, ""+this.socket, CONSISTENCY, ""+REPLICATION_FACTOR);
        pb.inheritIO();
        Process p = pb.start();

        /* Add it to the network */
        this.procList.add(p);
        this.network.add(new ProcessSignature<>(p, nodeId, newSocket));
        TimeUnit.SECONDS.sleep(SECONDS);

        /* Create a socket, connect to the leader node and request to join the network. */
        Socket sock = new Socket("localhost", this.leadNodeSocket);
        PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
        String responseMSG;
        out.println("JOIN" + ":" + ""+newSocket + ":" + ""+nodeId);

        /* Wait for response. */
        while ((responseMSG = in.readLine()) != null)
        {
            /* Leader approves request. */
            if (responseMSG.equals("DONE")){ break; }
        }
        sock.close();
        this.waitResponse();
        deleteAllReplicas();
        recreateAllReplicas();
        System.out.println("Done!");
    }


    public void removeNode(String removeId) throws IOException{
        System.out.print("Node departing... ");

        /* Create a socket, connect to the leader node and request to join the network. */
        Socket sock = new Socket("localhost", this.leadNodeSocket);
        PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
        String responseMSG;
        out.println("LEAVE" + ":" + removeId);

        /* Wait for response. */
        while ((responseMSG = in.readLine()) != null)
        {
            /* Leader approves request. */
            if (responseMSG.equals("DONE")){ break; }
        }

        sock.close();
        this.waitResponse();
        deleteAllReplicas();
        recreateAllReplicas();

        /* Nodes have been updated. Now we can kill the process. */
        boolean flag = true;
        ListIterator<ProcessSignature<Process, Integer, Integer>> it = this.network.listIterator();
        while (it.hasNext() && flag){
            if (it.next().getId() == Integer.parseInt(removeId)){
                int pos = it.nextIndex() - 1;
                this.network.remove(pos);
                this.procList.get(pos).destroy();
                this.procList.remove(pos);
                flag = false;
            }
        }
        System.out.println("Done!");
    }


    /* This function waits for confirmation that an operation has been completed. */
    public void waitResponse() throws IOException{
        /* Create a server socket for the main program. */
        ServerSocket ss = new ServerSocket(this.socket);

        /* Accept client connection. */
        Socket cs = ss.accept();

        /* Set up IO */
        PrintWriter out =
                new PrintWriter(cs.getOutputStream(), true);
        BufferedReader in =
                new BufferedReader(new InputStreamReader(cs.getInputStream()));
        String response;

        /* Read response. */
        while((response = in.readLine()) != null){
            String[] splitMSG = response.split(":");
            if (splitMSG[0].equals("DONE_JOIN")){ break; }
            if (splitMSG[0].equals("DONE_LEAVE")){ break; }
            if (splitMSG[0].equals("DONE_INSERT")){ break; }
            if (splitMSG[0].equals("DONE_DELETE")){ break; }
            if (splitMSG[0].equals("DONE_QUERY")){ break; }
            if (splitMSG[0].equals("DONE_RECREATE_ALL_REP")){ break; }
            if (splitMSG[0].equals("DONE_DELETE_ALL_REP")){ break; }
        }

        /* Update leader node with the result and close connection. */
        out.println("DONE");
        ss.close();
    }


    /* Print the state of the network. */
    public void dumpNetwork() throws IOException {
        Socket sock = new Socket("localhost", this.leadNodeSocket);
        PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
        String responseMSG;
        out.println("DUMP" + ":" + procList.size());

        /* Wait for response. */
        while ((responseMSG = in.readLine()) != null)
        {
            /* Leader approves request. */
            if (responseMSG.equals("DONE")){ break; }
        }
        sock.close();
    }


    /* A function that listens to the terminal for user commands */
    private void listen() throws IOException {
        while(true)
        {
            BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
            String input = stdIn.readLine();
            if (input != null)
            {
                String[] splitInput =input.split(":");
                new UserInterface(splitInput, this).start();
            }
        }
    }


    /* Terminate program */
    public void terminate(){
        for (Process p : procList) { p.destroy(); }
        System.exit(0);
    }


    /* DATA OPERATIONS */
    /******************************************************************************************************************/

    /* Insert data to a node. */
    public void insertData(String key, int value) throws IOException {
        /*Get a random socket*/
        int index = new Random().nextInt(procList.size());
        int s = network.get(index).getSocket();

        Socket sock = new Socket("localhost", s);
        PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
        String responseMSG;
        out.println("INSERT" + ":" + key + ":" + value + ":" + s + ":" + "REPORT");

        /* Wait for response. */
        while ((responseMSG = in.readLine()) != null)
        {
            if (responseMSG.equals("DONE")){ break; }
        }
        sock.close();
        this.waitResponse();
    }

    /* Delete data from a node. */
    public void deleteData(String key, int value) throws IOException {
        /*Get a random socket*/
        int index = new Random().nextInt(procList.size());
        int s = network.get(index).getSocket();

        Socket sock = new Socket("localhost", s);
        PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
        String responseMSG;
        out.println("DELETE" + ":" + key + ":" + value + ":" + s + ":" + "REPORT");

        /* Wait for response. */
        while ((responseMSG = in.readLine()) != null)
        {
            if (responseMSG.equals("DONE")){ break; }
        }
        sock.close();
        this.waitResponse();
    }

    public void query(String queryKey) throws IOException {
        /*Get a random socket*/
        int index = new Random().nextInt(procList.size());
        int s = network.get(index).getSocket();

        Socket sock = new Socket("localhost", s);
        PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
        String responseMSG;
        out.println("QUERY" + ":" + queryKey + ":" + s + ":" + procList.size());

        /* Wait for response. */
        while ((responseMSG = in.readLine()) != null)
        {
            if (responseMSG.equals("DONE")){ break; }
        }
        sock.close();
        this.waitResponse();
    }

    public void recreateAllReplicas() throws IOException {
        Socket sock = new Socket("localhost", this.leadNodeSocket);
        PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
        String responseMSG;
        out.println("RECREATE_ALL_REP" + ":" + procList.size());

        /* Wait for response. */
        while ((responseMSG = in.readLine()) != null)
        {
            /* Leader approves request. */
            if (responseMSG.equals("DONE")){ break; }
        }
        sock.close();
        this.waitResponse();
    }

    public void deleteAllReplicas() throws IOException {
        Socket sock = new Socket("localhost", this.leadNodeSocket);
        PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
        String responseMSG;
        out.println("DELETE_ALL_REP" + ":" + procList.size());

        /* Wait for response. */
        while ((responseMSG = in.readLine()) != null)
        {
            /* Leader approves request. */
            if (responseMSG.equals("DONE")){ break; }
        }
        sock.close();
        this.waitResponse();
    }


    /* Print all data starting from the leader node. */
    public void printData() throws IOException{
        Socket sock = new Socket("localhost", this.leadNodeSocket);
        PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
        String responseMSG;
        out.println("PRINT_DATA" + ":" + procList.size());

        /* Wait for response. */
        while ((responseMSG = in.readLine()) != null)
        {
            /* Leader approves request. */
            if (responseMSG.equals("DONE")){ break; }
        }
        sock.close();
    }
}
