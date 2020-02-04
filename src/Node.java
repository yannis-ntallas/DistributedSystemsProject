package com;

/*
 * NATIONAL TECHNICAL UNIVERSITY OF ATHENS
 * SCHOOL OF ELECTRICAL AND COMPUTER ENGINEERING
 * Distributed Systems Project
 * @author: Ntallas Ioannis, 03111418
 * @email: ynts@outlook.com
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.TimeUnit;

public class Node {
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLACK = "\u001B[30m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_WHITE = "\u001B[37m";

    private int prevSocket;
    private int nextSocket;
    private int mySocket;
    private int leadSocket;
    private int mainSocket;
    private String id;
    private String hid;
    private ArrayList<Data> nodeData;
    private ArrayList<Data> replicas;
    private String consistency;
    private int replicationFactor;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public Node(int mySocket, String id, int leaderSocket, int mainSocket, String con, int repF){
        this.mySocket = mySocket;
        this.id = id;
        this.leadSocket = leaderSocket;
        this.mainSocket = mainSocket;
        this.prevSocket = mySocket;
        this.nextSocket = mySocket;
        this.hid = "";
        this.consistency = con;
        this.replicationFactor = repF;
        this.nodeData = new ArrayList<>();
        this.replicas = new ArrayList<>();
    }

    /* Get */
    public int getMySocket(){ return this.mySocket; }
    public int getPrevSocket(){ return this.prevSocket; }
    public int getNextSocket(){ return this.nextSocket; }
    public int getMainSocket(){ return this.mainSocket; }
    public String getId(){ return this.id; }

    /* Set */
    public void setPrevSocket(int s){ this.prevSocket = s; }
    public void setNextSocket(int s){ this.nextSocket = s; }
    public void setHid()throws NoSuchAlgorithmException {
        this.hid = sha1Hash(this.id);
    }


    public static void main(String args[]) throws IOException, NoSuchAlgorithmException {
        /* Create the node called by process builder. All arguments are given ass string so we parse them to
         * proper format.
         */
        Node n = new Node(Integer.parseInt(args[0]), args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]),
                args[4], Integer.parseInt(args[5]));
        n.setHid();

        /* Create a server socket and attach it to the node to execute requests by other nodes. */
        ServerSocket serverSocket = new ServerSocket(n.mySocket);

        while (true) {
            Socket clientSocket = serverSocket.accept();
            new ServerThread(clientSocket, n).start();
        }
    }


    /* BASIC NODE FUNCTIONALITY */
    /*****************************************************************************************************************/

    /* Send a request to the specified socket and receive response of the node. */
    public String sendRequest(int socket, String request) throws IOException {
        String response;
        String responseMSG = null;

        /* Connect to server and send request */
        Socket s = new Socket("localhost", socket);
        PrintWriter out = new PrintWriter(s.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
        out.println(request);

        while ((response = in.readLine()) != null) {
            String[] splitMSG =response.split(":");

            if (splitMSG[0].equals("DONE")) { break; }
            if (splitMSG[0].equals("ID_IS")) {
                responseMSG=splitMSG[1];
                break;
            }
            break;
        }
        s.close();
        return responseMSG;
    }


    /* A request by a client node to the current node to join the network right before the current node. */
    public void handleJoin(int clientSocket, String clientId) throws InterruptedException {
        try {
            String clientHash = sha1Hash(clientId);
            String prevId = sendRequest(prevSocket, "PROVIDE_ID");
            String prevHash = sha1Hash(prevId);
            String myHash = sha1Hash(id);

            boolean currentIsFirst = false;
            if (myHash.compareTo(prevHash) < 0) {currentIsFirst = true;}

            if (clientIsBefore(clientHash, myHash, prevHash)) {
                /* The node has been accepted to be placed before the current node in the network.
                 * The current node, the client and the previous must update their neighbour sockets.
                 */
                sendRequest(clientSocket, "UPDATE" + ":" + this.mySocket + ":" + this.prevSocket);
                sendRequest(this.prevSocket, "UPDATE" + ":" + clientSocket + ":" + "EMPTY");
                sendRequest(this.mySocket, "UPDATE" + ":" + "EMPTY" + ":" + clientSocket);

                /* Hold data to remove AFTER sending it to another node, because the indexing
                 * of the ArrayList will change.
                 */
                ArrayList<Data> toBeRemoved = new ArrayList<>();

                /* If the current node has data that belong to the new node we must send it to him. */
                if (!(nodeData.isEmpty())) {
                    for (Data d : nodeData) {
                        String hKey = sha1Hash(d.getKey());
                        boolean send = false;

                        /* The new node becomes the first node. */
                        if (currentIsFirst) {
                            if(hKey.compareTo(prevHash) > 0) {send = true;}
                            if(hKey.compareTo(clientHash) <= 0) {send = true;}
                        }

                        /* Normal situation. */
                        else {
                            if (hKey.compareTo(clientHash) <= 0) {send = true;}
                        }

                        if (send) {
                        /* Send the data to the new node. */
                            System.out.println("Sending data from " + id + " to " + clientId + "...");
                            System.out.println("-> key: " + d.getKey() + ", val: " + d.getValue());
                            sendRequest(clientSocket, "INSERT" + ":" + d.getKey() + ":" + d.getValue() +
                                    ":" + mySocket + ":" + "NO_REPORT");
                            toBeRemoved.add(d);
                        }
                    }
                }
                for (Data d : toBeRemoved){
                            /* Delete from current node. */
                            System.out.println("Erasing data...");
                            sendRequest(mySocket, "DELETE" + ":" + d.getKey() + ":" + d.getValue() +
                                    ":" + mySocket + ":" + "NO_REPORT");
                }

                /* Send DONE to leader node, since it started there. */
                sendRequest(this.leadSocket, "DONE_JOIN");

            }
            else {
                /* The client doesn't go here, so we forward the request to the next node. */
                sendRequest(this.nextSocket, "JOIN" + ":" + clientSocket + ":" + clientId);
            }
        }
        catch(NoSuchAlgorithmException e) {
            e.printStackTrace();
            System.out.println("Exception while hashing client id [ joinRequest(); ].");
        }
        catch(IOException e){
            e.printStackTrace();
            System.out.println("Exception at socket communication in join Node.");
        }
    }


    /* A request for leaving the network is handled here. */
    public void handleLeave(String leaveId) throws NoSuchAlgorithmException, IOException{

        /* If the node is leaving then it updates its neighbours and sends a "done" message to the leader node. */
        if (leaveId.equals(id)){
            System.out.println("Node " + id + " is leaving.");
            sendRequest(nextSocket, "UPDATE" + ":" + "EMPTY" + ":" + prevSocket);
            sendRequest(prevSocket, "UPDATE" + ":" + nextSocket + ":" + "EMPTY");

            /* Send any files to the appropriate nodes. */
            if(!(nodeData.isEmpty())){
                for (Data d : nodeData){
                    sendRequest(nextSocket, "INSERT" + ":" + d.getKey() + ":" + d.getValue() + ":" +
                        mySocket + ":" + "NO_REPORT");
                }
            }
            sendRequest(leadSocket, "DONE_LEAVE");
        }
        else { sendRequest(nextSocket, "LEAVE" + ":" + leaveId); }
    }


    /* DATA FUNCTIONS */
    /*****************************************************************************************************************/

    /* A function that checks if the data can be inserted in the current node. */
    public void insertData(String key, int value, int initSocket, String param)throws IOException, NoSuchAlgorithmException {
        String prevId = sendRequest(prevSocket, "PROVIDE_ID");
        String prevHash = sha1Hash(prevId);
        String keyHash = sha1Hash(key);
        Data d = new Data(key, value);

        /* Checking the first node. */
        if (hid.compareTo(prevHash) < 0){
            /* Accept the key under following conditions. */
            if ((keyHash.compareTo(hid) <= 0) || (keyHash.compareTo(prevHash)) > 0){
                /* Check for duplicate key */
                int index = this.hasDuplicateKey(nodeData, keyHash);
                Lock w = lock.writeLock();
                if (index != -1){
                    w.lock();
                    nodeData.remove(index);
                    w.unlock();
                }
                /* Add new entry. */
                System.out.println("Adding data...");

                w.lock();
                nodeData.add(d);
                w.unlock();

                if (replicationFactor > 1) {
                    if (param.equals("REPORT")) {
                    /* Check if it needs to be reported to main */
                        if (consistency.equals("eventual")) {
                            sendRequest(initSocket, "DONE_INSERT");
                            sendRequest(this.nextSocket, "REPLICATE" + ":" + key + ":" + value + ":" + this.mySocket + ":"
                                    + (this.replicationFactor - 1) + ":" + "NO_REPORT");
                        }

                    /* Create replicas */
                        if (consistency.equals("linear")) {
                            sendRequest(this.nextSocket, "REPLICATE" + ":" + key + ":" + value + ":" + this.mySocket + ":"
                                    + (this.replicationFactor - 1) + ":" + "REPORT");
                        }
                    }
                }
                else{ sendRequest(initSocket, "DONE_INSERT"); }

            }
            /* Else forward the insert request */
            else { sendRequest(nextSocket, "INSERT" + ":" + key + ":" + value + ":" + initSocket + ":" + param); }
        }
        else {
            if((keyHash.compareTo(hid) <= 0) && (keyHash.compareTo(prevHash) > 0)){
                int index = this.hasDuplicateKey(nodeData, keyHash);
                Lock w = lock.writeLock();
                if (index != -1){
                    w.lock();
                    nodeData.remove(index);
                    w.unlock();
                }
                System.out.println("Adding data..." + key + ", " + value);

                w.lock();
                nodeData.add(d);
                w.unlock();

                if (replicationFactor > 1) {
                    if (param.equals("REPORT")) {
                    /* Check if it needs to be reported to main */
                        if (consistency.equals("eventual")) {
                            sendRequest(initSocket, "DONE_INSERT");
                            sendRequest(this.nextSocket, "REPLICATE" + ":" + key + ":" + value + ":" + this.mySocket + ":"
                                    + (this.replicationFactor - 1) + ":" + "NO_REPORT");
                        }

                    /* Create replicas */
                        if (consistency.equals("linear")) {
                            sendRequest(this.nextSocket, "REPLICATE" + ":" + key + ":" + value + ":" + this.mySocket + ":"
                                    + (this.replicationFactor - 1) + ":" + "REPORT");
                        }
                    }
                }
                else{ sendRequest(initSocket, "DONE_INSERT"); }

            }
            else { sendRequest(nextSocket, "INSERT" + ":" + key + ":" + value + ":" + initSocket + ":" + param); }
        }
    }


    /* Search for a specific entry or all entries with * character. */
    public void query(String queryKey, int initSocket, int counter) throws IOException, NoSuchAlgorithmException {
        /* If key is * then we print all nodes like PRINT function */
        if (queryKey.equals("*")){
            if(counter > 0){
                System.out.println("Printing data of node ID: " + id + ", Hash: " + hid);
                System.out.println(" ");

                if (this.nodeData.isEmpty()){ System.out.println(ANSI_GREEN + "    [D] No data." + ANSI_RESET); }
                else {
                    for (Data d : this.nodeData) {
                        System.out.println(ANSI_GREEN + "    [D] Value: " + d.getValue() + ", Key: " + d.getKey() + ANSI_RESET);
                    }
                }

                if (this.replicas.isEmpty()){ System.out.println(ANSI_RED + "    [R] No replicas." + ANSI_RESET); }
                else{
                    for (Data r : this.replicas){
                        System.out.println(ANSI_RED + "    [R] Value: " + r.getValue() + ", Key: " + r.getKey() + ANSI_RESET);
                    }
                }

                System.out.println(" ");

                sendRequest(nextSocket, "QUERY" + ":" + "*" + ":" + initSocket + ":" +Integer.toString(counter - 1));
            }
            else{ sendRequest(initSocket, "DONE_QUERY"); }
        }
        /* Check if we have the key */
        else {
            String queryHash = sha1Hash(queryKey);
            int index = this.hasDuplicateKey(this.nodeData, queryHash);
            Lock r = lock.readLock();
            r.lock();
            int rIndex = this.hasDuplicateKey(this.replicas, queryHash);

            /* Key in data. */
            if (index != -1) {
                System.out.println("Entry was found at node with ID " + id + ", Hash: " + hid);
                System.out.println(" ");
                System.out.println(ANSI_GREEN + "    [D] Key: " + queryKey + ", Hash: " + queryHash +
                        ", Value: " + nodeData.get(index).getValue() + ANSI_RESET);
                System.out.println(" ");

                /* Inform begin node that search is over. */
                sendRequest(initSocket, "DONE_QUERY");
            }
            /* Key in replicas */
            else if (rIndex != -1) {

                System.out.println("Entry was found at node with ID " + id + ", Hash: " + hid);
                System.out.println(" ");
                System.out.println(ANSI_RED + "    [R] Key: " + queryKey + ", Hash: " + queryHash +
                        ", Value: " + replicas.get(rIndex).getValue() + ANSI_RESET);

                System.out.println(" ");


                /* Inform begin node that search is over. */
                sendRequest(initSocket, "DONE_QUERY");
            }
            /* Key not found. */
            else {
                sendRequest(nextSocket, "QUERY" + ":" + queryKey + ":" + initSocket + ":" + Integer.toString(counter));
            }
            r.unlock();
        }
    }


    public void deleteData(String key, int value, int initSocket, String param)throws IOException, NoSuchAlgorithmException{
        /* Check if current has the data. */
        String hashedKey = sha1Hash(key);
        int index = this.hasElement(nodeData, hashedKey, value);
        if (index != -1){
            this.nodeData.remove(index);

            /* Check if it needs to be reported to main */
            if (consistency.equals("eventual") && param.equals("REPORT")) {
                sendRequest(initSocket, "DONE_DELETE");

                /* DELETE replicas */
                sendRequest(this.nextSocket, "DELETE_REPL" + ":" + key + ":" + value + ":" + this.mySocket + ":"
                        + (this.replicationFactor - 1) + ":" + "NO_REPORT");
            }

            /* DELETE replicas */
            if (consistency.equals("linear") && param.equals("REPORT")) {
                sendRequest(this.nextSocket, "DELETE_REPL" + ":" + key + ":" + value + ":" + this.mySocket + ":"
                        + (this.replicationFactor - 1) + ":" + "REPORT");
            }
        }
        /* Forward the request to the next node. */
        else { sendRequest(nextSocket, "DELETE" + ":" + key + ":" + value + ":" + initSocket + ":" + param); }
    }


    /* REPLICATION FUNCTIONS */
    /*****************************************************************************************************************/

    public void insertReplica(String key, int value, int owner, int counter, String param) throws IOException, NoSuchAlgorithmException{
        /* If we haven't reached max replicas */
        if (counter > 0){
            Data r = new Data(key, value);
            String rHash = sha1Hash(key);
            int index = this.hasDuplicateKey(replicas, rHash);
            Lock w = lock.writeLock();
            if (index != -1){

                w.lock();
                replicas.remove(index);
                w.unlock();

            }
            System.out.println(ANSI_CYAN + "Inserting replica of (" + key + ", " + value + ") in node " + id + ANSI_RESET);

            w.lock();
            replicas.add(r);
            w.unlock();

            counter--;
            if((this.consistency.equals("linear")) && (counter == 0) && (param.equals("REPORT"))){
                sendRequest(owner, "REPLICATION_DONE");
            }
            else if (counter > 0){
                System.out.println(ANSI_RED + "Forwarding to next socket: " + this.nextSocket + " with counter = " + counter + ANSI_RESET);
                sendRequest(this.nextSocket, "REPLICATE" + ":" + key + ":" + value + ":" + owner +
                        ":" + counter + ":" + param);
            }
            else{return;}
        }
        else {System.out.println("ERROR! 1");}
    }

    public void deleteReplica(String key, int value, int owner, int counter, String param) throws IOException, NoSuchAlgorithmException{
        /* If we haven't reached max replicas */
        if (counter > 0){
            String rHash = sha1Hash(key);
            int index = this.hasElement(replicas, rHash, value);
            if (index != -1){
                Lock w = lock.writeLock();
                w.lock();
                this.replicas.remove(index);
                w.unlock();
            }
            counter--;
            if((this.consistency.equals("linear")) && (counter == 0) && (param.equals("REPORT"))){
                sendRequest(owner, "DELETE_REPL_DONE");
            }
            else if (counter > 0){
                sendRequest(this.nextSocket, "DELETE_REPL" + ":" + key + ":" + value + ":" + owner +
                        ":" + counter + ":" + param);
            }
            else{return;}
        }
        else {System.out.println("ERROR! 2");}
    }

    public void deleteAllReplicas(int counter) throws IOException {
        if (counter > 0) {
            replicas.clear();
            sendRequest(this.nextSocket, "DELETE_ALL_REP" + ":" + (counter - 1));
        }
        else { sendRequest(this.leadSocket, "DONE_DELETE_ALL_REP"); }
    }

    public void recreateReplicas(int counter) throws IOException, InterruptedException {
        if (counter > 0){
            for (Data d : nodeData){
                System.out.println(ANSI_BLUE + "Creating replicas for (" + d.getKey() + ", " + d.getValue() + ")." + ANSI_RESET);

                sendRequest(this.nextSocket, "REPLICATE" + ":" + d.getKey() + ":" + d.getValue() + ":" +
                        this.mySocket + ":" + (this.replicationFactor - 1) + ":" + "NO_REPORT");
                TimeUnit.MILLISECONDS.sleep(200);
            }
            sendRequest(this.nextSocket, "RECREATE_ALL_REP" + ":" + (counter - 1));
        }
        else { sendRequest(this.leadSocket, "DONE_RECREATE_ALL_REP"); }
    }

    /* SUPPORT FUNCTIONS */
    /*****************************************************************************************************************/

    /* Print all nodes in connection order and starting from the leader node. */
    public void dumpState(int counter) throws IOException, NoSuchAlgorithmException{
        if (counter > 0) {
            System.out.println("ID: " + id + ", Hashed ID: " + hid + ", Socket: " + mySocket +
                    ", Previous: " + prevSocket +", Next: " + nextSocket);
            sendRequest(nextSocket, "DUMP" + ":" + Integer.toString(counter - 1));
        }
    }


    /* Print the data each node holds, starting from the leader node. */
    public void printData(int counter) throws IOException, NoSuchAlgorithmException {
        if (counter > 0){
            System.out.println("Printing data of node ID: " + id + ", Hash: " + hid);
            System.out.println(" ");
            if (this.nodeData.isEmpty()) { System.out.println("    [D] No data."); }
            else {
                for (Data d : this.nodeData) {
                    System.out.println("    [D] Value: " + d.getValue() + ", Key: " + d.getKey() + ", Hash: " + sha1Hash(d.getKey()));
                }
            }
            if (this.replicas.isEmpty()) { System.out.println("    [R] No replicas."); }
            else{
                for (Data r : this.replicas){
                    System.out.println("    [R] Value: " + r.getValue() + ", Key: " + r.getKey() + ", Hash: " + sha1Hash(r.getKey()));
                }
            }
            System.out.println(" ");
            sendRequest(nextSocket, "PRINT_DATA" + ":" + Integer.toString(counter - 1));
        }
    }


    /* A function to check if the client node can join the network before the current node. */
    public boolean clientIsBefore(String clientHash, String currentHash, String prevHash){
        boolean result = false;

        /* Case when the network has only the leader node. */
        if (currentHash.compareTo(prevHash) == 0) {
            if (clientHash.compareTo(currentHash) > 0) { result = true; }
        }

        /* Case that current node is the first node. */
        else if (currentHash.compareTo(prevHash) < 0) {
            if ((clientHash.compareTo(currentHash) > 0) && (clientHash.compareTo(prevHash)) > 0) { result = true; }
            if ((clientHash.compareTo(currentHash) < 0) && (clientHash.compareTo(prevHash)) < 0) { result = true; }
        }

        /* Normal case. */
        else {
            if ((clientHash.compareTo(currentHash) < 0) && (prevHash.compareTo(clientHash) < 0)) {
                result = true;
            }
        }
        return result;
    }


    public int hasElement(ArrayList<Data> list, String k, int v) throws NoSuchAlgorithmException{
        Lock r = lock.readLock();
        r.lock();
        for (Data d : list) {
            if ((sha1Hash(d.getKey()).equals(k)) && (d.getValue() == v)){
                r.unlock();
                return list.indexOf(d); }
        }
        r.unlock();
        return -1;
    }

    public int hasDuplicateKey(ArrayList<Data> list, String k) throws NoSuchAlgorithmException{
        Lock r = lock.readLock();
        r.lock();
        for (Data d : list) {
            if (sha1Hash(d.getKey()).equals(k)){
                r.unlock();
                return list.indexOf(d); }
        }
        r.unlock();
        return -1;
    }


    /* Hashing String with SHA1 */
    public String sha1Hash(String input) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA1");
        byte[] byteData = md.digest(input.getBytes());

        /* Convert the byte to hex format. */
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < byteData.length; i++) {
            sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
        }
        return sb.toString();
    }
}
