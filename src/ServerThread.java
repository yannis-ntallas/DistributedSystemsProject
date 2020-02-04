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
import java.net.Socket;
import java.security.NoSuchAlgorithmException;

public class ServerThread extends Thread {
    protected Socket clientSocket;
    protected Node serverNode;

    public ServerThread(Socket s, Node n){
        this.clientSocket = s;
        this.serverNode = n;
    }

    /* All functionality of the thread goes here. */
    public void run(){
        try {
            /* Setup client-server IO */
            PrintWriter out =
                    new PrintWriter(clientSocket.getOutputStream(), true);
            BufferedReader in =
                    new BufferedReader(
                            new InputStreamReader(clientSocket.getInputStream()));
            String message;

            while((message = in.readLine()) != null){
                String[] splitMSG = message.split(":");
                String command = splitMSG[0];
                switch (command) {
                    /* The update command changes the sockets of the next or previous node of the
                     * server node, based on the client's message. UPDATE is called when a client
                     * wishes to join the network, has calculated the proper position and needs to
                     * update the neighbour nodes to correct their prev and next sockets.
                     *
                     * Command format: UPDATE:NEXT:PREVIOUS
                     * Example:        UPDATE:8080:NULL
                     */
                    case "UPDATE":
                       // System.out.println(message);
                        if (!(splitMSG[1].equals("EMPTY"))){
                            this.serverNode.setNextSocket(Integer.parseInt(splitMSG[1]));
                        }
                        if (!(splitMSG[2].equals("EMPTY"))){
                            this.serverNode.setPrevSocket(Integer.parseInt(splitMSG[2]));
                        }
                        out.println("DONE");
                        break;

                    /* A node requests to join so the server node will call its join handler to check
                     * if the node can enter before the server node.
                     *
                     * Command format: JOIN:SOCKET:ID
                     * Example:        JOIN:8080:42
                     */
                    case "JOIN":
                        out.println("DONE");
                        this.serverNode.handleJoin(Integer.parseInt(splitMSG[1]), splitMSG[2]);
                        break;

                    /* A node requests to leave so the server node will call its join handler to check
                     * if it is the node that must leave, else forwards the message.
                     *
                     * Command format: LEAVE:ID
                     * Example:        LEAVE:42
                     */
                    case "LEAVE":
                        out.println("DONE");
                        this.serverNode.handleLeave(splitMSG[1]);
                        break;

                    /* A node requests for the server node's ID. The server replies with the ID.
                     *
                     * Command format:  PROVIDE_ID
                     * Response format: ID_IS:ID
                     * Example:         ID_IS:42
                     */
                    case "PROVIDE_ID":
                        String id = this.serverNode.getId();
                        out.println("ID_IS" + ":" + id);
                        break;

                    /* After a join the main program must be updated with the result. The leader node is responsible
                     * for that. So the server node after whom the client joined sends a response to the leader to
                     * be forwarded to the main program The following sendRequest(); is called by the leader node.
                     */
                    case "DONE_JOIN":
                        out.println("DONE");
                        this.serverNode.sendRequest(this.serverNode.getMainSocket(), "DONE_JOIN");
                        break;

                    /* After updating its neighbour nodes, the leader informs the main program that all is done and
                     * the corresponding process can be safely terminated.
                     */
                    case "DONE_LEAVE":
                        out.println("DONE");
                        this.serverNode.sendRequest(this.serverNode.getMainSocket(), "DONE_LEAVE");
                        break;

                    /* A command by the user to do a full dump of the network state for monitoring purposes. */
                    case "DUMP":
                        out.println("DONE");
                        this.serverNode.dumpState(Integer.parseInt(splitMSG[1]));
                        break;

                    /* Data inserted or deleted from nodes are (value, key) pairs, where both variables are strings.
                     *
                     * Command format: INSERT:KEY:VALUE
                     * Example:        INSERT:alpha:42
                     */
                    case "INSERT":
                        out.println("DONE");
                        this.serverNode.insertData(splitMSG[1], Integer.parseInt(splitMSG[2]), Integer.parseInt(splitMSG[3]), splitMSG[4]);
                        break;

                    /* Report insert to main. */
                    case "DONE_INSERT":
                        out.println("DONE");
                        this.serverNode.sendRequest(this.serverNode.getMainSocket(), "DONE_INSERT");
                        break;

                    /* Data inserted or deleted from nodes are (value, key) pairs, where both variables are strings.
                     *
                     * Command format: DELETE:KEY:VALUE
                     * Example:        DELETE:alpha:42
                     */
                    case "DELETE":
                        out.println("DONE");
                        this.serverNode.deleteData(splitMSG[1], Integer.parseInt(splitMSG[2]), Integer.parseInt(splitMSG[3]), splitMSG[4]);
                        break;

                    /* Report delete to main. */
                    case "DONE_DELETE":
                        out.println("DONE");
                        this.serverNode.sendRequest(this.serverNode.getMainSocket(), "DONE_DELETE");
                        break;

                    /* Report print to main. */
                    case "PRINT_DATA":
                        out.println("DONE");
                        this.serverNode.printData(Integer.parseInt(splitMSG[1]));
                        break;

                    /* Search for all entries or a specific entry in a nodes. Query searches both data and replicas and reports
                     * the first thing it finds.
                     *
                     * Command format: QUERY:KEY
                     * Example:        QUERY:42
                     * Example 2:      QUERY:*   (Print all data)
                     */
                    case "QUERY":
                        out.println("DONE");
                        this.serverNode.query(splitMSG[1], Integer.parseInt(splitMSG[2]), Integer.parseInt(splitMSG[3]));
                        break;

                    /* Report query is over to main. */
                    case "DONE_QUERY":
                        out.println("DONE");
                        this.serverNode.sendRequest(this.serverNode.getMainSocket(), "DONE_QUERY");
                        break;

                    /* Create the replicas of an entry. */
                    case "REPLICATE":
                        out.println("DONE");
                        this.serverNode.insertReplica(splitMSG[1], Integer.parseInt(splitMSG[2]), Integer.parseInt(splitMSG[3]),
                                Integer.parseInt(splitMSG[4]), splitMSG[5]);
                        break;

                    /* Report replication to main. */
                    case "REPLICATION_DONE":
                        out.println("DONE");
                        this.serverNode.sendRequest(this.serverNode.getMainSocket(), "DONE_INSERT");
                        break;

                    /* Delete the replicas of a specific entry. */
                    case "DELETE_REPL":
                        out.print("DONE");
                        this.serverNode.deleteReplica(splitMSG[1], Integer.parseInt(splitMSG[2]), Integer.parseInt(splitMSG[3]),
                                Integer.parseInt(splitMSG[4]), splitMSG[5]);
                        break;

                    /* Report replica delete to main. */
                    case "DELETE_REPL_DONE":
                        out.println("DONE");
                        this.serverNode.sendRequest(this.serverNode.getMainSocket(), "DONE_DELETE");
                        break;

                    /* Recreate all the replicas of alla the data of every node. */
                    case "RECREATE_ALL_REP":
                        out.println("DONE");
                        this.serverNode.recreateReplicas(Integer.parseInt(splitMSG[1]));
                        break;

                    case "DONE_RECREATE_ALL_REP":
                        out.println("DONE");
                        this.serverNode.sendRequest(this.serverNode.getMainSocket(), "DONE_RECREATE_ALL_REP");
                        break;

                    /* Delete all the replicas in the network. */
                    case "DELETE_ALL_REP":
                        out.println("DONE");
                        this.serverNode.deleteAllReplicas(Integer.parseInt(splitMSG[1]));
                        break;

                    case "DONE_DELETE_ALL_REP":
                        out.println("DONE");
                        this.serverNode.sendRequest(this.serverNode.getMainSocket(), "DONE_DELETE_ALL_REP");
                        break;

                    default:
                        break;
                }
            }
            clientSocket.close();
        }
        catch (IOException e){
            /* Exception while reading from or writing to client socket.*/
            e.printStackTrace();
            System.out.println("Exception at Server Thread!");
            System.out.println("Client: " + clientSocket.toString());
            System.out.println("Server: " + Integer.toString(serverNode.getMySocket()));
        }
        catch (NoSuchAlgorithmException e){
            e.printStackTrace();
        }
        catch (InterruptedException e){
            e.printStackTrace();
        }
    }

}
