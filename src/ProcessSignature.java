package com;

/*
 * NATIONAL TECHNICAL UNIVERSITY OF ATHENS
 * SCHOOL OF ELECTRICAL AND COMPUTER ENGINEERING
 * Distributed Systems Project
 * @author: Ntallas Ioannis, 03111418
 * @email: ynts@outlook.com
 */

public class ProcessSignature<Process, Id, Socket> {
    private final Process pid;
    private final Id id;
    private final Socket sock;

    public ProcessSignature(Process p, Id i, Socket s){
        this.pid = p;
        this.id = i;
        this.sock = s;
    }

    public Process getPid(){
        return pid;
    }
    public Id getId(){
        return id;
    }
    public Socket getSocket(){
        return sock;
    }
}
