//package com.company;

public class Main {

    public static void main(String[] args) { 
        int total_req = 1000;
        DracoRequestProcessor drp = new DracoRequestProcessor("i", 30);
        long start = System.nanoTime();
        for (int i = 0; i < total_req; i++) {
            Request rq = new Request(OpCode.create, "key" + i, "value" + i);
            drp.processRequest(rq);
        }
        drp.processRequest(new Request(OpCode.closeSession, "key", "value"));
        try {
            drp.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
        long end = System.nanoTime();
        long microseconds = (end-start)/1000;
        System.out.println("Duration ms/op: " + microseconds/(total_req * 1000.0));
    }
}
