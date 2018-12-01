//package com.company;

public class Request {

    public String dracoReturnVal;
    public boolean dracoDone;

    public int type;
    public String path;
    public String value;

    public Request(int type, String path, String value) {
        this.type = type;
        this.path = path;
        this.value = value;
    }
}
