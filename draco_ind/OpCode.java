//package com.company;

public interface OpCode {
    public final int notification = 0;

    public final int create = 1;

    public final int delete = 2;

    public final int exists = 3;

    public final int getData = 4;

    public final int setData = 5;

    public final int getACL = 6;

    public final int setACL = 7;

    public final int getChildren = 8;

    public final int sync = 9;

    public final int ping = 11;

    public final int getChildren2 = 12;

    public final int check = 13;

    public final int multi = 14;

    public final int create2 = 15;

    public final int reconfig = 16;

    public final int checkWatches = 17;

    public final int removeWatches = 18;

    public final int createContainer = 19;

    public final int deleteContainer = 20;

    public final int createTTL = 21;

    public final int auth = 100;

    public final int setWatches = 101;

    public final int sasl = 102;

    public final int createSession = -10;

    public final int closeSession = -11;

    public final int error = -1;
}
