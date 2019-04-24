package simpledb.index.extensibleHashIndex;

import simpledb.query.Constant;

import java.util.*;

public class Bucket {
    private int bucketID;
    private int localDepth;
//    private ArrayList<Integer> searchKeys = new ArrayList<Integer>();
    private ArrayList<Constant> searchKeys = new ArrayList<Constant>();


    public Bucket(int bucketID, int localDepth) {
        this.bucketID = bucketID;
        this.localDepth = localDepth;
    }

    public int getBucketID() {
        return bucketID;
    }
    public int getLocalDepth() {
        return localDepth;
    }
    public ArrayList<Constant> getSearchKeys() {
        return searchKeys;
    }
    public void addKey(Constant key) {
        searchKeys.add(key);
    }
    public void deleteKey(Constant key) {
        searchKeys.remove(key);
    }
}