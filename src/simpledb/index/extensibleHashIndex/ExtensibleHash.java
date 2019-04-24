package simpledb.index.extensibleHashIndex;

import java.util.*;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

public class ExtensibleHash implements Index {
    private String idxname;
    private Schema sch;
    private Transaction tx;
    private Constant searchkey = null;
    private TableScan ts = null;
    private ArrayList<Bucket> buckets = new ArrayList<Bucket>();
    public static int KEYS = 4; // Arbitrary cap on number of keys allowed per bucket
    private int globalDepth = 1; // Starting global depth will be 1, then build onwards
    private int start = 1;

    public ExtensibleHash(String idxname, Schema sch, Transaction tx) {
        this.idxname = idxname;
        this.sch = sch;
        this.tx = tx;
    }

    public String toString(Constant searchkey, int KEYS) {
        String string = Integer.toBinaryString(searchkey.hashCode());
        int difference = string.length() - KEYS;

        // If the string we find is longer than what we want, take only the substring starting
        // at the index of the difference (returns correct amount of bits we want to see as a string)
        if(difference > 0) {
            string = string.substring(difference);
        }
        // If the difference shows that the length we want is larger than the length of the string,
        // we will pad the string we found to have preceding 0's
        if(difference < 0) {
            for(int i = 0; i > difference; difference++) {
                string += "0";
            }
        }
        //return bit representation as String
        return string;
    }

    public void beforeFirst(Constant key) {
        // if global depth is 1 (starts on 1), then it is new and we must create from scratch. Change value after
        if(start == 1) {
            Bucket bucket1 = new Bucket(1, KEYS);
            Bucket bucket2 = new Bucket(2, KEYS);
            buckets.add(bucket1);
            buckets.add(bucket2);
            start = -1;
        }
        close();
        this.searchkey = key;

        String binary = this.toString(searchkey, globalDepth);
        String tblname = idxname + binary;
        TableInfo ti = new TableInfo(tblname, sch);
        ts = new TableScan(ti, tx);
    }

    /**
     * Moves to the next record having the search key.
     * The method loops through the table scan for the bucket,
     * looking for a matching record, and returning false
     * if there are no more such records.
     * @see simpledb.index.Index#next()
     */
    public boolean next() {
        while (ts.next())
            if (ts.getVal("dataval").equals(searchkey))
                return true;
        return false;
    }

    /**
     * Retrieves the dataRID from the current record
     * in the table scan for the bucket.
     * @see simpledb.index.Index#getDataRid()
     */
    public RID getDataRid() {
        int blknum = ts.getInt("block");
        int id = ts.getInt("id");
        return new RID(blknum, id);
    }

    /**
     * Inserts a new record into the table scan for the bucket.
     * @see simpledb.index.Index#insert(simpledb.query.Constant, simpledb.record.RID)
     */
    public void insert(Constant bucketID, RID rid) {
        beforeFirst(bucketID);
        // For all buckets in the bucketlist
        for (Bucket bucket : buckets) {
            // If we find a bucket with the given ID
            if (bucket.getBucketID() == bucketID.hashCode()) {
                // If there is space in the bucket, add the searchkey
                if (bucket.getSearchKeys().size() < KEYS) {
                    bucket.addKey(searchkey);
                    String tblname = idxname + bucketID;
                    TableInfo ti = new TableInfo(tblname, sch);
                    ts = new TableScan(ti, tx);
                } else {
                    int depth = bucket.getLocalDepth();
                    // If the buckets localDepth is greater than the globalDepth, add to local depth
                    if (depth < globalDepth) {
                        System.out.print("Bucket: " + bucket + " local depth increased!\n It was: " + bucket.getLocalDepth());
                        depth++;
                        System.out.println(". It is now:" + bucket.getLocalDepth());
                        // Make bucket into two buckets
                        Bucket newBucket1 = new Bucket(Integer.parseInt("0" + bucket.getBucketID()), depth);
                        Bucket newBucket2 = new Bucket(Integer.parseInt("1" + bucket.getBucketID()), depth);
                        // Redistribute values
                        ArrayList<Constant> values = bucket.getSearchKeys();
                        for (Constant value : values) {
                            String string = toString(value, depth);
                            if (Integer.parseInt(string) == newBucket1.getBucketID()) {
                                newBucket1.addKey(value);
                            } else {
                                newBucket2.addKey(value);
                            }
                        }
                        buckets.remove(bucket);
                        // Add bucketID (key) to the correct bucket
                        String string = toString(bucketID, depth);
                        if (Integer.parseInt(string) == newBucket1.getBucketID()) {
                            newBucket1.addKey(bucketID);
                        } else {
                            newBucket2.addKey(bucketID);
                        }
                        buckets.add(newBucket1);
                        buckets.add(newBucket2);

                        String tblname = idxname + bucketID;
                        TableInfo ti = new TableInfo(tblname, sch);
                        ts = new TableScan(ti, tx);
                    } else { // When the global depth also must be changed
                        System.out.print("Global depth increased!\n It was: " + globalDepth);
                        globalDepth++;
                        System.out.println(". It is now:" + globalDepth);


                        System.out.print("Bucket: " + bucket + " local depth increased!\n It was: " + bucket.getLocalDepth());
                        depth++;
                        System.out.println(". It is now:" + bucket.getLocalDepth());
                        // Make bucket into two buckets
                        Bucket newBucket1 = new Bucket(Integer.parseInt("0" + bucket.getBucketID()), depth);
                        Bucket newBucket2 = new Bucket(Integer.parseInt("1" + bucket.getBucketID()), depth);
                        // Redistribute values
                        ArrayList<Constant> values = bucket.getSearchKeys();
                        for (Constant value : values) {
                            String string = toString(value, depth);
                            if (Integer.parseInt(string) == newBucket1.getBucketID()) {
                                newBucket1.addKey(value);
                            } else {
                                newBucket2.addKey(value);
                            }
                        }
                        buckets.remove(bucket);
                        // Add bucketID (key) to the correct bucket
                        String string = toString(bucketID, depth);
                        if (Integer.parseInt(string) == newBucket1.getBucketID()) {
                            newBucket1.addKey(bucketID);
                        } else {
                            newBucket2.addKey(bucketID);
                        }
                        buckets.add(newBucket1);
                        buckets.add(newBucket2);

                        String tblname = idxname + bucketID;
                        TableInfo ti = new TableInfo(tblname, sch);
                        ts = new TableScan(ti, tx);
                    }
                }
            }
        }
        ts.insert();
        ts.setInt("block", rid.blockNumber());
        ts.setInt("id", rid.id());
        ts.setVal("dataval", bucketID);
    }
    /**
     * Deletes the specified record from the table scan for
     * the bucket.  The method starts at the beginning of the
     * scan, and loops through the records until the
     * specified record is found.
     * @see simpledb.index.Index#delete(simpledb.query.Constant, simpledb.record.RID)
     */
    public void delete(Constant val, RID rid) {
        beforeFirst(val);
        while(next())
            if (getDataRid().equals(rid)) {
                ts.delete();
                return;
            }
    }

    /**
     * Closes the index by closing the current table scan.
     * @see simpledb.index.Index#close()
     */
    public void close() {
        if (ts != null)
            ts.close();
    }
}
