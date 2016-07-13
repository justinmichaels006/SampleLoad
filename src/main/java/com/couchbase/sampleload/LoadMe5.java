package com.couchbase.sampleload;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonStringDocument;
import com.couchbase.client.java.document.json.JsonObject;
import javafx.event.Event;
import org.json.simple.parser.ParseException;

import java.io.IOException;

/**
 * Created by justin on 5/16/16.
 */
public class LoadMe5 {

    public static void main(String[] args) throws Exception {

        CouchbaseCluster cluster = CouchbaseCluster.create("192.168.61.101");
        final Bucket bucket = cluster.openBucket("testload");
        JsonObject jsonObject = (JsonObject) bucket.get("CONT").content();
        bucket.close();
        cluster.disconnect();

        int numDocs = 5000;
        CouchbaseEventDaoImpl8 cb = CouchbaseEventDaoImpl8.getInstance();

        for (int i = 0; i < numDocs; i++) {
            cb.insertEvent(jsonObject, "cl1", 0, true, true);
        }

    }
}
