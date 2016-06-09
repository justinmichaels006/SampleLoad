package com.couchbase.sampleload;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonGenerationException;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.JsonMappingException;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.deps.io.netty.channel.ConnectTimeoutException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.error.TemporaryFailureException;
import rx.Observable;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.java.util.retry.RetryBuilder.anyOf;

public class CouchbaseEventDaoImpl2 {

    private static CouchbaseEventDaoImpl2 instance;
    // Cluster 1 Objects
    private Cluster cl1Cluster;
    private Bucket cl1PocBucket;
    private Bucket cl1EventHistoryBucket;

    // Cluster 2 Objects 10.16.144.198
    private Cluster cl2Cluster;
    private Bucket cl2PocBucket;
    private Bucket cl2EventHistoryBucket;

    private String cl1 = "cl1";
    private String cl2 = "cl2";

    public static CouchbaseEventDaoImpl2 getInstance() {
        if (instance == null) {
            instance = new CouchbaseEventDaoImpl2();

        }
        return instance;
    }

    private CouchbaseEventDaoImpl2() {

    //Setting up Cluster 1 on DC1
    try {
        cl1Cluster = CouchbaseCluster.create("192.168.61.101");
            //observableBucket.doOnCompleted(onCompleted)
        cl1PocBucket = cl1Cluster.openBucket("testload");
                //.retryWhen(any().max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build()).toBlocking().first();
                //cl1PocBucket = cl1Cluster.openBucket("event");
                //cl1EventHistoryBucket = cl1Cluster.openBucket("event_history");
    } catch(Exception exc) {
        System.out.println("Failure in creating CL1 cluster " + exc.getMessage());
    }
    //Setting up Cluster 2 on DC2
    try {
        cl2Cluster = CouchbaseCluster.create("192.168.61.102");
        cl2PocBucket = cl2Cluster.openBucket("testload");
            //.retryWhen(any().max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build()).toBlocking().first();
            // cl2EventHistoryBucket = cl2Cluster.openBucket("event_history");
    } catch(Exception exc) {
        System.out.println("Failure in creating CL2 cluster " + exc.getMessage());
    }

    }

    /**
     * This method will be the main call into insert event flow. This will take
     * care of cluster failover management, decision on Sync / Async, decison on
     * ReplicateTo
     *
     * @param event
     *            - The event Object
     * @param cluster
     *            - The Primary couchbase cluster
     * @param ttl
     *            - Document expiry in seconds
     * @throws JsonGenerationException
     * @throws JsonMappingException
     * @throws IOException
     */
    public void insertEvent(JsonObject event, String cluster, int ttl) throws Exception {
        Bucket primaryCluster;
        Bucket failOverCluster;
        if (cl1.equals(cluster)) {
            primaryCluster = cl1PocBucket;
            failOverCluster = cl2PocBucket;
        } else {
            primaryCluster = cl2PocBucket;
            failOverCluster = cl1PocBucket;
        }
        try {
            System.out.println("Starting Here");
            insertEvent2(event, primaryCluster, ttl);
        } catch (Exception exc) {
            System.out.println("Exception in primary CLuster" + exc.getMessage()
                    + " Switching to failOverCluster" + failOverCluster);// NOPMD
            exc.printStackTrace();//NOPMD
            insertEvent2(event, failOverCluster, ttl);
        }
    }


    public void insertEvent2(JsonObject event, Bucket cluster, int ttl)
            throws JsonGenerationException, JsonMappingException, IOException {
    //Code to execute the insert / append on the AsyncBucket (getEventBucket() returns a AsyncBucket for cluster (cl1/cl2) )

    ObjectMapper mapper = new ObjectMapper();
    String strEvent = mapper.writeValueAsString(event);
    UUID eventID = UUID.randomUUID(); //Justin

    int MAX_RETRIES = 20;
    int RETRY_DELAY = 5;
    int MAX_DELAY = 20;

    RawJsonDocument jsonDoc = RawJsonDocument.create("Event:" + eventID, ttl, strEvent);

    StringBuilder guidStringValue = new StringBuilder();
    guidStringValue.append(event.getString("CUST_id"));
    guidStringValue.append(":");
    guidStringValue.append(eventID);
    guidStringValue.append(",");
    StringDocument guidDoc = StringDocument.create("Guid:" + eventID, ttl, guidStringValue.toString());

    long startTime = System.nanoTime();
    // Handling Event insert
    Observable<RawJsonDocument> obsrv1 = cluster.async().insert(jsonDoc)
            .retryWhen(anyOf(TemporaryFailureException.class)
            .max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build())
            .retryWhen(anyOf(BackpressureException.class)
            .max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build());
    long time1 = System.nanoTime();

    // Handling Append Only structures
    // Appending Guid Document
    Observable<StringDocument> obsrv2 = cluster.async().append(guidDoc)
            .retryWhen(anyOf(TemporaryFailureException.class)
            .max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build())
            .retryWhen(anyOf(BackpressureException.class)
            .max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build())
            .onErrorResumeNext(exc -> {
                if (exc instanceof DocumentDoesNotExistException) {
                    return cluster.async().insert(guidDoc);
                }
                else {
                    return Observable.error(exc);
                }
            });

    StringDocument vcardDoc = StringDocument.create("VCard:" + event.getString("CUST_element4"), ttl, guidStringValue.toString());
    Observable<StringDocument> obsrv3 = cluster.async().append(vcardDoc)
                .retryWhen(anyOf(TemporaryFailureException.class)
                .max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build())
                .retryWhen(anyOf(BackpressureException.class)
                .max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build())
                .onErrorResumeNext(exc -> {
                    if (exc instanceof DocumentDoesNotExistException) {
                        return cluster.async().insert(vcardDoc);
                    } else {
                        return Observable.error(exc);
                    }
                });

    //Observable.merge(obsrv1, obsrv2, obsrv3).toBlocking().subscribe(document1 -> System.out.println("Got: " + document1.id()));
    Observable.merge(obsrv1, obsrv2, obsrv3).toBlocking().subscribe(document1 -> System.out.println("Got: " + document1.id()));
    }
}
