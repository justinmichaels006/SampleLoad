/* */
package com.couchbase.sampleload;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.ReplicaNotAvailableException;
import com.couchbase.client.core.ReplicaNotConfiguredException;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DurabilityException;
import com.couchbase.client.java.error.TemporaryFailureException;
import org.json.simple.parser.ParseException;
import rx.Observable;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.java.util.retry.RetryBuilder.anyOf;

public class LoadMe2 {

    public static void main(String[] args) throws IOException, ParseException {

        CouchbaseCluster cluster = CouchbaseCluster.create("192.168.61.101");
        final Bucket bucket = cluster.openBucket("testload");

        int numDocs = 10000;
        long start = System.nanoTime();
        final int MAX_RETRIES = 20000;
        final int RETRY_DELAY = 50;
        final int MAX_DELAY = 1000;
        JsonDocument doc = null;

        //Be sure to create a baseline CONT document to use first
        JsonObject jsonObjCONT = (JsonObject) bucket.get("CONT").content();

        for (int i = 0; i < numDocs; i++) {

            final String contID = "CONT::" + i;

            //CONT element 14 firstName
            String[] names = {"test", "Zero", "Club", "Moonkys", "znakes", "SeamOnster", "dnktwhm", "Rambo", "NDA", "sister"};
            Random ran = new Random();
            String CONTelement14 = names[ran.nextInt(names.length)];
            jsonObjCONT.put("Cont_element14", CONTelement14);
            doc = JsonDocument.create(contID, jsonObjCONT);

            /*Observable
                    .just(jsonObjCONT)
                    .map(num -> {
                        return JsonDocument.create(contID, jsonObjCONT);
                    })
                    .flatMap(doc -> {
                        return bucket
                                .async()
                                .insert(doc, ReplicateTo.ONE)
                                // do retry for each op individually to not fail the full batch
                                .retryWhen(anyOf(BackpressureException.class).max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build())
                                .retryWhen(anyOf(TemporaryFailureException.class).max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build());
                    })
                    .subscribe(doc -> System.out.println("Got: " + doc.id()));*/

            try {
                bucket.insert(doc, ReplicateTo.ONE);
            } catch (DurabilityException e) { //if the durability cannot be met
                if (e.getCause() instanceof ReplicaNotConfiguredException) {
                    //this exception is a fail fast if not enough replicas are configured on the bucket
                    System.out.println("Couldn't persist to FOUR nor replicate to THREE, not enough replicas configured " + e.toString());
                } else if (e.getCause() instanceof ReplicaNotAvailableException) {
                    //this exception occurs if enough replica are configured on the bucket but currently not enough are online
                    //eg. during a failover
                    System.out.println("Couldn't persist/replicate on 1 replica, not enough replicas online " + e.toString());
                } else {
                    System.out.println("Durability Exception " + e.toString());
                }
            }
        }

        long end = System.nanoTime();

        System.out.println("Bulk loading "+numDocs+" docs took: "+TimeUnit.NANOSECONDS.toSeconds(end-start)+"s.");
    }
}
