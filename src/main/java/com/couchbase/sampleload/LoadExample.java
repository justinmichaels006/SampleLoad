package com.couchbase.sampleload;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.deps.io.netty.channel.ConnectTimeoutException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.TemporaryFailureException;
import org.json.simple.parser.ParseException;
import rx.Observable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.java.util.retry.RetryBuilder.anyOf;

/**
 * Created by justin on 5/18/16.
 */
public class LoadExample {

    public static void main(String[] args) throws IOException, ParseException {

        CouchbaseCluster cluster = CouchbaseCluster.create("192.168.61.101");
        final Bucket bucket = cluster.openBucket("testload");

        List<JsonDocument> docArray = new ArrayList<>();
        final int MAX_RETRIES = 20000;
        final int RETRY_DELAY = 50;
        final int MAX_DELAY = 1000;

        for (int i = 0; i <= 100000; i++) {
            JsonArray addressArray = JsonArray.empty();

            JsonObject address1 = JsonObject.create()
                    .put("address", "1234 Somewhere St.")
                    .put("city", "Somewhere")
                    .put("state", "CA")
                    .put("zip", "12345");

            addressArray.add(address1);

            JsonObject content = JsonObject.create()
                    .put("id", "client:" + i)
                    .put("name", "Client " + i)
                    .put("address", addressArray);

            String eventID = ("client::" + i).toString();
            System.out.println("ID" + eventID + "plus" + content);

            docArray.add(i, JsonDocument.create(eventID, content));
        }

        Observable
                .from(docArray)
                //.map(num -> {
                //    return docArray.listIterator();
                //})
                .flatMap(doc -> {
                    return bucket.async().upsert(doc)
                            // do retry for each op individually to not fail the full batch
                            .retryWhen(anyOf(BackpressureException.class)
                                    .max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build())
                            .retryWhen(anyOf(TemporaryFailureException.class)
                                    .max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build())
                            .retryWhen(anyOf(ConnectTimeoutException.class)
                                    .max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.SECONDS, RETRY_DELAY, MAX_DELAY)).build());
                }).toBlocking().subscribe(document1 -> System.out.println("Got: " + document1.id()));


    }
}
