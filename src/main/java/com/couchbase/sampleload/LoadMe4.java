package com.couchbase.sampleload;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.deps.io.netty.channel.ConnectTimeoutException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.TemporaryFailureException;
import com.couchbase.client.java.transcoder.JsonTranscoder;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import rx.Observable;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.java.util.retry.RetryBuilder.anyOf;

public class LoadMe4 {

    public static void main(String[] args) throws IOException, ParseException {

        CouchbaseEnvironment environment = DefaultCouchbaseEnvironment.builder()
                .queryEnabled(true)
                .dnsSrvEnabled(false)
                //.observeIntervalDelay()
                .build();
        CouchbaseCluster cluster = CouchbaseCluster.create(environment, "127.0.0.1");
        final Bucket bucket = cluster.openBucket("default");

        int numDocs = 20000;
        long start = System.nanoTime();
        final int MAX_RETRIES = 20000;
        final int RETRY_DELAY = 50;
        final int MAX_DELAY = 1000;
        int i = 0;
        List<JsonDocument> docArray = new ArrayList<>();
        List<JsonDocument> docArray2 = new ArrayList<>();

        String sCurrentLine;
        JSONParser parser = new JSONParser();
        JsonTranscoder trans = new JsonTranscoder();
        JsonObject jsonObj;

        BufferedReader br1;
        BufferedReader br2;

        br1 = new BufferedReader(new FileReader("/Users/justin/Documents/javastuff/CBScala/data/users.json"));
        br2 = new BufferedReader(new FileReader("/Users/justin/Documents/javastuff/CBScala/data/clickstream.json"));

        while ((sCurrentLine = br2.readLine()) != null) {
                //System.out.println("Record:\t" + sCurrentLine);
                //jsonObj = jsonParser.parse(sCurrentLine);

                String obj;
                try {
                    obj = parser.parse(sCurrentLine).toString();
                    //JSONObject jsonObject = (JSONObject) obj;
                    jsonObj = trans.stringToJsonObject(obj);
                    //UUID theid = UUID.randomUUID();
                    String theid = "click::" + i;
                    docArray.add(i, JsonDocument.create(theid.toString(), jsonObj));
                } catch (ParseException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                i++;
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

        long end = System.nanoTime();

        System.out.println("Bulk loading " + i + " docs took: " + TimeUnit.NANOSECONDS.toSeconds(end - start) + "s.");

    }
}
