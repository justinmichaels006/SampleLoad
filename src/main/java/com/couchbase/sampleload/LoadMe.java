/* */
package com.couchbase.sampleload;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.TemporaryFailureException;
import com.couchbase.client.java.transcoder.JsonTranscoder;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import rx.Observable;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.java.util.retry.RetryBuilder.anyOf;

public class LoadMe {

    public static void main(String[] args) throws IOException, ParseException {

        CouchbaseCluster cluster = CouchbaseCluster.create("192.168.61.101");
        final Bucket bucket = cluster.openBucket("testload");

        int numDocs = 100;
        int subDocs = 30;
        int totalDocs = numDocs + (subDocs * numDocs);
        long start = System.nanoTime();
        final int MAX_RETRIES = 20000;
        final int RETRY_DELAY = 50;
        final int MAX_DELAY = 1000;
        final String[] element57 = {"WNET", "MMTV", "CNBC", "CNNN"};

        Random random = new Random();

        //final String filePath1 = "/tmp/SampleLoad/ACCT.json";
        //final String filePath2 = "/tmp/SampleLoad/CUST.json";
        final String filePath1 = "/Users/justin/Documents/javastuff/SampleLoad/ACCT.json";
        final String filePath2 = "/Users/justin/Documents/javastuff/SampleLoad/CUST.json";


        // read the json file
        FileReader reader1 = null;
        FileReader reader2 = null;

        try {
            reader1 = new FileReader(filePath1);
            reader2 = new FileReader(filePath2);
        } catch (FileNotFoundException ex) {
            //ex.printStackTrace();
            ex.getMessage();
        }

        JSONParser jsonParser = new JSONParser();
        JsonTranscoder trans = new JsonTranscoder();

        String jsonString1 =  (String) jsonParser.parse(reader1).toString();
        String jsonString2 =  (String) jsonParser.parse(reader2).toString();

        JsonObject jsonObjA = null;
        JsonObject jsonObjCH = null;

        try {
            jsonObjA = trans.stringToJsonObject(jsonString1);
            jsonObjCH = trans.stringToJsonObject(jsonString2);
        } catch (Exception e) {
            e.printStackTrace();
        }

        //JsonObject jsonObjA = (JsonObject) bucket.get("A").content();
        //JsonObject jsonObjCH = (JsonObject) bucket.get("CH").content();

        for (int i=0; i < numDocs; i++) {

            final String chID = "CH" + "::" + i;

            //Element a3 and ch11 should be the same
            int Aelement3_CHelement11 = random.nextInt(9999);
            jsonObjCH.put("CH_element11", Aelement3_CHelement11);

            //DEBUG: System.out.println("A" + jsonObjA + "CH" + jsonObjCH);
            Random rand = new Random();

            //ch.element41=13
            Integer CHelement41 = rand.nextInt(99);
            jsonObjCH.put("CH_element41", CHelement41);

            //ch.element57="WNET"
            int CHelement57 = rand.nextInt(element57.length);
            jsonObjCH.put("CH_element57", CHelement57);

            System.out.println("CH" + jsonObjCH);

            final JsonObject finalJsonObjCH = jsonObjCH;
            Observable
                    .just(jsonObjCH)
                    .map(num -> {
                        return JsonDocument.create(chID, finalJsonObjCH);
                    })
                    .flatMap(doc -> {
                        return bucket
                                .async()
                                .upsert(doc)
                                // do retry for each op individually to not fail the full batch
                                .retryWhen(anyOf(BackpressureException.class).max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build())
                                .retryWhen(anyOf(TemporaryFailureException.class).max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build());
                    })
                    .toBlocking()
                    .last();

            //The sub document loop.
            //For every CH let's create 3000 A
            for (int x=0; x < subDocs; x++) {

                final String aID = "A" + "::" + i + "::" + x;
                //Common attribute between A and CH data types
                jsonObjA.put("A_element3", Aelement3_CHelement11);

                Double Aelement1 = rand.nextDouble();
                jsonObjA.put("A_element1", Aelement1);

                System.out.println("A" + jsonObjA);

                final JsonObject finalJsonObjA = jsonObjA;
                Observable
                        .just(jsonObjA)
                        .map(num -> {
                            return JsonDocument.create(aID, finalJsonObjA);
                        })
                        .flatMap(doc -> {
                            return bucket
                                    .async()
                                    .upsert(doc)
                                    // do retry for each op individually to not fail the full batch
                                    .retryWhen(anyOf(BackpressureException.class).max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build())
                                    .retryWhen(anyOf(TemporaryFailureException.class).max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build());
                        })
                        .toBlocking()
                        .last();

            }

        }

        long end = System.nanoTime();

        System.out.println("Bulk loading " + totalDocs + " docs took: " + TimeUnit.NANOSECONDS.toSeconds(end - start) + "s.");

    }
}
