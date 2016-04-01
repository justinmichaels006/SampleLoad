/* */
package com.couchbase.sampleload;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.endpoint.AbstractGenericHandler;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.deps.io.netty.channel.ConnectTimeoutException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.*;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.error.TemporaryFailureException;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.transcoder.JsonTranscoder;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import rx.Observable;
import rx.exceptions.CompositeException;
import rx.exceptions.OnErrorNotImplementedException;
import rx.functions.Action1;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.ConnectException;
import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.util.retry.RetryBuilder.anyOf;

public class LoadMe3 {

    public static void main(String[] args) throws IOException, ParseException {

        //Other notes to self ...
        //CouchbaseCluster cluster = CouchbaseCluster.create("192.168.61.101");
        /*CouchbaseConnectionFactoryBuilder builder = new CouchbaseConnectionFactoryBuilder();
        builder.setDaemon(true);
        builder.setObsPollInterval(1);*/

        CouchbaseEnvironment environment = DefaultCouchbaseEnvironment.builder()
                .queryEnabled(true)
                .dnsSrvEnabled(false)
                //.observeIntervalDelay()
                .build();
        CouchbaseCluster cluster = CouchbaseCluster.create(environment, "192.168.61.101");
        final Bucket bucket = cluster.openBucket("testload");

        int numDocs = 20000;
        long start = System.nanoTime();
        final int MAX_RETRIES = 20000;
        final int RETRY_DELAY = 50;
        final int MAX_DELAY = 1000;

        final String filePath1 = "/Users/justin/Documents/javastuff/SampleLoad/EventHistory.json";
        FileReader reader1 = null;

        try {
            reader1 = new FileReader(filePath1);
        } catch (FileNotFoundException ex) {
            //ex.printStackTrace();
            ex.getMessage();
        }

        JSONParser jsonParser = new JSONParser();
        JsonTranscoder trans = new JsonTranscoder();

        String jsonString1 = (String) jsonParser.parse(reader1).toString();

        JsonObject jsonObjEVENT = null;

        //List<String> documents = new ArrayList<String>();
        List<JsonDocument> docArray = new ArrayList<>();
        Random ran = new Random();
        final String AB = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        final String XY = "0123456789";
        char[] XX = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};
        SecureRandom rand = new SecureRandom();
        int len;
        String eventHistory = "evntHistory";

        for (int i = 0; i < numDocs; i++) {

            try {
                jsonObjEVENT = trans.stringToJsonObject(jsonString1);
            } catch (Exception e) {
                e.printStackTrace();
            }

            //evnt_id
            UUID eventID = UUID.randomUUID();
            jsonObjEVENT.put("EVNT_ID", i + "::" + eventID.toString());

            //wal_prov = 'AAA'
            String[] wallet = {"KY", "BBB", "AAA", "CCC", "DDD", "EEE", "FFF", "GGG", "HHH", "III",};
            String WAL_PROV = wallet[ran.nextInt(wallet.length)];
            //len = 3;
            //StringBuilder WAL_PROV = new StringBuilder(len);
            //for (int x = 0; x < len; x++) {
            //    WAL_PROV.append(AB.charAt(rand.nextInt(AB.length())));
            //}
            jsonObjEVENT.put("WAL_PROV", WAL_PROV.toString());

            //evnt_bts = 'datastamp'

            //proc_nm = 'AAA'
            String[] procurement = {"GOO", "PAY", "WAY", "BIP", "SNA", "BLU", "CYB", "MOL", "FOO", "ZOO",};
            String PROC_NM = procurement[ran.nextInt(procurement.length)];
            jsonObjEVENT.put("PROC_NM", PROC_NM);

            //card_acct_no = '9999999999999999' UUID?
            UUID CARD_ACCT_NO = UUID.randomUUID();
            jsonObjEVENT.put("CARD_ACCT_NO", CARD_ACCT_NO.toString());

            //card_seg_num = '99'
            len = 2;
            StringBuilder CARD_SEG_NUM = new StringBuilder(len);
            for (int r = 0; r < len; r++) {
                CARD_SEG_NUM.append(XY.charAt(rand.nextInt(XY.length())));
            }
            jsonObjEVENT.put("CARD_SEG_NUM", CARD_SEG_NUM.toString());

            //issuer_cd = '999'
            len = 3;
            StringBuilder ISSUER_CD = new StringBuilder(len);
            for (int r = 0; r < len; r++) {
                ISSUER_CD.append(XY.charAt(rand.nextInt(XY.length())));
            }
            jsonObjEVENT.put("ISSUER_CD", ISSUER_CD.toString());

            //evnt_nm = 'VRFYCARD' 'CHKCARD'
            String[] cardstate = {"TESTCARD", "WIPCARD", "PROCCARD", "VRFYCARD", "CHKCARD", "COMPCARD"};
            String EVNT_NM = cardstate[ran.nextInt(cardstate.length)];
            jsonObjEVENT.put("EVNT_NM", EVNT_NM);

            //evnt_bts = timestamp range
            String[] nowstamp = {"2016-02-01 23:59:59.999999", "2016-02-02 23:59:59.999999", "2016-02-03 23:59:59.999999",
                    "2016-02-04 23:59:59.999999", "2016-02-05 23:59:59.999999", "2016-02-06 23:59:59.999999",
                    "2016-02-07 23:59:59.999999", "2016-02-08 23:59:59.999999", "2016-02-09 23:59:59.999999",
                    "2016-02-10 23:59:59.999999", "2016-02-11 23:59:59.999999", "2016-02-12 23:59:59.999999",
                    "2016-02-13 23:59:59.999999", "2016-02-14 23:59:59.999999", "2016-02-15 23:59:59.999999",
                    "2016-02-16 23:59:59.999999", "2016-02-17 23:59:59.999999", "2016-02-18 23:59:59.999999",
                    "2016-02-19 23:59:59.999999", "2016-02-20 23:59:59.999999"};
            String EVNT_BTS = nowstamp[ran.nextInt(nowstamp.length)];
            jsonObjEVENT.put("EVNT_BTS", EVNT_BTS);

            //dev_id = 'ASDFLKJLKJSDLKJD'
            //vcard_acct_no = '123456789'

            docArray.add(i, JsonDocument.create(eventID.toString(), jsonObjEVENT));
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

        /*Observable
                .from(docArray)
                .flatMap(doc -> {
                    return bucket.async().append(StringDocument.create(eventHistory, doc.id().toString()))
                            .retryWhen(anyOf(BackpressureException.class).max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build())
                            .retryWhen(anyOf(TemporaryFailureException.class).max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build())
                            .onErrorResumeNext(ex -> {
                                if (ex instanceof DocumentDoesNotExistException) {
                                    return
                                            bucket.async().insert(StringDocument.create(eventHistory, doc.id().toString()));
                                } else {
                                    return
                                            Observable.error(ex); //propagate the other errors!
                                }
                            });
                }).toBlocking().subscribe(document2 -> System.out.println("Got: " + document2));*/

        long end = System.nanoTime();

        System.out.println("Bulk loading " + numDocs + " docs took: " + TimeUnit.NANOSECONDS.toSeconds(end - start) + "s.");

    }
}


