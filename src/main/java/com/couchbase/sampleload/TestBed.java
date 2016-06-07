package com.couchbase.sampleload;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.ReplicaMode;
import com.couchbase.client.java.document.AbstractDocument;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.transcoder.JsonTranscoder;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Created by justin on 3/4/16.
 */
public class TestBed {

    private ArrayList<JsonDocument> docs;

    public static void main(String[] args) throws IOException, ParseException {

        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
                .queryEnabled(true)
                .dnsSrvEnabled(false)
                //.observeIntervalDelay()
                .build();
        CouchbaseCluster cluster = CouchbaseCluster.create(env, "192.168.61.101");
        Bucket bucket = cluster.openBucket("testload");

        // Merge operations
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
        JsonObject jsonObj = null;

        // The three structures created are the EVENT itself, the GUID to maintain the list and the token (aka vcard).
        final JsonDocument jsonDoc = JsonDocument.create("keyx", jsonObj);
        final StringDocument guidDoc = StringDocument.create("keyz", jsonObj.get("EVNT_ID").toString());
        final StringDocument vcardDoc = StringDocument.create("keyw", jsonObj.get("VCARD_ACCT_NO").toString());

        try {
            jsonObj = trans.stringToJsonObject(jsonString1);
        } catch (Exception e) {
            e.printStackTrace();
        }

        long startTime = System.nanoTime();

        //Handling Event insert
        Observable<JsonDocument> obsrv1 = Observable
                .just(jsonDoc)
                .flatMap(doc -> {
                    return bucket.async().upsert(doc);
                });

        long time1 = System.nanoTime();
        List<StringDocument> appendDocs = new ArrayList<StringDocument>();
        appendDocs.add(guidDoc);
        if(vcardDoc != null) {
            appendDocs.add(vcardDoc);
        }

        //Handling Append Only structures
        Observable<StringDocument> obsrv2 = Observable
                .from(appendDocs)
                .flatMap(new Func1<StringDocument, Observable<StringDocument>>() {
                    @Override
                    public Observable<StringDocument> call(
                            final StringDocument doc) {
                        return bucket
                                .async()
                                .append(doc)
                                .onErrorResumeNext(
                                        new Func1<Throwable, Observable<StringDocument>>() {
                                            @Override
                                            public Observable<StringDocument> call(
                                                    final Throwable exc) {
                                                if (exc instanceof DocumentDoesNotExistException) {
                                                    return bucket
                                                            .async()
                                                            .insert(doc);
                                                } else {
                                                    return Observable
                                                            .error(exc); // propagate errrors
                                                }
                                            }
                                        });
                    }
                });
        //Merging both the Observable to wait until the last one is complete.
        Observable.merge(obsrv1.subscribeOn(Schedulers.io()),obsrv2.subscribeOn(Schedulers.io())).toBlocking();

        // To create and maintain this structure if we passed them into an observable
        Observable
                .just(jsonDoc, guidDoc, vcardDoc)
                .flatMap(
                        new Func1<AbstractDocument, Observable<AbstractDocument>>() {
                            @Override
                            public Observable<AbstractDocument> call(
                                    final AbstractDocument doc) {
                                if (doc instanceof RawJsonDocument) {
                                    return bucket.async().upsert(doc);
                                } else {
                                    return bucket.async().append(doc)
                                            .onErrorResumeNext( exc -> {
                                                if (exc instanceof DocumentDoesNotExistException) {
                                                    return bucket.async().insert(doc);
                                                } else {
                                                    return Observable
                                                            .error(exc); // propagate other errors!
                                                }
                                            });
                                }
                            }
                        })
                .subscribe(document1 -> System.out.println("Got: " + document1.id()));

        // New
        // Method to create JsonDocument Observable. Notice that the call to Couchbase bucket is in Sync
        /*Observable<RawJsonDocument> createRawJsonObservable (final RawJsonDocument doc) {
            return Observable.create(new Observable.OnSubscribe<RawJsonDocument>() {

                public void call(Subscriber<? super RawJsonDocument> s) {
                // TODO Auto-generated method stub
                // simulate latency
                    try {
                        bucket.insert(doc);
                    } catch (Exception e) {
                        //e.printStackTrace();
                        s.onError(e);
                    }
                    s.onNext(doc);
                    s.onCompleted();
                }
            });
        }
        //Method to create JsonDocument Observable. Notice that the call to Couchbase bucket is in Sync
        private Observable<StringDocument> createStringObservable(final StringDocument doc) {
            return Observable.create(new OnSubscribe<StringDocument>() {

                public void call(Subscriber<? super StringDocument> s) {
                // TODO Auto-generated method stub
                // simulate latency
                    try {
                        bucket.append(doc);
                    } catch (Exception e) {
                        if(e instanceof DocumentDoesNotExistException) {
                            bucket.insert(doc);
                        } else {
                            s.onError(e);
                        }
                        //e.printStackTrace();
                    }
                    s.onNext(doc);
                    s.onCompleted();
                }
            });
        }

        //Following is the code to merge.
        Observable<StringDocument> obsrv1 = createJsonObservable(jsonDoc);
        Observable<StringDocument> obsrv2 = createStringObservable(guidDoc);
        Observable<StringDocument> obsrv3 = createStringObservable(vcardDoc);
        Observable.merge(obsrv1.subscribeOn(Schedulers.io()),
                obsrv2.subscribeOn(Schedulers.io()),
                obsrv3.subscribeOn(Schedulers.io())).toBlocking().forEach(new Action1<AbstractDocument>() {

            public void call(AbstractDocument arg0) {
                //System.out.println(arg0);

            }
            });*/

    }
}
