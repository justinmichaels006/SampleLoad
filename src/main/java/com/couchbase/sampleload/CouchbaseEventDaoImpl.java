package com.couchbase.sampleload;

import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonGenerationException;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.JsonMappingException;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.AbstractDocument;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.sun.xml.internal.ws.util.StringUtils;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action1;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.util.UUID;

public class CouchbaseEventDaoImpl {
    private static CouchbaseEventDaoImpl instance;
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

    public static CouchbaseEventDaoImpl getInstance() {
        if (instance == null) {
            instance = new CouchbaseEventDaoImpl();

        }
        return instance;
    }

    private CouchbaseEventDaoImpl() {
        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.create();
        try {
            cl1Cluster = CouchbaseCluster.create(env,"192.168.61.101");
            System.out.println("created cluster-cl1");
            cl1PocBucket = cl1Cluster.openBucket("testload");
            System.out.println("created event bucket-cl1");
            cl1EventHistoryBucket = cl1Cluster.openBucket("travel-sample");
            System.out.println("created event history bucket-cl1");
        } catch(Exception exc) {
            System.out.println("CL1 Error in constructor");
        }
        try {
            cl2Cluster = CouchbaseCluster.create(env, "192.168.61.102");
            System.out.println("created cluster-cl2");
            cl2PocBucket = cl2Cluster.openBucket("testload");
            System.out.println("created event bucket-cl2");
            cl2EventHistoryBucket = cl2Cluster.openBucket("travel-sample");
            System.out.println("created event history bucket-cl2");
        } catch(Exception exc) {
            System.out.println("CL2 Error in constructor");
        }
        // cl2EventHistoryBucket = cl2Cluster.openBucket("event_history");
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
     * @param async
     *            - Boolean true - Async
     * @param repl
     *            - Boolean true - replicateTo ONE
     * @throws JsonGenerationException
     * @throws JsonMappingException
     * @throws IOException
     */
    public void insertEvent(JsonObject event, String cluster, int ttl, boolean async, boolean repl)
            throws Exception {
        String primaryCluster;
        String failOverCluster;
        if (cl1.equals(cluster)) {
            primaryCluster = cl1;
            failOverCluster = cl2;
        } else {
            primaryCluster = cl2;
            failOverCluster = cl1;
        }
        if (async) {
            if (repl) {
                try {
                    insertEventAsyncRepl(event, primaryCluster, ttl);
                } catch (Exception exc) {
/*LogFacade.logError("CouchbasePOC", "insertEvent", "", "", "Exception occured connecting to Primary Cluster",
null, severity, error, entryURL, cardType, status);*/
                    System.out.println("Exception in primary CLuster" + exc.getMessage()
                            + " Switching to failOverCluster" + failOverCluster);// NOPMD
                    exc.printStackTrace();//NOPMD
                    insertEventAsyncRepl(event, failOverCluster, ttl);
                }
            } else {
                try {
                    insertEventAsync(event, primaryCluster, ttl);
                } catch (Exception exc) {
                    System.out.println("Exception in primary CLuster" + exc.getMessage()
                            + " Switching to failOverCluster" + failOverCluster);// NOPMD
                    try {
                        insertEventAsync(event, failOverCluster, ttl);
                    } catch(Exception ex) {
                        System.out.println("Got exception in failover cluster as well " + ex.getMessage());//NOPMD
                    }
                }
            }
        } else {
            try {
                insertEvent(event, primaryCluster, ttl);
            } catch (Exception exc) {
                System.out.println("Switching to failOverCluster" + failOverCluster);// NOPMD
                insertEvent(event, failOverCluster, ttl);
            }

        }

    }

    public void insertEvent(JsonObject event, String cluster, int ttl)
            throws JsonGenerationException, JsonMappingException, IOException {
        //String token = TraversePath.startComponentCall("insertEvent");
        String status = "S";
        try {
            ObjectMapper mapper = new ObjectMapper();
            String strEvent = mapper.writeValueAsString(event);
            // System.out.println("Event inserting ::" + strEvent);
            // System.out.println("eventID=" + event.getEventId());
            //String token1 = TraversePath.startComponentCall("createEvent");

            UUID eventID = UUID.randomUUID(); //Justin
            RawJsonDocument jsonDoc = RawJsonDocument.create("Event:" + eventID, ttl, strEvent);
            if (cl1.equals(cluster)) {
                cl1PocBucket.upsert(jsonDoc);
            } else {
                cl2PocBucket.upsert(jsonDoc);
            }

            //TraversePath.endComponentCall(token1, "createEvent", "1.0", "S", 0, "");
            StringBuilder guidStringValue = new StringBuilder(",");
            //guidStringValue.append(strEvent); //Justin
            //guidStringValue.append(":"); //Justin
            guidStringValue.append(eventID);

            try {
                //String key2 = TraversePath.startComponentCall("createGuid");
                if (cl1.equals(cluster)) {
                    cl1PocBucket
                            .append(StringDocument.create("Guid:" + eventID, ttl, guidStringValue.toString()));
                } else {
                    cl2PocBucket
                            .append(StringDocument.create("Guid:" + eventID, ttl, guidStringValue.toString()));
                }

                //TraversePath.endComponentCall(key2, "createGuid", "1.0", "S", 0, "");
            } catch (Exception e) {
                // if fails first insert the basic JSON first.
                //String key3 = TraversePath.startComponentCall("createGuidInsert");
                if (cl1.equals(cluster)) {
                    cl1PocBucket
                            .insert(StringDocument.create("Guid:" + eventID, ttl, guidStringValue.toString()));
                } else {
                    cl2PocBucket
                            .insert(StringDocument.create("Guid:" + eventID, ttl, guidStringValue.toString()));
                }

                //TraversePath.endComponentCall(key3, "createGuidInsert", "1.0", "S", 0, "");

            }

            /*if (!StringUtils.isEmpty(event.getVirtualCardNumber())) {

                if (!StringUtils.isEmpty(event.getVirtualCardNumber())) {
                    guidStringValue.append(":");
                    guidStringValue.append(event.getVirtualCardNumber());
                }

                try {
                    //String key4 = TraversePath.startComponentCall("createVCard");
                    if (cl1.equals(cluster)) {
                        cl1PocBucket.append(StringDocument.create("LatestEvent:" + event.getVirtualCardNumber(), ttl,
                                guidStringValue.toString()));
                    } else {
                        cl2PocBucket.append(StringDocument.create("LatestEvent:" + event.getVirtualCardNumber(), ttl,
                                guidStringValue.toString()));
                    }

                    //TraversePath.endComponentCall(key4, "createVCard", "1.0", "S", 0, "");
                } catch (Exception e) {
                    // if fails first insert the basic JSON first.
                    //String key5 = TraversePath.startComponentCall("createVCardInsert");
                    if (cl1.equals(cluster)) {
                        cl1PocBucket.insert(StringDocument.create("LatestEvent:" + event.getVirtualCardNumber(), ttl,
                                guidStringValue.toString()));
                    } else {
                        cl2PocBucket.insert(StringDocument.create("LatestEvent:" + event.getVirtualCardNumber(), ttl,
                                guidStringValue.toString()));
                    }

                    //TraversePath.endComponentCall(key5, "createVCardInsert", "1.0", "S", 0, "");
                }

            }*/

        } catch (Exception exc) {
            status = "F";
            exc.printStackTrace();
        } finally {
            //TraversePath.endComponentCall(token, "insertEvent", "1.0", status, 0, "cluster:" + cluster + ":" + ttl);
        }

    }



    public void insertEventAsync(JsonObject event, String cluster, int ttl)
            throws Exception {
        //String token = TraversePath.startComponentCall("insertEvent");
        String status = "S";
        try {
            ObjectMapper mapper = new ObjectMapper();
            String strEvent = mapper.writeValueAsString(event);

            UUID eventID = UUID.randomUUID(); //Justin
            RawJsonDocument jsonDoc = RawJsonDocument.create("Event:" + eventID, ttl, strEvent);

            StringBuilder guidStringValue = new StringBuilder();
            //guidStringValue.append(event.getEventName());
            //guidStringValue.append(":");
            guidStringValue.append(eventID);
            guidStringValue.append(",");
            StringDocument guidDoc = StringDocument.create("Guid:" + eventID, ttl, guidStringValue.toString());
            StringDocument vcardDoc = null;
            /*if (!StringUtils.isEmpty(event.getVirtualCardNumber())) {
                vcardDoc = StringDocument.create("VCard:" + event.getVirtualCardNumber(), ttl,
                        guidStringValue.toString());
            }*/

            long startTime = System.nanoTime();
// Handling Event insert
            Observable<RawJsonDocument> obsrv1 = null;
            if (cl1.equals(cluster)) {
                obsrv1 = createJsonObservableCl1(jsonDoc);
            } else {
                obsrv1 = createJsonObservableCl2(jsonDoc);
            }
            long time1 = System.nanoTime();

// Handling Append Only structures
            Observable<StringDocument> obsrv2 = null;
            if (cl1.equals(cluster)) {
                obsrv2 = createStringObservableCl1(guidDoc);
            } else {
                obsrv2 = createStringObservableCl2(guidDoc);
            }
// Handling VCard APpend
            if (vcardDoc != null) {
                Observable<StringDocument> obsrv3 = null;
                if (cl1.equals(cluster)) {
                    obsrv3 = createStringObservableCl1(vcardDoc);
                } else {
                    obsrv3 = createStringObservableCl2(vcardDoc);
                }
                Observable
                        .merge(obsrv1.subscribeOn(Schedulers.io()), obsrv2.subscribeOn(Schedulers.io()),
                                obsrv3.subscribeOn(Schedulers.io()))
                        .toBlocking().forEach(new Action1<AbstractDocument>() {

                    public void call(AbstractDocument arg0) {
// System.out.println(arg0);

                    }

                });
            } else {
                Observable.merge(obsrv1.subscribeOn(Schedulers.io()), obsrv2.subscribeOn(Schedulers.io())).toBlocking()
                        .forEach(new Action1<AbstractDocument>() {

                            public void call(AbstractDocument arg0) {
                                System.out.println(arg0);

                            }

                        });
            }

            long time2 = System.nanoTime();
// Merging both the Observable to wait until the last one is
// complete.
// Observable.merge(obsrv1,obsrv2).last().toBlocking().single();

            long finalTime = System.nanoTime();
            StringBuilder userDef = new StringBuilder("replicate" + String.valueOf(startTime));
            userDef.append(":");
            userDef.append((time1 - startTime) / 1000);
            userDef.append(":");
            userDef.append((time2 - startTime) / 1000);
            userDef.append(":");
            userDef.append((finalTime - startTime) / 1000);
            userDef.append(":");
            userDef.append("Cluster");
            userDef.append(cluster);
            userDef.append(":ttl");
            userDef.append(ttl);

            //TraversePath.endComponentCall(token, "insertEventAsync", "1.0", status, 0, userDef.toString());

// System.out.println(startTime + ":" + (startTime - time1)/1000 +
// ":" + (startTime - time2)/1000 + ":" + (startTime -
// finalTime)/1000);

        } catch (Exception exc) {
            status = "F";
            exc.printStackTrace();
            throw exc;
        } finally {
            //TraversePath.endComponentCall(token, "insertEvent", "1.0", status, 0, "CLuster" + cluster + ":ttl" + ttl);
        }

    }

    public void insertEventAsyncRepl(JsonObject event, String cluster, int ttl)
            throws Exception {
        //String token = TraversePath.startComponentCall("insertEvent");
        String status = "S";
        try {
            ObjectMapper mapper = new ObjectMapper();
            String strEvent = mapper.writeValueAsString(event);

            UUID eventID = UUID.randomUUID(); //Justin
            RawJsonDocument jsonDoc = RawJsonDocument.create("Event:" + eventID, ttl, strEvent);

            StringBuilder guidStringValue = new StringBuilder();
            //guidStringValue.append(event.getEventName());
            //guidStringValue.append(":");
            guidStringValue.append(eventID);
            guidStringValue.append(",");
            StringDocument guidDoc = StringDocument.create("Guid:" + eventID, ttl, guidStringValue.toString());
            StringDocument vcardDoc = null;
            /*if (!StringUtils.isEmpty(event.getVirtualCardNumber())) {
                vcardDoc = StringDocument.create("VCard:" + event.getVirtualCardNumber(), ttl,
                        guidStringValue.toString());
            }*/

            long startTime = System.nanoTime();
// Handling Event insert
            Observable<RawJsonDocument> obsrv1 = null;
            if (cl1.equals(cluster)) {
                obsrv1 = createJsonObservableReplCl1(jsonDoc);
            } else {
                obsrv1 = createJsonObservableReplCl2(jsonDoc);
            }
            long time1 = System.nanoTime();

// Handling Append Only structures
            Observable<StringDocument> obsrv2 = null;
            if (cl1.equals(cluster)) {
                obsrv2 = createStringObservableReplCl1(guidDoc);
            } else {
                obsrv2 = createStringObservableReplCl2(guidDoc);
            }
// Handling VCard APpend
            if (vcardDoc != null) {
                Observable<StringDocument> obsrv3 = null;
                if (cl1.equals(cluster)) {
                    obsrv3 = createStringObservableReplCl1(vcardDoc);
                } else {
                    obsrv3 = createStringObservableReplCl2(vcardDoc);
                }

                Observable
                        .merge(obsrv1.subscribeOn(Schedulers.io()), obsrv2.subscribeOn(Schedulers.io()),
                                obsrv3.subscribeOn(Schedulers.io()))
                        .toBlocking().forEach(new Action1<AbstractDocument>() {

                    public void call(AbstractDocument arg0) {
// System.out.println(arg0);

                    }

                });
            } else {
                Observable.merge(obsrv1.subscribeOn(Schedulers.io()), obsrv2.subscribeOn(Schedulers.io())).toBlocking()
                        .forEach(new Action1<AbstractDocument>() {

                            public void call(AbstractDocument arg0) {
// System.out.println(arg0);

                            }

                        });
            }

            long time2 = System.nanoTime();
// Merging both the Observable to wait until the last one is
// complete.
// Observable.merge(obsrv1,obsrv2).last().toBlocking().single();

            long finalTime = System.nanoTime();
            StringBuilder userDef = new StringBuilder("replicate" + String.valueOf(startTime));
            userDef.append(":");
            userDef.append((time1 - startTime) / 1000);
            userDef.append(":");
            userDef.append((time2 - startTime) / 1000);
            userDef.append(":");
            userDef.append((finalTime - startTime) / 1000);
            userDef.append(":");
            userDef.append("Cluster");
            userDef.append(cluster);
            userDef.append(":ttl");
            userDef.append(ttl);

            //TraversePath.endComponentCall(token, "insertEventAsyncRepl", "1.0", status, 0, userDef.toString());

// System.out.println(startTime + ":" + (startTime - time1)/1000 +
// ":" + (startTime - time2)/1000 + ":" + (startTime -
// finalTime)/1000);

        } catch (Exception exc) {
            status = "F";
//exc.printStackTrace();
            throw exc;
        } finally {
            //TraversePath.endComponentCall(token, "insertEvent", "1.0", status, 0, "CLuster" + cluster + ":ttl" + ttl);
        }

    }


    // Creating Observables for Cl1 cluster
    private Observable<RawJsonDocument> createJsonObservableCl1(final RawJsonDocument doc) {
        return Observable.create(new Observable.OnSubscribe<RawJsonDocument>() {

            public void call(Subscriber<? super RawJsonDocument> s) {

                try {
                    cl1PocBucket.insert(doc);
                } catch (Exception e) {
//System.out.println("got error in observable cl1");
                    e.printStackTrace();
                    s.onError(e);
//Observable.error(e);
                }
                s.onNext(doc);
                s.onCompleted();
            }
        });
    }

    private Observable<StringDocument> createStringObservableCl1(final StringDocument doc) {
        return Observable.create(new Observable.OnSubscribe<StringDocument>() {

            public void call(Subscriber<? super StringDocument> s) {

                try {
                    cl1PocBucket.append(doc);
                } catch (Exception e) {
                    if (e instanceof DocumentDoesNotExistException) {
                        cl1PocBucket.insert(doc);
                    } else {
                        e.printStackTrace();
//s.onError(e);
                        Observable.error(e);
                    }
// e.printStackTrace();

                }
                s.onNext(doc);
                s.onCompleted();
            }
        });
    }

    private Observable<RawJsonDocument> createJsonObservableReplCl1(final RawJsonDocument doc) {
        return Observable.create(new Observable.OnSubscribe<RawJsonDocument>() {

            public void call(Subscriber<? super RawJsonDocument> s) {

                try {
                    cl1PocBucket.insert(doc, ReplicateTo.ONE);
                } catch (Exception e) {
//e.printStackTrace();
                    s.onError(e);
//Observable.error(e);
                }
                s.onNext(doc);
                s.onCompleted();
            }
        });
    }

    private Observable<StringDocument> createStringObservableReplCl1(final StringDocument doc) {
        return Observable.create(new Observable.OnSubscribe<StringDocument>() {

            public void call(Subscriber<? super StringDocument> s) {

                try {
                    cl1PocBucket.append(doc, ReplicateTo.ONE);
                } catch (Exception e) {
                    if (e instanceof DocumentDoesNotExistException) {
                        cl1PocBucket.insert(doc, ReplicateTo.ONE);
                    } else {
//e.printStackTrace();
                        s.onError(e);
//Observable.error(e);
                    }
// e.printStackTrace();

                }
                s.onNext(doc);
                s.onCompleted();
            }
        });
    }

    // Creating Observables for Cl2 cluster
    private Observable<RawJsonDocument> createJsonObservableCl2(final RawJsonDocument doc) {
        return Observable.create(new Observable.OnSubscribe<RawJsonDocument>() {

            public void call(Subscriber<? super RawJsonDocument> s) {

                try {
                    cl2PocBucket.insert(doc);
                } catch (Exception e) {
//System.out.println("Got error in observable cl2");
//e.printStackTrace();
                    s.onError(e);
//Observable.error(e);
                }
                s.onNext(doc);
                s.onCompleted();
            }
        });
    }

    private Observable<StringDocument> createStringObservableCl2(final StringDocument doc) {
        return Observable.create(new Observable.OnSubscribe<StringDocument>() {

            public void call(Subscriber<? super StringDocument> s) {

                try {
                    cl2PocBucket.append(doc);
                } catch (Exception e) {
                    if (e instanceof DocumentDoesNotExistException) {
                        cl2PocBucket.insert(doc);
                    } else {
//e.printStackTrace();
                        s.onError(e);
//Observable.error(e);
                    }
// e.printStackTrace();

                }
                s.onNext(doc);
                s.onCompleted();
            }
        });
    }

    private Observable<RawJsonDocument> createJsonObservableReplCl2(final RawJsonDocument doc) {
        return Observable.create(new Observable.OnSubscribe<RawJsonDocument>() {

            public void call(Subscriber<? super RawJsonDocument> s) {

                try {
                    cl2PocBucket.insert(doc, ReplicateTo.ONE);
                } catch (Exception e) {
//e.printStackTrace();
                    s.onError(e);
//Observable.error(e);
                }
                s.onNext(doc);
                s.onCompleted();
            }
        });
    }

    private Observable<StringDocument> createStringObservableReplCl2(final StringDocument doc) {
        return Observable.create(new Observable.OnSubscribe<StringDocument>() {

            public void call(Subscriber<? super StringDocument> s) {

                try {
                    cl2PocBucket.append(doc, ReplicateTo.ONE);
                } catch (Exception e) {
                    if (e instanceof DocumentDoesNotExistException) {
                        cl2PocBucket.insert(doc, ReplicateTo.ONE);
                    } else {
//e.printStackTrace();
                        s.onError(e);
//Observable.error(e);
                    }
// e.printStackTrace();

                }
                s.onNext(doc);
                s.onCompleted();
            }
        });
    }

    private Bucket getEventHistoryBucket(String cluster) {
        if(cl2.equals(cluster)) {
            return cl2EventHistoryBucket;
        } else {
            return cl1EventHistoryBucket;
        }
    }


}
