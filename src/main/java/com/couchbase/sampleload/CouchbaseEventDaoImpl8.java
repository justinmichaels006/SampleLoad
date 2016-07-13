package com.couchbase.sampleload;

        import java.io.IOException;
        import java.util.UUID;
        import java.util.concurrent.TimeUnit;

        import com.couchbase.client.deps.com.fasterxml.jackson.databind.JsonMappingException;
        import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
        import com.couchbase.client.java.document.json.JsonObject;
        import rx.Observable;
        import rx.schedulers.Schedulers;

        import com.couchbase.client.core.time.Delay;
        import com.couchbase.client.java.AsyncBucket;
        import com.couchbase.client.java.AsyncCluster;
        import com.couchbase.client.java.Bucket;
        import com.couchbase.client.java.CouchbaseAsyncCluster;
        import com.couchbase.client.java.ReplicateTo;
        import com.couchbase.client.java.document.RawJsonDocument;
        import com.couchbase.client.java.document.StringDocument;
        import com.couchbase.client.java.error.DocumentDoesNotExistException;
        import static com.couchbase.client.java.util.retry.RetryBuilder.any;

public class CouchbaseEventDaoImpl8 {
    private static CouchbaseEventDaoImpl8 instance;
    // Cluster 1 Objects
    private AsyncCluster cl1Cluster;
    private AsyncBucket cl1PocBucket;
    private Bucket cl1EventHistoryBucket;

    // Cluster 2 Objects 10.16.144.198
    private AsyncCluster cl2Cluster;
    private AsyncBucket cl2PocBucket;
//private Bucket cl2EventHistoryBucket;

    private String cl1 = "cl1";
    private String cl2 = "cl2";

//Constants used during retry logic. 
    final int MAX_RETRIES = 20000;
    final int RETRY_DELAY = 50;
    final int MAX_DELAY = 1000;

    public static CouchbaseEventDaoImpl8 getInstance() {
        if (instance == null) {
            instance = new CouchbaseEventDaoImpl8();

        }
        return instance;
    }

    private CouchbaseEventDaoImpl8() {
        try {
            cl1Cluster = CouchbaseAsyncCluster.create("192.168.61.101");

            cl1PocBucket = cl1Cluster.openBucket("testload").retryWhen(
                    any().max(MAX_RETRIES).delay(
                            Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build()).toBlocking().first();
        } catch(Exception exc) {
            System.out.println("Failure in creating CL1 cluster " + exc.getMessage());
        }

        try {
            cl2Cluster = CouchbaseAsyncCluster.create("192.168.61.102");
            cl2PocBucket = cl2Cluster.openBucket("testload").retryWhen(
                    any().max(MAX_RETRIES).delay(
                            Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build()).toBlocking().first();
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
     *            - The event Object
     * @param cluster
     *            - The Primary couchbase cluster
     * @param ttl
     *            - Document expiry in seconds
     * @param async
     *            - Boolean true - Async
     * @param repl
     *            - Boolean true - replicateTo ONE
     //* @throws JsonGenerationException
     * @throws JsonMappingException
     * @throws IOException
     */
    public void insertEvent(JsonObject event, String cluster, int ttl, boolean async, boolean repl) {
            //throws JsonGenerationException, JsonMappingException, IOException {
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
                    System.out.println("Switching to failOverCluster8" + failOverCluster);// NOPMD
                    insertEventAsyncRepl(event, failOverCluster, ttl);
                }
            } else {
                try {
                    insertEventAsync(event, primaryCluster, ttl);
                } catch (Exception exc) {
                    System.out.println("Switching to failOverCluster8" + failOverCluster);// NOPMD
                    insertEventAsync(event, failOverCluster, ttl);
                }
            }
        } else {
            System.out.println("Switching to failOverCluster" + failOverCluster);
            }

        }

    public void insertEventAsync(JsonObject event, String cluster, int ttl) {
            //throws JsonGenerationException, JsonMappingException, IOException {
        //String token = TraversePath.startComponentCall("insertEvent");
        //String status = "S";
        try {
            ObjectMapper mapper = new ObjectMapper();
            String strEvent = mapper.writeValueAsString(event);
            UUID eventID = UUID.randomUUID(); //Justin

            RawJsonDocument jsonDoc = RawJsonDocument.create("Event:" + eventID, ttl, strEvent);

            StringBuilder guidStringValue = new StringBuilder();
            guidStringValue.append(eventID);
            guidStringValue.append(":");
            guidStringValue.append(event.toString());
            guidStringValue.append(",");
            StringDocument guidDoc = StringDocument.create("Guid:" + event, ttl, guidStringValue.toString());

            long startTime = System.nanoTime();
// Handling Event insert
            Observable<RawJsonDocument> obsrv1 = getEventBucket(cluster).insert(jsonDoc);
            long time1 = System.nanoTime();

// Handling Append Only structures
// Appending Guid Document
            Observable<StringDocument> obsrv2 = getEventBucket(cluster).append(guidDoc)
                    .onErrorResumeNext(exc -> {
                        if (exc instanceof DocumentDoesNotExistException) {
                            return getEventBucket(cluster).insert(guidDoc);
                        } else {
                            return Observable.error(exc);
                        }
                    });

             Observable.merge(obsrv1.subscribeOn(Schedulers.io()), obsrv2.subscribeOn(Schedulers.io())).toBlocking()
                        .forEach(action -> {
                        });

            long time2 = System.nanoTime();
// Merging both the Observable to wait until the last one is
// complete.
// Observable.merge(obsrv1,obsrv2).last().toBlocking().single();

            long finalTime = System.nanoTime();
            StringBuilder userDef = new StringBuilder("8replicate" + String.valueOf(startTime));
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

        } catch (Exception exc) {
            exc.printStackTrace();
        }

    }

    public void insertEventAsyncRepl(JsonObject event, String cluster, int ttl) {
            //throws JsonGenerationException, JsonMappingException, IOException {
        //String token = TraversePath.startComponentCall("insertEvent");
        //String status = "S";
        try {
            ObjectMapper mapper = new ObjectMapper();
            String strEvent = mapper.writeValueAsString(event);
            UUID eventID = UUID.randomUUID(); //Justin

            RawJsonDocument jsonDoc = RawJsonDocument.create("Event:" + eventID, ttl, strEvent);

            StringBuilder guidStringValue = new StringBuilder();
            guidStringValue.append(eventID);
            guidStringValue.append(":");
            guidStringValue.append(event.toString());
            guidStringValue.append(",");
            StringDocument guidDoc = StringDocument.create("Guid:" + eventID, ttl, guidStringValue.toString());

            long startTime = System.nanoTime();
// Handling Event insert
            Observable<RawJsonDocument> obsrv1 = getEventBucket(cluster).insert(jsonDoc, ReplicateTo.ONE);
            long time1 = System.nanoTime();

// Handling Append Only structures
            Observable<StringDocument> obsrv2 = getEventBucket(cluster).append(guidDoc, ReplicateTo.ONE)
                    .onErrorResumeNext(exc -> {
                        if (exc instanceof DocumentDoesNotExistException) {
                            return getEventBucket(cluster).insert(guidDoc, ReplicateTo.ONE);
                        } else {
                            return Observable.error(exc);
                        }
                    });

            Observable.merge(obsrv1.subscribeOn(Schedulers.io()), obsrv2.subscribeOn(Schedulers.io())).toBlocking()
                        .forEach(action -> {
                        });

            long time2 = System.nanoTime();
// Merging both the Observable to wait until the last one is
// complete.
// Observable.merge(obsrv1,obsrv2).last().toBlocking().single();

            long finalTime = System.nanoTime();
            StringBuilder userDef = new StringBuilder("8replicate" + String.valueOf(startTime));
            userDef.append(":");
            userDef.append("time1::" + (time1 - startTime)); // 1000);
            userDef.append(":");
            userDef.append("time2::" + (time2 - time1)); // 1000);
            userDef.append(":");
            userDef.append("final::" + (finalTime - startTime)); // 1000);
            userDef.append(":");
            userDef.append("Cluster");
            userDef.append(cluster);
            userDef.append(":ttl");
            userDef.append(ttl);

            //Time profile //Justin
            System.out.println(userDef);

            //TraversePath.endComponentCall(token, "insertEventAsyncRepl8", "1.0", status, 0, userDef.toString());

// System.out.println(startTime + ":" + (startTime - time1)/1000 +
// ":" + (startTime - time2)/1000 + ":" + (startTime -
// finalTime)/1000);

        } catch (Exception exc) {
            exc.printStackTrace();
        }

    }


    /*public Event insertReadEventAsyncRepl(Event event, String cluster, int ttl)
            throws JsonGenerationException, JsonMappingException, IOException {
        String token = TraversePath.startComponentCall("insertReadEventAsynRepl");
        String status = "S";
        Event retEvent = null;
        try {
            insertEventAsyncRepl(event, cluster, ttl);

            retEvent = fetchEventsByEventId(event.getEventId(), cluster);
            if (!retEvent.getEventId().equals(event.getEventId())) {
                throw new Exception("Event Ids do not match");
            }

        } catch (Exception exc) {
            status = "F";
            exc.printStackTrace();
        } finally {
            TraversePath.endComponentCall(token, "insertReadEventAsynRepl", "1.0", status, 0,
                    "CLuster" + cluster + ":ttl" + ttl);
        }
        return retEvent;

    }*/

    /*public List<Event> fetchEventsByGuid(String guid, String cluster) throws Exception {

// Bucket pocBucket = cluster.openBucket("event");
        StringDocument stringDocument = getEventBucket(cluster).get("Guid:" + guid, StringDocument.class).toBlocking().first();

// System.out.println("fetch Events::" + stringDocument);
        List<Event> events = new ArrayList<Event>();
        String document = stringDocument.content();
        document = document.substring(1, document.length());
        String[] contentAfterSplit = document.split(",");
        for (String content : contentAfterSplit) {
            String[] eventIds = content.split(":");

            events.add(fetchEventsByEventId(eventIds[1], cluster));

        }

        return events;

    }*/

    /*public Event fetchEventsByEventId(String eventId, String cluster) throws Exception {

        String token = TraversePath.startComponentCall("fetchEventsByEventId");
        String status = "S";
        Event retEvent = null;
        try {

// Bucket pocBucket = cluster.openBucket("event");
            JsonDocument jsonDocument = getEventBucket(cluster).get("Event:" + eventId).toBlocking().first();

            retEvent = new ObjectMapper().readValue(jsonDocument.content().toString(), Event.class);
// System.out.println("fetch Events with eventId::" +
// event.toString());

        } catch (Exception exc) {
            status = "F";
            exc.printStackTrace();
        } finally {
            TraversePath.endComponentCall(token, "fetchEventsByEventId", "1.0", status, 0, "cluster:" + cluster);
        }
        return retEvent;

    }*/

    /*public String fetchEventsByVirtualCard(String virtualCard, String cluster) {

// Bucket pocBucket = cluster.openBucket("event");
        StringDocument stringDocument = getEventBucket(cluster).get("LatestEvent:" + virtualCard, StringDocument.class).toBlocking().first();
        String content = stringDocument.content();
        String[] contentAfterSplit = content.split(",");
        String lastAppended = contentAfterSplit[contentAfterSplit.length - 1];

        String[] splitWithColon = lastAppended.split(":");
        String eventIdFromStringDocument = splitWithColon[1];
        System.out.println("eventId from virtualCard : " + eventIdFromStringDocument);

        return eventIdFromStringDocument;

    }
*/
    /*public int queryEventHistory(String acctNo, String panSeqNo, String issuerCd, String walProv) {

// .limit(5)
// .offset(20);
        String token = TraversePath.startComponentCall("queryEventHistory");
        StringBuilder sbWhere = new StringBuilder();
        sbWhere.append("CARD_ACCT_NO=");
        sbWhere.append(acctNo);
        sbWhere.append(" and CARD_SEQ_NUM = '");
        sbWhere.append(panSeqNo);
        sbWhere.append("' and ISSUER_CD='");
        sbWhere.append(issuerCd);
        if (walProv != null) {
            sbWhere.append("' and WAL_PROV='");
            sbWhere.append(walProv);
        }

        sbWhere.append("' ORDER BY STR_TO_MILLIS(EVNT_BTS) DESC");
        sbWhere.append(" LIMIT 10 OFFSET 10");
        System.out.println(sbWhere);
        Statement selectstmt = select("*").from("event_history").where(sbWhere.toString());
// N1qlParams ryow =
// N1qlParams.build().consistency(ScanConsistency.NOT_BOUNDED);
        N1qlParams ryow = N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS);
        N1qlQueryResult q = cl1EventHistoryBucket.query(N1qlQuery.simple(selectstmt, ryow));
        int resultCount = q.info().resultCount();
        TraversePath.endComponentCall(token, "queryEventHistory", "1.0", "S", 0, acctNo + ":" + resultCount);
// System.out.println(q);
        return resultCount;
// System.out.println("result count" + aNum);
// int pageSize = 5;
*//*
* List<JsonObject> listResults = new ArrayList<JsonObject>();
* 
* for (N1qlQueryRow s : q)
* 
* { System.out.println(s); listResults.add(s.value()); }
*//*

*//*
* for (int x = 0; x < aNum; x+=pageSize) { for (int y = 0; y <
* pageSize; y++) { System.out.println(listString.get(x+y)); }
* System.out.println("-----NEXT 5 of------" + aNum); }
*//*
// System.out.println(listResults);
    }*/

    /**
     * Method returns a Couchbase Event bucket based on the Cluster
     * 
     * @param cluster
     * @return Couchbase Bucket
     */
    private AsyncBucket getEventBucket(String cluster) {
        if (cl2.equals(cluster)) {
            return cl2PocBucket;
        } else {
            return cl1PocBucket;
        }
    }

    /*public static void main(String args[]) throws Exception {
        CouchbaseEventDaoImpl8 dao = CouchbaseEventDaoImpl8.getInstance();
        JSONObject event = new JSONObject();
        event.setEventId("eventinsertread1testasync");
        event.setEventName("eventinsertread1");
        event.setGuid("guideventinsertread1");
        event.setVirtualCardNumber("vcardeventinsertread1");
        dao.insertEvent(event, "cl1", 3600,true,false);

    }*/

}
