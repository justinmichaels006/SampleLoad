package com.couchbase.sampleload;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.lang.Tuple2;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.*;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.error.TemporaryFailureException;
import com.couchbase.client.java.query.*;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import com.couchbase.client.java.transcoder.JsonTranscoder;
import com.couchbase.client.java.transcoder.Transcoder;
import com.couchbase.client.java.view.OnError;
import com.couchbase.client.java.view.ViewRow;
import com.sun.xml.internal.fastinfoset.util.StringArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

import javax.management.Query;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.util.retry.RetryBuilder.anyOf;

public class AppendAndPage {

    public static void main(String[] args) throws IOException, ParseException {

        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
                .queryEnabled(true)
                .dnsSrvEnabled(false)
                //.observeIntervalDelay()
                .build();
        CouchbaseCluster cluster = CouchbaseCluster.create(env, "192.168.61.101");
        Bucket bucket = cluster.openBucket("testload");

        // A single GUID would capture an array of events.
        // Each event_id would represent a unique document in the bucket.
        List<StringDocument> docArray = new ArrayList<>();
        StringArray strArray = new StringArray();

        String event_namex_event_idx = "event_name123:event_id123";
        String guid_GUIDx = "GUID123";
        String eventHistory = "evntHistory";
        int numEvents = 1000;

        Statement selectstmt = select("META(testload).id")
                .from("testload")
                .where("WAL_PROV = 'KY' OR PROC_NM = 'WAY'")
                .limit(20);
        //.offset(20);
        N1qlParams ryow = N1qlParams.build().consistency(ScanConsistency.NOT_BOUNDED);
        N1qlQueryResult q = bucket.query(N1qlQuery.simple(selectstmt, ryow));

        int aNum = q.info().resultCount();
        int pageSize = 5;
        List<String> listString = new ArrayList<String>();

        for (N1qlQueryRow s : q) {
            listString.add(s + "\n");
        }

        for (int x = 0; x < aNum; x += pageSize) {
            System.out.println("-----NEXT 5 of------" + aNum);
            for (int y = 0; y < pageSize; y++) {
                System.out.println(listString.get(x + y));
            }
        }

        bucket.query(N1qlQuery.simple(select("*").from("beer-sample").limit(10)))
                        .forEach(row -> System.out.println(row.value()));

        bucket.query(N1qlQuery.simple("SELECT Name, SUM(Count) FROM `default` GROUP BY Name HAVING SUM(Count) > 5000"))
                .forEach(System.out::println);

        bucket.query(N1qlQuery.simple(select("Name", "SUM(Count)").from("default").groupBy("Name")))
                .forEach(System.out::println);
        //Some other options
        //Iterator<N1qlQueryRow> listIterate = q.rows(); ... System.out.println(listIterate.next());
        //System.out.println(listString.get(i));
        //System.out.println(listString.stream().skip(z).limit(w).collect(Collectors.toCollection(ArrayList::new)));

    }
}
