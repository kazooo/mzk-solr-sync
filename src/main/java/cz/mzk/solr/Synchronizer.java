package cz.mzk.solr;

import cz.mzk.configuration.AppConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


@Component
@Slf4j
public class Synchronizer {

    private final CursorFetch cursorFetch;
    private final SendBuffer sendBuffer;
    private Date lastCheckDate;
    private static final List<String> ignoredFieldNames = Collections.singletonList("_version_");

    public Synchronizer(AppConfiguration config) {
        lastCheckDate = config.getLastModifiedDate();
        cursorFetch = new CursorFetch(config.getSrcSolrClient());
        sendBuffer = new SendBuffer(5_000, config.getDstSolrClient());
    }

    public void run() {
        long transferred = 0;
        cursorFetch.from(lastCheckDate);

        while (!cursorFetch.done()) {
            try {
                SolrDocumentList docs = cursorFetch.next();
                List<SolrInputDocument> inputDocs = convert(docs);
                transferred += inputDocs.size();
                sendBuffer.add(inputDocs);
            } catch (IOException | SolrServerException e) {
                e.printStackTrace();
            }
        }

        cursorFetch.reset();
        sendBuffer.empty();
        lastCheckDate = new Date();
        log.info("Transferred: " + transferred + " docs, last check date: " + lastCheckDate);
    }

    private List<SolrInputDocument> convert(SolrDocumentList docs) {
        return docs.stream().map(this::convert).collect(Collectors.toList());
    }

    private SolrInputDocument convert(SolrDocument doc) {
        SolrInputDocument inputDoc = new SolrInputDocument();
        for (Map.Entry<String, Object> pair : doc.entrySet()) {
            if (!ignoredFieldNames.contains(pair.getKey())) {
                inputDoc.addField(pair.getKey(), pair.getValue());
            }
        }
        return inputDoc;
    }

    @PreDestroy
    public void close() {
        cursorFetch.close();
        sendBuffer.close();
    }
}
