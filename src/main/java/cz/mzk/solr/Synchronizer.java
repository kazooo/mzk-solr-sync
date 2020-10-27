package cz.mzk.solr;

import cz.mzk.configuration.AppConfiguration;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


@Component
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
        cursorFetch.from(lastCheckDate);

        while (!cursorFetch.done()) {
            try {
                SolrDocumentList docs = cursorFetch.next();
                List<SolrInputDocument> inputDocs = convert(docs);
                sendBuffer.add(inputDocs);
            } catch (IOException | SolrServerException e) {
                e.printStackTrace();
            }
        }

        cursorFetch.reset();
        lastCheckDate = new Date();
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
}
