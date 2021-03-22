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

    private final ModificationCursor modificationCursor;
    private final SendBuffer sendBuffer;
    private final List<String> ignoredRoots;
    private Date lastCheckDate;

    private static final String DNNT_FIELD_NAME = "dnnt";
    private static final List<String> ignoredFieldNames = Collections.singletonList("_version_");

    public Synchronizer(AppConfiguration config) {
        ignoredRoots = config.getIgnoredRoots();
        ignoredRoots.forEach(uuid -> log.info("Ignore " + uuid));
        lastCheckDate = config.getLastModifiedDate();
        sendBuffer = new SendBuffer(config.getBufferSize(), config.getDstSolrClient());
        modificationCursor = new ModificationCursor(config.getSrcSolrClient(), config.getQuerySize());
    }

    public void run() {
        long transferred = 0;
        log.info("Last modified date: " + lastCheckDate + ", start synchronization...");
        modificationCursor.from(lastCheckDate);
        // next time check documents that
        // have been changed after the synchronization start
        lastCheckDate = new Date();

        while (!modificationCursor.done()) {
            try {
                List<SolrDocument> docs = filter(modificationCursor.next());
                log.debug("Got " + docs.size() + " documents...");
                List<SolrInputDocument> inputDocs = convert(docs);
                log.debug("Converted " + docs.size() + " documents...");
                transferred += inputDocs.size();
                sendBuffer.add(inputDocs);
            } catch (IOException | SolrServerException e) {
                e.printStackTrace();
            }
        }

        modificationCursor.reset();
        sendBuffer.empty();
        log.info("Transferred: " + transferred + " docs, last check date: " + lastCheckDate);
    }

    private List<SolrDocument> filter(SolrDocumentList docs) {
        return docs.stream()
                .filter(doc -> !ignoredRoots.contains((String) doc.getFieldValue("root_pid")))
                .collect(Collectors.toList());
    }

    private List<SolrInputDocument> convert(List<SolrDocument> docs) {
        return docs.stream().map(this::convert).collect(Collectors.toList());
    }

    private SolrInputDocument convert(SolrDocument doc) {
        SolrInputDocument inputDoc = new SolrInputDocument();
        for (Map.Entry<String, Object> pair : doc.entrySet()) {
            if (!ignoredFieldNames.contains(pair.getKey())) {
                inputDoc.addField(pair.getKey(), pair.getValue());
            }
        }
        inputDoc.addField(DNNT_FIELD_NAME, true);
        return inputDoc;
    }

    @PreDestroy
    public void close() {
        log.debug("Close sources...");
        modificationCursor.close();
        sendBuffer.close();
    }
}
