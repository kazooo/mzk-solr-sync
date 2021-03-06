package cz.mzk.synchronizer;

import cz.mzk.configuration.AppConfiguration;
import cz.mzk.cursor.ModificationCursor;
import cz.mzk.util.SendBuffer;
import cz.mzk.util.SolrField;
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
public class ModificationSynchronizer {

    private final ModificationCursor modificationCursor;
    private final SendBuffer sendBuffer;
    private final Set<String> ignoredRoots;
    private Date lastCheckDate;

    private static final List<String> ignoredFieldNames = Collections.singletonList("_version_");

    public ModificationSynchronizer(AppConfiguration config) {
        ignoredRoots = config.getIgnoredRoots();
        ignoredRoots.forEach(uuid -> log.info("Ignore " + uuid));
        lastCheckDate = config.getLastModifiedDate();
        sendBuffer = new SendBuffer(config.getBufferSize(), config.getDstSolrClient());
        modificationCursor = new ModificationCursor(config.getSrcSolrClient(), config.getQuerySize());
    }

    public void sync() {
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
                log.debug("Transferred: " + transferred + " docs.");
            } catch (IOException | SolrServerException e) {
                e.printStackTrace();
            }
        }

        sendBuffer.empty();
        log.info("Transferred: " + transferred + " docs, last check date: " + lastCheckDate);
    }

    private List<SolrDocument> filter(SolrDocumentList docs) {
        return docs.stream()
                .filter(doc -> !ignoredRoots.contains((String) doc.getFieldValue(SolrField.ROOT_UUID)))
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
        if (!doc.containsKey(SolrField.DNNT)) {
            inputDoc.addField(SolrField.DNNT, true);
        }
        return inputDoc;
    }

    @PreDestroy
    public void close() {
        log.debug("Close sources...");
        modificationCursor.close();
        sendBuffer.close();
    }
}
