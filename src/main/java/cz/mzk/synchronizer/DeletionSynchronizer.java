package cz.mzk.synchronizer;

import cz.mzk.configuration.AppConfiguration;
import cz.mzk.cursor.BookDocCursor;
import cz.mzk.cursor.RootDocCursor;
import cz.mzk.util.SolrField;
import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;


@Slf4j
@Component
public class DeletionSynchronizer {

    private final SolrClient srcSolrClient;
    private final SolrClient dstSolrClient;
    private final RootDocCursor dstRootDocCursor;
    private final BookDocCursor srcBookDocCursor;
    private final BookDocCursor dstBookDocCursor;

    public DeletionSynchronizer(AppConfiguration config) {
        int querySize = config.getQuerySize();
        srcSolrClient = config.getSrcSolrClient();
        dstSolrClient = config.getDstSolrClient();
        dstRootDocCursor = new RootDocCursor(dstSolrClient, querySize);
        srcBookDocCursor = new BookDocCursor(srcSolrClient, querySize);
        dstBookDocCursor = new BookDocCursor(dstSolrClient, querySize);
    }

    public void sync() {
        int removed = 0;
        while (!dstRootDocCursor.done()) {
            try {
                removed += dstRootDocCursor.next().stream()
                        .map(doc -> (String) doc.getFieldValue(SolrField.UUID))
                        .filter(rootUuid -> !numFoundIsEqual(rootUuid, srcSolrClient, dstSolrClient))
                        .mapToInt(this::syncBook).sum();
            } catch (IOException | SolrServerException e) {
                e.printStackTrace();
            }
        }
        commitDst();
        log.info("Finish synchronization of deletions, removed " + removed + " docs");
    }

    private int syncBook(String rootUuid) {
        Set<String> srcSet = collectUuidsByRoot(rootUuid, srcBookDocCursor);
        Set<String> dstSet = collectUuidsByRoot(rootUuid, dstBookDocCursor);
        dstSet.removeAll(srcSet);      // filter uuids that don't exist in the source
        remove(dstSet, dstSolrClient); // and remove them from the destination
        return dstSet.size();
    }

    private void remove(Set<String> removeSet, SolrClient client) {
        if (removeSet.isEmpty())
            return;
        try {
            client.deleteById(new ArrayList<>(removeSet));
        } catch (SolrServerException | IOException e) {
            e.printStackTrace();
        }
    }

    private void commitDst() {
        try {
            dstSolrClient.commit();
        } catch (SolrServerException | IOException e) {
            e.printStackTrace();
        }
    }

    private static Set<String> collectUuidsByRoot(String rootUuid, BookDocCursor bookDocCursor) {
        Set<String> result = new HashSet<>();
        bookDocCursor.forRoot(rootUuid);
        while (!bookDocCursor.done()) {
            try {
                bookDocCursor.next().stream()
                        .map(doc -> (String) doc.getFieldValue(SolrField.UUID))
                        .forEach(result::add);
            } catch (IOException | SolrServerException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    private boolean numFoundIsEqual(String rootUuid, SolrClient src, SolrClient dst) {
        try {
            long d = getCountByRoot(rootUuid, dst);
            long s = getCountByRoot(rootUuid, src);
            return d == s;
        } catch (IOException | SolrServerException e) {
            e.printStackTrace();
            return false;
        }
    }

    private long getCountByRoot(String rootUuid, SolrClient solrClient) throws IOException, SolrServerException {
        return getCount(solrClient, new SolrQuery(SolrField.ROOT_UUID + ":\"" + rootUuid + "\"").setRows(0));
    }

    private long getCount(SolrClient solrClient, SolrQuery params) throws IOException, SolrServerException {
        return solrClient.query(params).getResults().getNumFound();
    }

    @PreDestroy
    public void close() {
        log.debug("Close sources...");
        dstRootDocCursor.close();
        srcBookDocCursor.close();
        dstBookDocCursor.close();
    }
}
