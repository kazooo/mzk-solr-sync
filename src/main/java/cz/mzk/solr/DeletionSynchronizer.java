package cz.mzk.solr;

import cz.mzk.configuration.AppConfiguration;
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

    private static final String ROOT_UUID_FILED_NAME = "root_pid";

    public DeletionSynchronizer(AppConfiguration config) {
        srcSolrClient = config.getSrcSolrClient();
        dstSolrClient = config.getDstSolrClient();
        dstRootDocCursor = new RootDocCursor(dstSolrClient, config.getQuerySize());
        srcBookDocCursor = new BookDocCursor(srcSolrClient, config.getQuerySize());
        dstBookDocCursor = new BookDocCursor(dstSolrClient, config.getQuerySize());
    }

    public void sync() {
        while (!dstRootDocCursor.done()) {
            try {
                dstRootDocCursor.next().stream()
                        .map(doc -> (String) doc.getFieldValue("PID"))
                        .forEach(rootUuid -> {
                            if (!numFoundIsEqual(rootUuid, srcSolrClient, dstSolrClient))
                                syncBook(rootUuid);
                        });
            } catch (IOException | SolrServerException e) {
                e.printStackTrace();
            }
        }
    }

    private void syncBook(String rootUuid) {
        Set<String> srcSet = collectUuidsByRoot(rootUuid, srcBookDocCursor);
        Set<String> dstSet = collectUuidsByRoot(rootUuid, dstBookDocCursor);
        dstSet.removeAll(srcSet);      // filter uuids that don't exist in the source
        remove(dstSet, dstSolrClient); // and remove them from the destination
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

    private static Set<String> collectUuidsByRoot(String rootUuid, BookDocCursor bookDocCursor) {
        Set<String> result = new HashSet<>();
        bookDocCursor.forRoot(rootUuid);
        while (!bookDocCursor.done()) {
            try {
                bookDocCursor.next().stream()
                        .map(doc -> (String) doc.getFieldValue("PID"))
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
        return getCount(solrClient, new SolrQuery(ROOT_UUID_FILED_NAME + ":\"" + rootUuid + "\"").setRows(0));
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
