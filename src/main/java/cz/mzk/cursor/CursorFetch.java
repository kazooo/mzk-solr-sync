package cz.mzk.cursor;

import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CursorMarkParams;

import java.io.IOException;


@Slf4j
public class CursorFetch {

    private boolean done;
    private SolrQuery params;
    private String lastCursorMark;
    private final SolrClient solrClient;

    public CursorFetch(SolrClient sc) {
        solrClient = sc;
        reset();
    }

    public void setParams(SolrQuery ps) {
        params = ps;
        reset();
    }

    public boolean done() {
        return done;
    }

    public void reset() {
        log.debug("Reset...");
        done = false;
        lastCursorMark = CursorMarkParams.CURSOR_MARK_START;
    }

    public SolrDocumentList next() throws IOException, SolrServerException {
        params.set(CursorMarkParams.CURSOR_MARK_PARAM, lastCursorMark);
        QueryResponse response = solrClient.query(params);
        String nextCursorMark = response.getNextCursorMark();
        if (lastCursorMark.equals(nextCursorMark)) {
            done = true;
            log.debug("Reached the end of cursor response...");
        }
        lastCursorMark = nextCursorMark;
        return response.getResults();
    }

    public void close() {
        try {
            log.debug("Close source Solr client...");
            solrClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
