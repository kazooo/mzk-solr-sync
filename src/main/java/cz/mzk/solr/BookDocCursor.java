package cz.mzk.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocumentList;

import java.io.IOException;


public class BookDocCursor {

    private final CursorFetch cursorFetch;
    private final int rows;

    private static final String UUID_FIELD_NAME = "PID";
    private static final String ROOT_UUID_FIELD_NAME = "root_uuid";

    public BookDocCursor(SolrClient solrClient, int r) {
        cursorFetch = new CursorFetch(solrClient);
        rows = r;
    }

    public void forRoot(String rootUuid) {
        cursorFetch.setParams(createCursorParameters(rootUuid));
    }

    public boolean done() {
        return cursorFetch.done();
    }

    public SolrDocumentList next() throws IOException, SolrServerException {
        return cursorFetch.next();
    }

    public void reset() {
        cursorFetch.reset();
    }

    public void close() {
        cursorFetch.close();
    }

    private SolrQuery createCursorParameters(String rootUuid) {
        SolrQuery params = new SolrQuery(ROOT_UUID_FIELD_NAME + ":\"" + rootUuid + "\"");
        params.addField(UUID_FIELD_NAME);
        params.setSort(SolrQuery.SortClause.asc(UUID_FIELD_NAME));
        params.setRows(rows);
        return params;
    }
}
