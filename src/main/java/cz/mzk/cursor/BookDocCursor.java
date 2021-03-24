package cz.mzk.cursor;

import cz.mzk.util.SolrField;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocumentList;

import java.io.IOException;


public class BookDocCursor {

    private final CursorFetch cursorFetch;
    private final int rows;

    public BookDocCursor(SolrClient solrClient, int r) {
        cursorFetch = new CursorFetch(solrClient);
        rows = r;
    }

    public void forRoot(String rootUuid) {
        cursorFetch.setParams(createCursorParameters(rootUuid, rows));
    }

    public boolean done() {
        return cursorFetch.done();
    }

    public SolrDocumentList next() throws IOException, SolrServerException {
        return cursorFetch.next();
    }

    public void close() {
        cursorFetch.close();
    }

    private SolrQuery createCursorParameters(String rootUuid, int rows) {
        SolrQuery params = new SolrQuery(SolrField.ROOT_UUID + ":\"" + rootUuid + "\"");
        params.setSort(SolrQuery.SortClause.asc(SolrField.UUID));
        params.addField(SolrField.UUID);
        params.setRows(rows);
        return params;
    }
}
