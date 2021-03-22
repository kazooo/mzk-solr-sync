package cz.mzk.cursor;

import cz.mzk.util.SolrField;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocumentList;

import java.io.IOException;


public class RootDocCursor {

    private final CursorFetch cursorFetch;
    private final int rows;

    public RootDocCursor(SolrClient destSolrClient, int r) {
        cursorFetch = new CursorFetch(destSolrClient);
        cursorFetch.setParams(createCursorParameters());
        rows = r;
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

    private SolrQuery createCursorParameters() {
        SolrQuery params = new SolrQuery("*:*");
        params.addFilterQuery("{!frange l=1 u=1 v=eq(PID,root_pid)}");
        params.setSort(SolrQuery.SortClause.asc(SolrField.UUID));
        params.addField(SolrField.UUID);
        params.setRows(rows);
        return params;
    }
}
