package cz.mzk.cursor;

import cz.mzk.util.SolrField;
import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocumentList;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;


@Slf4j
public class ModificationCursor {

    private final int rows;
    private final CursorFetch cursorFetch;

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:sss'Z'");

    public ModificationCursor(SolrClient sc, int r) {
        cursorFetch = new CursorFetch(sc);
        rows = r;
    }

    public void from(Date lcd) {
        log.debug("Setup querying from " + lcd + "...");
        cursorFetch.setParams(createCursorParameters(lcd));
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

    private SolrQuery createCursorParameters(Date lastCheckDate) {
        String query = "(" +
                SolrField.MODIFIED_DATE + ":[" + sdf.format(lastCheckDate) + " TO *] OR " +
                SolrField.TIMESTAMP + ":[" + sdf.format(lastCheckDate) + " TO *]" +
                ")";
        query += " AND !(" +
                SolrField.MODEL_PATH + ":\"soundrecording\" OR " +
                SolrField.MODEL_PATH + ":\"soundunit\" OR " +
                SolrField.MODEL_PATH + ":\"sheetmusic\" OR " +
                SolrField.MODEL_PATH + ":\"track\"" +
                ")";
        SolrQuery params = new SolrQuery(query);
        params.setSort(SolrQuery.SortClause.asc(SolrField.UUID));
        params.setRows(rows);
        return params;
    }
}
