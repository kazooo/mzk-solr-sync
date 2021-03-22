package cz.mzk.solr;

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

    private static final String UUID_FIELD_NAME = "PID";
    private static final String MODEL_PATH_FIELD_NAME = "model_path";
    private static final String TIMESTAMP_FIELD_NAME = "timestamp";
    private static final String MODIFIED_DATE_FIELD_NAME = "modified_date";

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
                MODIFIED_DATE_FIELD_NAME + ":[" + sdf.format(lastCheckDate) + " TO *] OR " +
                TIMESTAMP_FIELD_NAME + ":[" + sdf.format(lastCheckDate) + " TO *]" +
                ")";
        query += " AND !(" +
                MODEL_PATH_FIELD_NAME + ":\"soundrecording\" OR " +
                MODEL_PATH_FIELD_NAME + ":\"soundunit\" OR " +
                MODEL_PATH_FIELD_NAME + ":\"sheetmusic\" OR " +
                MODEL_PATH_FIELD_NAME + ":\"track\"" +
                ")";
        SolrQuery params = new SolrQuery(query);
        params.setSort(SolrQuery.SortClause.asc(UUID_FIELD_NAME));
        params.setRows(rows);
        return params;
    }
}
