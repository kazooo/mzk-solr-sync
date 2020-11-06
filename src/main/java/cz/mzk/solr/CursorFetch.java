package cz.mzk.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CursorMarkParams;

import java.io.IOException;
import java.util.Date;


public class CursorFetch {

    private boolean done;
    private SolrQuery params;
    private Date lastCheckDate;
    private String lastCursorMark;

    private final int rows;
    private final SolrClient solrClient;

    private static final String UUID_FIELD_NAME = "PID";
    private static final String MODEL_PATH_FIELD_NAME = "model_path";
    private static final String TIMESTAMP_FIELD_NAME = "timestamp";
    private static final String MODIFIED_DATE_FIELD_NAME = "modified_date";

    public CursorFetch(int r, SolrClient sc) {
        solrClient = sc;
        rows = r;
        reset();
    }

    public void from(Date lcd) {
        lastCheckDate = lcd;
        params = createCursorParameters();
    }

    public boolean done() {
        return done;
    }

    public void reset() {
        done = false;
        lastCheckDate = null;
        lastCursorMark = CursorMarkParams.CURSOR_MARK_START;
    }

    public SolrDocumentList next() throws IOException, SolrServerException {
        params.set(CursorMarkParams.CURSOR_MARK_PARAM, lastCursorMark);
        QueryResponse response = solrClient.query(params);
        String nextCursorMark = response.getNextCursorMark();
        if (lastCursorMark.equals(nextCursorMark)) {
            done = true;
        }
        lastCursorMark = nextCursorMark;
        return response.getResults();
    }

    private SolrQuery createCursorParameters() {
        String query = "(" +
                MODIFIED_DATE_FIELD_NAME + ":[" + lastCheckDate + " TO *] OR " +
                TIMESTAMP_FIELD_NAME + ":[" + lastCheckDate + " TO *]" +
                ")";
        query += " AND !(" +
                MODEL_PATH_FIELD_NAME + ":\"map\" OR " +
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

    public void close() {
        try {
            solrClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
