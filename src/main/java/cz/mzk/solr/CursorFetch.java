package cz.mzk.solr;

import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CursorMarkParams;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;


@Slf4j
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

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:sss'Z'");

    public CursorFetch(int r, SolrClient sc) {
        solrClient = sc;
        rows = r;
        reset();
    }

    public void from(Date lcd) {
        log.debug("Setup querying from " + lcd + "...");
        lastCheckDate = lcd;
        params = createCursorParameters();
    }

    public boolean done() {
        return done;
    }

    public void reset() {
        log.debug("Reset...");
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
            log.debug("Reached the end of cursor response...");
        }
        lastCursorMark = nextCursorMark;
        return response.getResults();
    }

    private SolrQuery createCursorParameters() {
        String query = "(" +
                MODIFIED_DATE_FIELD_NAME + ":[" + sdf.format(lastCheckDate) + " TO *] OR " +
                TIMESTAMP_FIELD_NAME + ":[" + sdf.format(lastCheckDate) + " TO *]" +
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
            log.debug("Close source Solr client...");
            solrClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
