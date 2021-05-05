package cz.mzk;

import cz.mzk.configuration.AppConfiguration;
import cz.mzk.synchronizer.ModificationSynchronizer;
import cz.mzk.util.SolrField;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ModificationSyncIntegrationTest {

    @Mock
    AppConfiguration configuration;

    private static final int totalDocAmount = 100;

    private TestSolrServer srcSolr;
    private TestSolrServer dstSolr;

    @Before
    public void setupConfigMock() throws IOException, SolrServerException {
        srcSolr = Mockito.spy(TestSolrServer.buildAndInit("src/test/resources/src_solr", "test_src_core"));
        dstSolr = Mockito.spy(TestSolrServer.buildAndInit("src/test/resources/dst_solr", "test_dst_core"));

        List<SolrInputDocument> inputDocs = TestSolrServer.generateRandomDocs(totalDocAmount);

        Instant now = Instant.now();
        for (SolrInputDocument doc : inputDocs) {
            doc.addField(SolrField.MODIFIED_DATE, Date.from(now));
            doc.addField(SolrField.TIMESTAMP, Timestamp.from(now));
            doc.addField(SolrField.MODEL_PATH, "monograph");
            doc.addField(SolrField.LABELS, "test-label");
        }
        srcSolr.add(inputDocs);
        srcSolr.commit();

        Instant yesterday = now.minus(1, ChronoUnit.DAYS);
        for (SolrInputDocument doc : inputDocs) {
            doc.setField(SolrField.MODIFIED_DATE, Date.from(yesterday));
        }
        dstSolr.add(inputDocs);
        dstSolr.commit();

        when(configuration.getLastModifiedDate()).thenReturn(Date.from(yesterday));
        when(configuration.getSrcSolrClient()).thenReturn(srcSolr);
        when(configuration.getDstSolrClient()).thenReturn(dstSolr);
        when(configuration.getQuerySize()).thenReturn(1000);
        when(configuration.getBufferSize()).thenReturn(150);
    }

    @Test
    public void testDeletionSync() throws IOException, SolrServerException {
        ModificationSynchronizer synchronizer = new ModificationSynchronizer(configuration);
        synchronizer.sync();

        int dstNumFound = (int) dstSolr.query(new SolrQuery("*:*")).getResults().getNumFound();
        int srcNumFound = (int) srcSolr.query(new SolrQuery("*:*")).getResults().getNumFound();
        assertEquals(totalDocAmount, dstNumFound);
        assertEquals(totalDocAmount, srcNumFound);

        int dnntNumFound = (int) dstSolr.query(new SolrQuery(SolrField.DNNT + ":true")).getResults().getNumFound();
        assertEquals(totalDocAmount, dnntNumFound);

        int labeledNumFound = (int) dstSolr.query(new SolrQuery(SolrField.LABELS + ":\"test-label\"")).getResults().getNumFound();
        assertEquals(totalDocAmount, labeledNumFound);

        FacetField.Count srcModifiedDateFacetCount = getModifiedDateFacet(srcSolr);
        FacetField.Count dstModifiedDateFacetCount = getModifiedDateFacet(dstSolr);
        assertEquals(srcModifiedDateFacetCount.getName(), dstModifiedDateFacetCount.getName());
        assertEquals(srcModifiedDateFacetCount.getCount(), dstModifiedDateFacetCount.getCount());

        verify(srcSolr, never()).deleteById(anyList());
        verify(srcSolr, never()).deleteById(anyString());
        verify(srcSolr, never()).deleteByQuery(anyString());
    }

    private FacetField.Count getModifiedDateFacet(TestSolrServer server) throws SolrServerException, IOException {
        List<FacetField> facetFields = server.query(new SolrQuery("*:*").addFacetField(SolrField.MODIFIED_DATE)).getFacetFields();
        Optional<FacetField> optionalFacetField = facetFields.stream().filter(field -> field.getName().equals(SolrField.MODIFIED_DATE)).findFirst();
        assertTrue(optionalFacetField.isPresent());
        List<FacetField.Count> facetFieldCounts = optionalFacetField.get().getValues();
        assertFalse(facetFieldCounts.isEmpty());
        return facetFieldCounts.get(0);
    }
}
