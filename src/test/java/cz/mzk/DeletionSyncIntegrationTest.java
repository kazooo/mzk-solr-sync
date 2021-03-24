package cz.mzk;

import cz.mzk.configuration.AppConfiguration;
import cz.mzk.synchronizer.DeletionSynchronizer;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.List;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyString;
import static org.junit.jupiter.api.Assertions.assertEquals;


@RunWith(MockitoJUnitRunner.class)
public class DeletionSyncIntegrationTest {

    @Mock
    AppConfiguration configuration;

    private static final int totalDocAmount = 100;
    private static final int srcDocAmount = 73;

    private TestSolrServer srcSolr;
    private TestSolrServer dstSolr;

    @Before
    public void setupConfigMock() throws IOException, SolrServerException {
        srcSolr = Mockito.spy(TestSolrServer.buildAndInit("src/test/resources/src_solr", "test_src_core"));
        dstSolr = Mockito.spy(TestSolrServer.buildAndInit("src/test/resources/dst_solr", "test_dst_core"));
        List<SolrInputDocument> inputDocs = TestSolrServer.generateRandomDocs(totalDocAmount);

        srcSolr.add(inputDocs.subList(0, srcDocAmount));
        srcSolr.commit();
        dstSolr.add(inputDocs);
        dstSolr.commit();

        when(configuration.getSrcSolrClient()).thenReturn(srcSolr);
        when(configuration.getDstSolrClient()).thenReturn(dstSolr);
        when(configuration.getQuerySize()).thenReturn(1000);
    }

    @Test
    public void testDeletionSync() throws IOException, SolrServerException {
        DeletionSynchronizer synchronizer = new DeletionSynchronizer(configuration);
        synchronizer.sync();

        int dstNumFound = (int) dstSolr.query(new SolrQuery("*:*")).getResults().getNumFound();
        int srcNumFound = (int) srcSolr.query(new SolrQuery("*:*")).getResults().getNumFound();
        assertEquals(srcDocAmount, dstNumFound);
        assertEquals(srcDocAmount, srcNumFound);

        verify(srcSolr, never()).deleteById(anyList());
        verify(srcSolr, never()).deleteById(anyString());
        verify(srcSolr, never()).deleteByQuery(anyString());
        verify(dstSolr, times(totalDocAmount - srcDocAmount)).deleteById(anyList());
    }
}
