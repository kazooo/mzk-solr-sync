package cz.mzk;

import cz.mzk.util.SolrField;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.CoreContainer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class TestSolrServer extends EmbeddedSolrServer {

    private TestSolrServer(CoreContainer coreContainer, String coreName) {
        super(coreContainer, coreName);
    }

    public static TestSolrServer buildAndInit(String solrHome, String coreName) throws IOException, SolrServerException {
        CoreContainer container = new CoreContainer(Path.of(solrHome), null);
        container.load();
        TestSolrServer testSolrServer = new TestSolrServer(container, coreName);
        testSolrServer.deleteByQuery("*:*");
        testSolrServer.commit();
        return testSolrServer;
    }

    public static List<SolrInputDocument> generateRandomDocs(int amount) {
        List<SolrInputDocument> inputDocs = new ArrayList<>();
        for (int i = 0; i < amount; i++) {
            SolrInputDocument doc = new SolrInputDocument();
            String generatedStr = generateRandomAlphanumericString();
            doc.addField(SolrField.UUID, generatedStr);
            doc.addField(SolrField.ROOT_UUID, generatedStr);
            inputDocs.add(doc);
        }
        return inputDocs;
    }

    private static String generateRandomAlphanumericString() {
        final int leftLimit = 48; // numeral '0'
        final int rightLimit = 122; // letter 'z'
        final int targetStringLength = 10;
        Random random = new Random();

        return random.ints(leftLimit, rightLimit + 1)
                /* ignore punctuation marks */
                .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                .limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }
}
