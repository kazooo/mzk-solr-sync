package cz.mzk.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class SendBuffer {

    private final SolrClient dstSolrClient;
    private final LimitedList<SolrInputDocument> docBuffer;

    public SendBuffer(int s, SolrClient dsc) {
        dstSolrClient = dsc;
        docBuffer = new LimitedList<>(s);
    }

    public void add(List<SolrInputDocument> inputDocs) {
        while (inputDocs != null) {
            inputDocs = docBuffer.addAll(inputDocs);
            if (inputDocs != null) {
                send();
            }
        }
    }

    private void send() {
        try {
            dstSolrClient.add(docBuffer);
        } catch (SolrServerException | IOException e) {
            e.printStackTrace();
        }
    }

    private static class LimitedList<E> extends ArrayList<E> {

        private final int size;

        public LimitedList(int s) {
            size = s;
        }

        public List<E> addAll(List<E> c) {
            int freeSpace = size - super.size();
            if (freeSpace == 0) return c;

            int maxEls = Integer.min(freeSpace, c.size());
            super.addAll(c.subList(0, maxEls));

            if (maxEls == freeSpace) {
                return c.subList(maxEls, c.size()-1);
            } else {
                return null;   // add all elements of incoming list
            }
        }
    }
}
