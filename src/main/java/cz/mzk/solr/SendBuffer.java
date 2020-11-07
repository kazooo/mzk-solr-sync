package cz.mzk.solr;

import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


@Slf4j
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
                log.debug("Buffer overflow, send current buffer content, then add " + inputDocs.size() + " documents...");
                send();
            }
        }
    }

    private void send() {
        try {
            log.debug("Sending " + docBuffer.size() + " documents to the destination Solr...");
            if (docBuffer.isEmpty()) return;
            dstSolrClient.add(docBuffer);
            docBuffer.clear();
        } catch (SolrServerException | IOException e) {
            e.printStackTrace();
        }
    }

    public void empty() {
        try {
            log.debug("Send the buffer content and commit...");
            send();
            dstSolrClient.commit();
        } catch (SolrServerException | IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            log.debug("Empty the buffer and close destination Solr client...");
            empty();
            dstSolrClient.close();
        } catch (IOException e) {
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
            // if no free space returns all docs
            if (freeSpace == 0) return c;

            // get how many doc can be stored in buffer,
            // all documents or 'freeSpace' of the buffer
            int maxEls = Integer.min(freeSpace, c.size());
            // store them
            super.addAll(c.subList(0, maxEls));

            // if stored only part of the original list...
            if (maxEls == freeSpace && maxEls < c.size()) {
                // returns part of the original list that is not in the buffer
                return c.subList(maxEls, c.size()-1);
            } else {
                return null;   // add all elements of incoming list
            }
        }
    }
}
