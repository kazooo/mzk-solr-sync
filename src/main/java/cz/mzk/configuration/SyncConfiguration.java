package cz.mzk.configuration;

import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;


@Configuration
@Slf4j
public class SyncConfiguration {

    public SyncConfiguration(@Qualifier("src_solr_client") SolrClient srcSC,
                             @Qualifier("src_solr_client") SolrClient dstSC,
                             AppConfiguration c) {
        config = c;
        srcSolrClient = srcSC;
        dstSolrClient = dstSC;
    }

    private final AppConfiguration config;
    private final SolrClient srcSolrClient;
    private final SolrClient dstSolrClient;

    @EventListener(ApplicationReadyEvent.class)
    public void syncAfterStartup() {
        log.info("Source Solr address: " + config.getSrcSolrHost());
        log.info("Destination Solr address: " + config.getDstSolrHost());

        if (config.isSyncAfterStart()) {
            log.info("Start synchronizing...");
            // fetch by modified_date by cursor
            // send to dst cursor
        }
    }

    @Scheduled(cron = "#{appConfiguration.cron}")
    public void triggerSync() {
        log.info("Trigger scheduled synchronizing...");
    }
}
