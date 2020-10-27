package cz.mzk;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;


@Configuration
@Slf4j
@AllArgsConstructor
public class SyncConfiguration {

    private final AppConfiguration config;

    @EventListener(ApplicationReadyEvent.class)
    public void syncAfterStartup() {
        log.info("Source Solr address: " + config.getSrcSolrHost());
        log.info("Destination Solr address: " + config.getDstSolrHost());
        log.info("Start synchronizing...");
    }
}
