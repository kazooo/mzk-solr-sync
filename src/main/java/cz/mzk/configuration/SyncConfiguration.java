package cz.mzk.configuration;

import cz.mzk.solr.Synchronizer;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;


@Configuration
@Slf4j
@AllArgsConstructor
public class SyncConfiguration {

    private final AppConfiguration config;
    private final Synchronizer synchronizer;

    @EventListener(ApplicationReadyEvent.class)
    public void syncAfterStartup() {
        log.info("Source Solr address: " + config.getSrcSolrHost());
        log.info("Destination Solr address: " + config.getDstSolrHost());
        log.info("Last modified date: " + config.getLastModifiedDate());

        if (config.isSyncAfterStart()) {
            log.info("Start synchronizing...");
            synchronizer.run();
        }
    }

    @Scheduled(cron = "#{appConfiguration.cron}")
    public void triggerSync() {
        log.info("Trigger scheduled synchronizing...");
        synchronizer.run();
    }
}
