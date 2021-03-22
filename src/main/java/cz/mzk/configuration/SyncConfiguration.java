package cz.mzk.configuration;

import cz.mzk.solr.DeletionSynchronizer;
import cz.mzk.solr.ModificationSynchronizer;
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
    private final ModificationSynchronizer modificationSynchronizer;
    private final DeletionSynchronizer deletionSynchronizer;

    @EventListener(ApplicationReadyEvent.class)
    public void syncAfterStartup() {
        log.info("Source Solr address: " + config.getSrcSolrHost());
        log.info("Destination Solr address: " + config.getDstSolrHost());

        if (config.isSyncAfterStart()) {
            log.info("Synchronize after start...");
            modificationSynchronizer.sync();
        }
    }

    @Scheduled(cron = "#{appConfiguration.modificationsSyncCron}")
    public void triggerModificationsSync() {
        log.info("Trigger scheduled synchronization of modifications...");
        modificationSynchronizer.sync();
    }

    @Scheduled(cron = "#{appConfiguration.deletionsSyncCron}")
    public void triggerDeletionsSync() {
        log.info("Trigger scheduled synchronization of deletions...");
        deletionSynchronizer.sync();
    }
}
