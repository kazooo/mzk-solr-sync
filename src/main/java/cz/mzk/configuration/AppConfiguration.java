package cz.mzk.configuration;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Date;
import java.util.Set;
import java.util.stream.Collectors;


@Configuration
@Getter
@Slf4j
public class AppConfiguration {

    @Value("${SRC_SOLR_HOST}")
    private String srcSolrHost;

    @Value("${DST_SOLR_HOST}")
    private String dstSolrHost;

    @Value("${MODIFICATIONS_SYNC_CRON}")
    private String modificationsSyncCron;

    @Value("${DELETIONS_SYNC_CRON}")
    private String deletionsSyncCron;

    @Value("${SYNC_MODIFS_AFTER_START:false}")
    private boolean syncModificationsAfterStart;

    @Value("${SYNC_DELETS_AFTER_START:false}")
    private boolean syncDeletionsAfterStart;

    @Value("#{new java.text.SimpleDateFormat('${DATE_FORMAT}').parse('${MODIFIED_DATE}')}")
    private Date lastModifiedDate;

    @Value("${QUERY_SIZE:1000}")
    private int querySize;

    @Value("${BUFFER_SIZE:5000}")
    private int bufferSize;

    @Value("${IGNORED_ROOTS_FILE:}")
    private String ignoredRootsFileName;

    /**
     * Temporary solution, replace with another data source in the future...
     * */
    public Set<String> getIgnoredRoots() {
        try {
            return Files.readAllLines(Paths.get(ignoredRootsFileName)).stream()
                    .map(uuid -> uuid.startsWith("uuid:") ? uuid : "uuid:" + uuid)
                    .collect(Collectors.toSet());
        } catch (IOException e) {
            log.warn("Can't load ignored document roots from " + ignoredRootsFileName);
            return Collections.emptySet();
        }
    }

    @Bean(name = "src_solr_client")
    public SolrClient getSrcSolrClient() {
        return getSolrClient(srcSolrHost);
    }

    @Bean(name = "dst_solr_client")
    public SolrClient getDstSolrClient() {
        return getSolrClient(dstSolrHost);
    }

    private SolrClient getSolrClient(String host) {
        return new HttpSolrClient.Builder(host).withConnectionTimeout(60000).build();
    }
}
