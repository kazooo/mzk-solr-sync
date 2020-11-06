package cz.mzk.configuration;

import lombok.Getter;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Date;


@Configuration
@Getter
public class AppConfiguration {

    @Value("${SRC_SOLR_HOST}")
    private String srcSolrHost;

    @Value("${DST_SOLR_HOST}")
    private String dstSolrHost;

    @Value("${CRON}")
    private String cron;

    @Value("${SYNC_AFTER_START:false}")
    private boolean syncAfterStart;

    @Value("#{new java.text.SimpleDateFormat('${DATE_FORMAT}').parse('${MODIFIED_DATE}')}")
    private Date lastModifiedDate;

    @Value("${QUERY_SIZE:1000}")
    private int querySize;

    @Value("${BUFFER_SIZE:5000}")
    private int bufferSize;

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
