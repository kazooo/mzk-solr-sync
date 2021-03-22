# Solr synchronization application

Simple application for MZK Solr indexes synchronization. \
Uses the *modified_date* field to filter modified documents and sends them to another index.

## Parameters

|          parameter       |    description                          |              example           | required |    default value   |
|:------------------------:|:---------------------------------------:|:------------------------------:|:--------:|:------------------:|
|  SRC_SOLR_HOST           |   source Solr url, with core name       |  http://solr.cz/solr/kramerius |   true   |      |
|  DST_SOLR_HOST           |   destination Solr url, with core name  |  http://solr.cz/solr/kramerius |   true   |      |
|  MODIFICATIONS_SYNC_CRON |   Quartz cron for modification sync     |  0 0 * ? * * (every hour)      |   true   |      |
|  DELETIONS_SYNC_CRON     |   Quartz cron for deletions sync        |  0 */12 * * * (every 12 hours) |   true   |      |
|  SYNC_AFTER_START        |   synchronize at start or not           |  true                          |   false  | false|
|  MODIFIED_DATE           |   last modified date                    |  "2021-02-19 15:00:00"         |   true   |      |
|  DATE_FORMAT             |   last modified date format             |  "yyyy-MM-dd HH:mm:ss"         |   true   |      |
|  QUERY_SIZE              |   number of docs per query              |  5000                          |   false  | 1000 |
|  BUFFER_SIZE             |   doc buffer size                       |  10000                         |   false  | 5000 |
|  IGNORED_ROOTS_FILE      |   ignored doc roots                     |  ignore.txt                    |   false  | null |


Start synchronizer executable jar file with parameters above
```
./gradlew bootJar
java -DSRC_SOLR_HOST=http://solr-host <another parameters with '-D' prefix> -jar app.jar
```

or using Gradle 'bootRun'
```
./gradlew bootRun --args='--SRC_SOLR_HOST=http://solr-host <another parameters with "--" prefix>'
```

Synchronizer can accept mentioned parameters from environment variables.
