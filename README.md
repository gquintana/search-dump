# search-dump

Elasticsearch &amp; OpenSearch dump tool.
* Inspired by [elasticsearch-dump](https://github.com/elasticsearch-dump/elasticsearch-dump)
* Writen in Java

## Command line arguments



### General

| Argument              | Property            | Environment         | Default | Example                         | Description                                                         |
|-----------------------|---------------------|---------------------|---------|---------------------------------|---------------------------------------------------------------------|
| --config              |                     | CONFIG              |         | ./config.properties             | Path to the config.properties file containing properties            |
| --index-names         | index.names         | INDEX_NAMES         |         | foo,bar*                        | Coma separated list of index names, or index names globs. Example:  |
| --index-skip-failed   | index.skip.failed   | INDEX_SKIP_FAILED   | true    | true false                      | When an index copy fails, continue with next one or stop            |
| --index-skip-existing | index.skip.existing | INDEX_SKIP_EXISTING | true    | true false                      | When an index already exists, insert data or skip index             |
| --reader-type         | reader.type         | READER_TYPE         |         | elasticsearch opensearch zip s3 | Where data should be read from                                      |
| --writer-type         | writer.type         | WRITER_TYPE         |         | elasticsearch opensearch zip s3 | Where data should be written to                                     |

### Elasticsearch and OpenSearch
| Argument             | Property           | Environment        | Default | Example                | Description                                                            |
|----------------------|--------------------|--------------------|---------|------------------------|------------------------------------------------------------------------|
| --reader-url         | reader.url         | READER_URL         |         | https://localhost:9200 | Connection URL                                                         |
| --reader-username    | reader.username    | READER_USERNAME    |         |                        | User name used for authentication                                      |
| --reader-password    | reader.password    | READER_PASSWORD    |         |                        | Password used for authentication                                       |
| --reader-page-size   | reader.page.size   | READER_PAGE_SIZE   | 1000    |                        | Search page size, number of documents retrieved in single search query |
| --reader-scroll-time | reader.scroll.time | READER_SCROLL_TIME | 5m      |                        | Search scroll time out, used for scroll queries                        |
| --writer-url         | writer.url         | WRITER_URL         |         | https://localhost:9200 | Connection URL                                                         |
| --writer-username    | writer.username    | WRITER_USERNAME    |         |                        | User name used for authentication                                      |
| --writer-password    | writer.password    | WRITER_PASSWORD    |         |                        | Password used for authentication                                       |
| --writer-bulk-size   | writer.bulk.size   | WRITER_BULK_SIZE   | 1000    |                        | Write bulk size, used for bulk indexing                                |



### Local Zip File

| Argument           | Property         | Environment      | Default | Example       | Description                       |
|--------------------|------------------|------------------|---------|---------------|-----------------------------------|
| --reader-file      | reader.file      | READER_FILE      |         | ./foo/bar.zip | Zip file name and path            |
| --writer-file      | writer.file      | WRITER_FILE      |         | ./foo/bar.zip | Zip file name and path            |
| --writer-file-size | writer.file.size | WRITER_FILE_SIZE | 1000    |               | Number of documents per data file |

### S3 Files

| Argument              | Property            | Environment         | Default | Example   | Description                                   |
|-----------------------|---------------------|---------------------|---------|-----------|-----------------------------------------------|
| --reader-endpoint-url | reader.endpoint.url | READER_ENDPOINT_URL |         |           | S3 Endpoint URL                               |
| --reader-region       | reader.region       | READER_REGION       |         | eu-west-1 | AWS Region                                    |
| --reader-bucket       | reader.bucket       | READER_BUCKET       |         |           | S3 Bucket name                                |
| --reader-key          | reader.key          | READER_KEY          |         | foo/bar   | Key prefix, base directory containing indices |
| --writer-endpoint-url | writer.endpoint.url | WRITER_ENDPOINT_URL |         |           | S3 Endpoint URL                               |
| --writer-region       | writer.region       | WRITER_REGION       |         | eu-west-1 | AWS Region                                    |
| --writer-bucket       | writer.bucket       | WRITER_BUCKET       |         |           | S3 Bucket name                                |
| --writer-key          | writer.key          | WRITER_KEY          |         | foo/bar   | Key prefix, base directory containing indices |
| --writer-file-size    | writer.file.size    | WRITER_FILE_SIZE    | 1000    |           | Number of documents per data file             |