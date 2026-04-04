## Version 3.6.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.6.0

### Features

* Add SearchAroundTool to search N documents around a given document ([#702](https://github.com/opensearch-project/skills/pull/702))
* Add MetricChangeAnalysisTool for detecting and analyzing metric changes via percentile comparison between baseline and selection periods ([#698](https://github.com/opensearch-project/skills/pull/698))

### Enhancements

* Add filter support for LogPatternAnalysisTool to enable log pattern analysis for specific services ([#707](https://github.com/opensearch-project/skills/pull/707))
* Update default tool descriptions for LogPatternAnalysisTool and DataDistributionTool to improve clarity for LLM usage ([#703](https://github.com/opensearch-project/skills/pull/703))

### Bug Fixes

* Fix LogPatternAnalysisTool missing attributes ([#690](https://github.com/opensearch-project/skills/pull/690))

### Maintenance

* Update Apache Spark dependencies (spark-common-utils_2.13) from 3.5.4 to 3.5.8 ([#713](https://github.com/opensearch-project/skills/pull/713))
