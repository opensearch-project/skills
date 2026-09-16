## Version 3.9.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.9.0

### Enhancements

* Add input schema to SearchAlertsTool for improved parameter discovery ([#769](https://github.com/opensearch-project/skills/pull/769))

### Bug Fixes

* Add null check for mapping properties in PPLTool to prevent NullPointerException on indices with empty mappings ([#773](https://github.com/opensearch-project/skills/pull/773))

### Infrastructure

* Update actions/checkout digest ([#771](https://github.com/opensearch-project/skills/pull/771))
* Remove Pull Request Labeler workflow to fix CI error ([#774](https://github.com/opensearch-project/skills/pull/774))

### Maintenance

* Bump 1password/load-secrets-action to v5.0.1 ([#784](https://github.com/opensearch-project/skills/pull/784))
