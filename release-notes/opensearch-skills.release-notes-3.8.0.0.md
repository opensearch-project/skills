## Version 3.8.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.8.0

### Enhancements

* Onboard code diff analyzer/reviewer and issue dedupe workflows ([#764](https://github.com/opensearch-project/skills/pull/764))
* Onboard new backport-pr reusable GitHub workflow ([#759](https://github.com/opensearch-project/skills/pull/759))
* Update maven2 mirror repository URL order ([#767](https://github.com/opensearch-project/skills/pull/767))

### Bug Fixes

* Fix compilation failure due to AD method signature change ([#758](https://github.com/opensearch-project/skills/pull/758))

### Infrastructure

* Update actions/setup-java action to v5 ([#736](https://github.com/opensearch-project/skills/pull/736))
* Update opensearch-build workflow references from commit SHA to main branch ([#749](https://github.com/opensearch-project/skills/pull/749))

### Maintenance

* Sync CODEOWNERS with maintainer list ([#755](https://github.com/opensearch-project/skills/pull/755))

### Refactoring

* Use PPL instead of DSL match_all query to fetch sample data in PPLTool ([#752](https://github.com/opensearch-project/skills/pull/752))
