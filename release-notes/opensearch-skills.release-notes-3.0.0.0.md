## Version 3.0.0.0-beta1 Release Notes

Compatible with OpenSearch 3.0.0.0-beta1

### Features
* Add web search tool ([#547](https://github.com/opensearch-project/skills/pull/547))

### Bug Fixes
* Fix list bug of PPLTool when pass empty list ([#541](https://github.com/opensearch-project/skills/pull/541))

### Infrastructure
* Replace `ml-common-client` build dependency to `ml-common-common` and `ml-common-spi` (#529)[https://github.com/opensearch-project/skills/pull/529]

### Maintainance
* Remove `space_type` in integ test to adapt to the change of k-NN plugin ([#535](https://github.com/opensearch-project/skills/pull/535))
* Fix jar hell for sql jar ([#545](https://github.com/opensearch-project/skills/pull/545))
* Add attributes to tools to adapt the upstream changes ([#549](https://github.com/opensearch-project/skills/pull/549))
* Support phasing off SecurityManager usage in favor of Java Agent ([#553](https://github.com/opensearch-project/skills/pull/553))

### Documentation
* Add tutorial to build and test custom tool ([#521](https://github.com/opensearch-project/skills/pull/521))
