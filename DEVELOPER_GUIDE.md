- [Developer Guide](#developer-guide)
  - [Forking and Cloning](#forking-and-cloning)
  - [Install Prerequisites](#install-prerequisites)
    - [JDK 11](#jdk-11)
  - [Building](#building)
  - [Using IntelliJ IDEA](#using-intellij-idea)
  - [Submitting Changes](#submitting-changes)

## Developer Guide

So you want to contribute code to this project? Excellent! We're glad you're here. Here's what you need to do.

### Forking and Cloning

Fork this repository on GitHub, and clone locally with `git clone`.

### Install Prerequisites

#### JDK 11

OpenSearch components build using Java 11 at a minimum. This means you must have a JDK 11 installed with the environment variable `JAVA_HOME` referencing the path to Java home for your JDK 11 installation, e.g. `JAVA_HOME=/usr/lib/jvm/jdk-11`.

### Building

To build from the command line, use `./gradlew`.

```
./gradlew clean
./gradlew build
./gradlew publishToMavenLocal
```

### Build your custom tool

* Create a new Java file in the specified package directory -> eg. cat src/main/java/org/opensearch/agent/tools/NewTool.java
* Modify ToolPlugin file to instantiate, initialize, and add the new tool, refer -> ([here](https://github.com/opensearch-project/skills/pull/81/files))
* Start the server with ./gradlew run

### Test your custom tool

* Make sure to have access to the LLM that you're using
* Create any remote connector using ([remote_inference_blueprints](https://github.com/opensearch-project/ml-commons/blob/main/docs/remote_inference_blueprints))
* Get the model_id from the step above and provide it as a parameter in below step to register the agent
* Register the agent that will run your custom tool for ([reference](https://opensearch.org/docs/latest/ml-commons-plugin/agents-tools/tools/ml-model-tool/#step-3-register-a-flow-agent-that-will-run-the-mlmodeltool))
* Get the agent_id from the step above and provide it as part of URL in below step to run the agent
* Run the agent ([refer](https://opensearch.org/docs/latest/ml-commons-plugin/agents-tools/tools/ml-model-tool/#step-4-run-the-agent))

### Using IntelliJ IDEA

Launch Intellij IDEA, choose **Import Project**, and select the `settings.gradle` file in the root of this package. 

### Submitting Changes

See [CONTRIBUTING](CONTRIBUTING.md).

### Backport

- [Link to backport documentation](https://github.com/opensearch-project/opensearch-plugins/blob/main/BACKPORT.md)
