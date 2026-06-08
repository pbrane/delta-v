# Delta-V Configuration  
  
## Overview:  
Currently, we have all the micro-services delta-v in place to monitor a communication service provider’s underlay network. The big gap in the project right and what is stopping the solution from being deployable by anyone is the lack of ability to configure the solution, both at the control plane and the data plane. I believe that we need a true service layer for configuration. A service that can be leveraged to perform tasks (driven by use cases/user stories) that various user roles (Admin, Engineer, Manager, Supporter, etc.) want to carry out.  
## Orchestration:  
- The configuration of the control plan, and the data plane where it makes sense, should be driving by a Kubernetes operator but it would also be nice to provide a user interface and an MCP that backed by a service that gives Admins and Network Engineers the ability to tailor the deployment to their needs.  
  
## Control plane:  
- the deployment of delta-v local, “3rd party” infrastructure: PostgreSQL, Envy, Kafka, etc.  
- the deployment of delta-v northbournd, “3rd party” infra dependencies: ClickHouse, VictoriaMetrics, Grafana, etc.  
- the deployment of the delta-v microservices themselves: (JVM parameters, Spring Boot parameters, Operational characterstics at boot time) that are needed to launch each micro-service, independently  
  
## Data plane:  
- the runtime behavior and monitoring characteristics of each of the delta-v microservices used for monitoring and publishing (the data plane), for example: snmp-confg, eventconf, poller-configuration, collectd-configuration, datacollection-config, perspective-pollerd, bsm, enlinkd, etc.  
  
## General thoughts:  
- I have long thought that the eventconf in OpenNMS was overloaded and was carrying the load for 3 trangential requirements and should be split out into the configuration of each of their corresponding functional services  
    - Formatting events (many of the delta-v microservices format events and publish to kafka)  
    - Daemon behavior (for example: “discard trap”, “log only”, etc.)  
    - Alarm behavior (Eventd got highjacked by me long long ago to hand the Alarm behavior before Alarmd was created and all the Alarm workflow was handled on the Eventd thread, after Alarmd was created, this alarm-data element should have been pulled into a separate configuration  
- There should probably eventually should be multiple instances of Kafka. This allows us to expose the partitions of one instance for external integration:  
    - IPC, RPC, and Sync events are really only needed by Delta-V  
    - The Metric, Flow, and Alarm topics are interesting not just for the Default Delta-V consumers but for users that want to write their own integrations  
- Configuration context and serialization:  
    -  XML has long been the configuration recipe for OpenNMS  
        - We have been moving some. configuration to Spring Boot application.yml files and we should continue in this direction for bootstrapping the microservices  
        - We establish a plan to move our configurations that remain in XML to JSON (or whatever serialization is now most popular in modern containerized infra)  
    - OpenNMS Horizon lacks a clear deliniation between the control plane configuration and the data plane  
    - the configuration service should be use case/user story driven and not model driven like a micro-service’s REST API  
    - the configurations service should understand the REST APIs of the micro-services and the REST APIs should be implemented to support the configuration service  
