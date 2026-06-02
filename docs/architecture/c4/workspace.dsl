workspace "Delta-V" "Cloud-native network monitoring platform (OpenNMS Horizon fork)" {

    model {
        operator = person "Network Operator" "Configures monitoring, requisitions, and thresholds." "person"
        noc = person "NOC / SRE" "Watches health, responds to alerts and outages." "person"

        network = softwareSystem "Monitored Network" "Routers, switches, servers and services reached via SNMP, ICMP, flows, syslog and SNMP traps." "external"
        oncall = softwareSystem "On-Call / Paging" "PagerDuty, email, and chat receivers fed by Alertmanager." "external"

        deltav = softwareSystem "Delta-V" "Network monitoring platform: discovery, polling, collection, flows, events, alarms and metrics." {
            # --- Active monitoring daemons ---
            group "Monitoring Daemons" {
                pollerd = container "pollerd" "Service availability polling." "Spring Boot / Java 21" "daemon" {
                    eventConsumer = component "Kafka Event Transport" "Consumes and produces OpenNMS events over Kafka." "KafkaEventTransportConfiguration" "component"
                    eventExpander = component "Event Expander / Enrichment" "Expands and enriches events from event-conf (logmsg, descr, severity)." "EventConfEnrichmentService, EventIpcManagerEnrichingWrapper, DaemonEventConfDao" "component"
                    pollerLogic = component "Pollerd Service Logic" "Schedules pollable services and detects outages." "Pollerd" "component"
                    rpcClient = component "Kafka RPC Client" "Dispatches device monitor requests to Minion." "KafkaRpcClientConfiguration" "component"
                    daoLayer = component "JDBC DAO Layer" "Reads/writes nodes, services, outages." "JdbcEventUtil, JdbcDistPollerDao, DaemonDataSourceConfiguration" "component"
                }
                collectd = container "collectd" "Performance data collection." "Spring Boot / Java 21" "daemon"
                discovery = container "discovery" "Network discovery." "Spring Boot / Java 21" "daemon"
                provisiond = container "provisiond" "Node provisioning and requisitions." "Spring Boot / Java 21" "daemon"
                enlinkd = container "enlinkd" "Link/topology discovery." "Spring Boot / Java 21" "daemon"
                perspectivepollerd = container "perspectivepollerd" "Remote-perspective polling." "Spring Boot / Java 21" "daemon"
                bsmd = container "bsmd" "Business service monitoring." "Spring Boot / Java 21" "daemon"
            }
            group "Event & Notification Daemons" {
                trapd = container "trapd" "SNMP trap reception and eventing." "Spring Boot / Java 21" "daemon"
                syslogd = container "syslogd" "Syslog reception and eventing." "Spring Boot / Java 21" "daemon"
                eventtranslator = container "eventtranslator" "Event translation rules." "Spring Boot / Java 21" "daemon"
                alarmd = container "alarmd" "Alarm reduction from events." "Spring Boot / Java 21" "daemon"
            }
            group "Streaming & Enrichment" {
                telemetryd = container "telemetryd" "Flow/telemetry ingestion bridge." "Spring Boot / Java 21" "daemon"
                flowEnricher = container "flow-enricher" "Enriches flows with node context, writes ClickHouse." "Spring Boot / Java 21" "daemon"
                alarmsPublisher = container "alarms-kafka-publisher" "Publishes alarm state to Kafka." "Spring Boot / Java 21" "daemon"
                alarmsMaterializer = container "alarms-materializer" "Projects alarm state into PostgreSQL." "Spring Boot / Java 21" "daemon"
                prometheusWriter = container "prometheus-writer" "Consumes time-series, remote-writes to VictoriaMetrics." "Spring Boot / Java 21" "daemon"
                alertsForwarder = container "alerts-forwarder" "Forwards alarm/alert state to Alertmanager." "Spring Boot / Java 21" "daemon"
            }
            group "Edge / IPC" {
                kafka = container "Kafka" "Event + RPC spine and sink transport." "Apache Kafka" "messaging"
                minionGateway = container "minion-gateway" "Bridges Kafka (cloud) to gRPC (Minion)." "Spring Boot / Java 21" "edge"
                envoy = container "Envoy" "TLS-terminating gRPC proxy in front of the gateway." "Envoy Proxy" "edge"
                minion = container "Minion" "Edge agent; performs all device I/O." "Karaf / Java" "edge"
            }
            group "Data Stores" {
                postgres = container "PostgreSQL" "Nodes, events, alarms, outages." "PostgreSQL 15" "database"
                clickhouse = container "ClickHouse" "Enriched flow documents." "ClickHouse" "database"
                victoriametrics = container "VictoriaMetrics" "Time-series metrics store." "VictoriaMetrics" "database"
            }
            group "Observability" {
                grafana = container "Grafana" "Dashboards over metrics and flows." "Grafana" "observability"
                alertmanager = container "Alertmanager" "Alert routing, grouping and notification." "Prometheus Alertmanager" "observability"
            }
        }

        # L1 relationships
        operator -> deltav "Configures and operates"
        noc -> deltav "Monitors health and outages"
        deltav -> network "Discovers, polls, collects and receives telemetry from (via Minion)"
        deltav -> oncall "Raises notifications to"

        # --- L2 container relationships ---
        operator -> grafana "Views dashboards"
        operator -> provisiond "Manages requisitions"
        noc -> grafana "Watches dashboards"

        # event + RPC spine (critical path)
        pollerd -> kafka "Consumes/produces events; sends device RPC" "Kafka" "critical"
        collectd -> kafka "Consumes/produces events; sends device RPC" "Kafka" "critical"
        discovery -> kafka "Consumes/produces events; sends device RPC" "Kafka" "critical"
        provisiond -> kafka "Consumes/produces events; sends device RPC" "Kafka" "critical"
        enlinkd -> kafka "Consumes/produces events; sends device RPC" "Kafka" "critical"
        perspectivepollerd -> kafka "Consumes/produces events; sends device RPC" "Kafka" "critical"
        bsmd -> kafka "Consumes/produces events" "Kafka"
        trapd -> kafka "Produces events from traps" "Kafka"
        syslogd -> kafka "Produces events from syslog" "Kafka"
        eventtranslator -> kafka "Translates events" "Kafka"
        alarmd -> kafka "Consumes events, produces alarm state" "Kafka"
        telemetryd -> kafka "Bridges flow sink to Kafka" "Kafka"

        # Minion I/O path (critical path)
        kafka -> minionGateway "RPC requests + sink" "Kafka" "critical"
        minionGateway -> envoy "gRPC" "gRPC" "critical"
        envoy -> minion "gRPC (mTLS)" "gRPC" "critical"
        minion -> network "Polls, collects, receives traps/syslog/flows" "SNMP/ICMP/UDP" "critical"

        # persistence
        pollerd -> postgres "Reads/writes" "JDBC"
        collectd -> postgres "Reads/writes" "JDBC"
        provisiond -> postgres "Reads/writes" "JDBC"
        enlinkd -> postgres "Reads/writes" "JDBC"
        alarmd -> postgres "Reads/writes" "JDBC"
        bsmd -> postgres "Reads/writes" "JDBC"

        # flow pipeline
        flowEnricher -> kafka "Consumes flow sink messages" "Kafka"
        flowEnricher -> postgres "Node-context lookup" "JDBC"
        flowEnricher -> clickhouse "Writes enriched flow documents" "HTTP"

        # alarm/alert pipeline
        alarmsPublisher -> kafka "Publishes alarm state" "Kafka"
        alarmsMaterializer -> kafka "Consumes alarm state" "Kafka"
        alarmsMaterializer -> postgres "Upserts alarms" "JDBC"
        alertsForwarder -> kafka "Consumes alarm/alert state" "Kafka"
        alertsForwarder -> alertmanager "POST /api/v2/alerts" "HTTP"
        alertmanager -> oncall "Notifies" "HTTP/SMTP"

        # metrics pipeline
        prometheusWriter -> kafka "Consumes time-series" "Kafka"
        prometheusWriter -> victoriametrics "Remote-write" "HTTP"
        grafana -> victoriametrics "Queries" "PromQL/HTTP"
        grafana -> clickhouse "Queries flows" "HTTP/SQL"

        # --- L3: daemon archetype (pollerd) ---
        kafka -> eventConsumer "Delivers events" "Kafka"
        eventConsumer -> eventExpander "Raw events"
        eventExpander -> pollerLogic "Enriched events"
        pollerLogic -> rpcClient "Requests device poll"
        rpcClient -> kafka "RPC request/response" "Kafka" "critical"
        pollerLogic -> daoLayer "Persists outages/state"
        daoLayer -> postgres "SQL" "JDBC"
    }

    views {
        systemContext deltav "SystemContext" "Delta-V in its operating environment." {
            include *
            autolayout lr
        }

        container deltav "Containers" "The deployable units of Delta-V and how they communicate." {
            include *
            autolayout lr
        }

        component pollerd "DaemonArchetype" "The shared Spring Boot daemon pattern (pollerd shown): event consume -> expand -> logic -> DAO + Minion RPC." {
            include *
            autolayout lr
        }

        styles {
            element "Element" {
                fontSize 22
                color "#08313F"
            }
            element "person" {
                shape person
                background "#E07A5F"
                color "#FDFCF7"
            }
            element "Software System" {
                background "#06425C"
                color "#FDFCF7"
            }
            element "external" {
                background "#5C8AA0"
                color "#FDFCF7"
            }
            element "daemon" {
                background "#06425C"
                color "#FDFCF7"
            }
            element "messaging" {
                shape pipe
                background "#19C3B2"
                color "#08313F"
            }
            element "edge" {
                background "#7FE3D8"
                color "#08313F"
            }
            element "database" {
                shape cylinder
                background "#0E7C9D"
                color "#FDFCF7"
            }
            element "observability" {
                background "#F6D04D"
                color "#08313F"
            }
            element "component" {
                background "#0E7C9D"
                color "#FDFCF7"
            }
            relationship "Relationship" {
                color "#08313F"
                thickness 2
            }
            relationship "critical" {
                color "#F6D04D"
                thickness 4
            }
        }
    }
}
