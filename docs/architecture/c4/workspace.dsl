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
                flowEnricher = container "flow-enricher" "Enriches flows with node context, writes ClickHouse." "Spring Boot / Java 21" "daemon" {
                    feDeser = component "Sink Message Deserializer" "Decodes Kafka sink messages into flow records." "SinkMessageDeserializer" "component"
                    feProtocol = component "Protocol Decoders" "Decodes NetFlow v5/v9, IPFIX and sFlow." "Netflow5/9, Ipfix, SFlow MessageProcessor" "component"
                    feEnrich = component "Flow Enrichment" "Adds node, interface and application context." "FlowEnrichmentFunction, JdbcSnmpInterfaceLookup" "component"
                    feClassify = component "Application Classifier" "Classifies flows by application (port-based)." "PortBasedApplicationClassifier" "component"
                    feMapper = component "Flow Document Mapper" "Maps enriched flows to ClickHouse documents." "FlowToDocumentMapper" "component"
                }
                alarmsPublisher = container "alarms-kafka-publisher" "Publishes alarm state to Kafka." "Spring Boot / Java 21" "daemon" {
                    apMapper = component "Alarm State Mapper" "Maps alarm rows to wire records." "AlarmStateMapper" "component"
                    apPublisher = component "Kafka Alarm Publisher" "Publishes compacted alarm-state records." "KafkaAlarmPublisher" "component"
                    apTopic = component "Topic Initializer" "Ensures the alarm-state topic exists." "AlarmsTopicInitializer" "component"
                }
                alarmsMaterializer = container "alarms-materializer" "Projects alarm state into PostgreSQL." "Spring Boot / Java 21" "daemon" {
                    amConsumer = component "Alarm State Consumer" "Consumes alarm-state records." "AlarmStateKafkaConsumer" "component"
                    amProjector = component "Alarm State Projector" "Projects records into alarm entities." "AlarmStateProjector" "component"
                    amWriter = component "Alarm Upsert Writer" "Upserts alarms into PostgreSQL." "AlarmUpsertWriter" "component"
                    amRetention = component "Retention Engine" "Evaluates retention rules and deletes." "RetentionEngine, AlarmDeleter" "component"
                }
                prometheusWriter = container "prometheus-writer" "Consumes time-series, remote-writes to VictoriaMetrics." "Spring Boot / Java 21" "daemon" {
                    pwConsumer = component "Time-Series Consumer" "Consumes time-series samples from Kafka." "Kafka client" "component"
                    pwRemoteWrite = component "Remote-Write Client" "Sends Prometheus remote-write to VictoriaMetrics." "HTTP" "component"
                }
                alertsForwarder = container "alerts-forwarder" "Forwards alarm/alert state to Alertmanager." "Spring Boot / Java 21" "daemon"
            }
            group "Edge / IPC" {
                kafka = container "Kafka" "Event + RPC spine and sink transport." "Apache Kafka" "messaging"
                minionGateway = container "minion-gateway" "Bridges Kafka (cloud) to gRPC (Minion)." "Spring Boot / Java 21" "edge" {
                    gwKafka = component "Kafka Bridge" "Consumes RPC requests + produces responses; relays sink/telemetry." "Kafka client" "component"
                    gwGrpc = component "gRPC Ingress" "Bidirectional streaming endpoint for Minions." "gRPC server" "component"
                    gwRouter = component "RPC Router" "Correlates RPC requests/responses by location and module." "Java" "component"
                    gwSink = component "Sink Relay" "Forwards Minion sink/telemetry to Kafka topics." "Java" "component"
                }
                envoy = container "Envoy" "TLS-terminating gRPC proxy in front of the gateway." "Envoy Proxy" "edge"
                minion = container "Minion" "Edge agent; performs all device I/O." "Karaf / Java" "edge" {
                    mnGrpc = component "gRPC Client" "Maintains streaming connection to the gateway via Envoy." "gRPC client" "component"
                    mnRpc = component "RPC Module Dispatcher" "Executes monitor/detector/collector requests." "Java" "component"
                    mnListeners = component "Telemetry Listeners" "SNMP traps, syslog, NetFlow/IPFIX/sFlow receivers." "Java" "component"
                    mnSink = component "Sink Producer" "Streams collected telemetry back to the gateway." "Java" "component"
                }
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

        # --- L3: Minion IPC ---
        kafka -> gwKafka "RPC requests + sink" "Kafka" "critical"
        gwKafka -> gwRouter "Hands off requests"
        gwRouter -> gwGrpc "Streams to Minion"
        gwGrpc -> envoy "gRPC" "gRPC" "critical"
        envoy -> mnGrpc "gRPC (mTLS)" "gRPC" "critical"
        mnGrpc -> mnRpc "Dispatches RPC"
        mnRpc -> network "SNMP/ICMP/etc." "SNMP/ICMP" "critical"
        mnListeners -> network "Receives traps/syslog/flows" "UDP"
        mnListeners -> mnSink "Telemetry"
        mnSink -> mnGrpc "Streams back"
        mnGrpc -> gwGrpc "Responses + sink"
        gwGrpc -> gwSink "Sink frames"
        gwSink -> kafka "Sink topics" "Kafka"

        # --- L3: flow pipeline ---
        # (telemetryd -> kafka already declared at L2; reused here)
        kafka -> feDeser "Flow sink messages" "Kafka"
        feDeser -> feProtocol "Raw protocol payloads"
        feProtocol -> feEnrich "Decoded flows"
        feEnrich -> feClassify "Adds application"
        feEnrich -> postgres "Node/interface lookup" "JDBC"
        feClassify -> feMapper "Enriched flows"
        feMapper -> clickhouse "Writes documents" "HTTP"

        # --- L3: metrics & alarm pipeline ---
        alarmd -> apMapper "Alarm rows"
        apMapper -> apPublisher "Wire records"
        apPublisher -> kafka "Alarm-state topic" "Kafka"
        kafka -> amConsumer "Alarm-state records" "Kafka"
        amConsumer -> amProjector "Records"
        amProjector -> amWriter "Alarm entities"
        amWriter -> postgres "Upserts" "JDBC"
        amRetention -> postgres "Deletes expired" "JDBC"
        kafka -> pwConsumer "Time-series samples" "Kafka"
        pwConsumer -> pwRemoteWrite "Samples"
        pwRemoteWrite -> victoriametrics "Remote-write" "HTTP"
        # (grafana -> victoriametrics "Queries" already declared at L2; reused here)
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

        container deltav "ContainersPolling" "L2 (polling): availability/performance/topology daemons, the Kafka spine and Minion device I/O." {
            include operator pollerd collectd perspectivepollerd discovery enlinkd bsmd kafka minionGateway envoy minion network postgres
            autolayout lr
        }

        container deltav "ContainersEvents" "L2 (events): trap/syslog/translation/alarm daemons, alarm publishing/materialization and notification." {
            include noc trapd syslogd eventtranslator alarmd alarmsPublisher alarmsMaterializer alertsForwarder kafka postgres alertmanager oncall
            autolayout lr
        }

        container deltav "ContainersStreaming" "L2 (streaming): telemetry/flow enrichment and the metrics path to VictoriaMetrics and Grafana." {
            include operator telemetryd flowEnricher prometheusWriter kafka clickhouse victoriametrics grafana postgres
            autolayout lr
        }

        component pollerd "DaemonArchetype" "The shared Spring Boot daemon pattern (pollerd shown): event consume -> expand -> logic -> DAO + Minion RPC." {
            include *
            autolayout lr
        }

        component minionGateway "MinionIpc" "The Kafka <-> gRPC IPC bridge: gateway, Envoy and the Minion edge agent." {
            include *
            include mnGrpc mnRpc mnListeners mnSink envoy
            autolayout lr
        }

        component flowEnricher "FlowPipeline" "Flow/telemetry path: telemetryd -> Kafka -> flow-enricher (decode, enrich, classify, map) -> ClickHouse." {
            include *
            include telemetryd
            autolayout lr
        }

        component alarmsMaterializer "MetricsAlarms" "Alarm path (alarmd -> publisher -> Kafka -> materializer -> Postgres) and metrics path (Kafka -> prometheus-writer -> VictoriaMetrics -> Grafana)." {
            include *
            include apMapper apPublisher apTopic pwConsumer pwRemoteWrite alarmd victoriametrics grafana
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
