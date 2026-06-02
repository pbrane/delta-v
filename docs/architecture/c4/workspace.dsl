workspace "Delta-V" "Cloud-native network monitoring platform (OpenNMS Horizon fork)" {

    model {
        operator = person "Network Operator" "Configures monitoring, requisitions, and thresholds." "person"
        noc = person "NOC / SRE" "Watches health, responds to alerts and outages." "person"

        network = softwareSystem "Monitored Network" "Routers, switches, servers and services reached via SNMP, ICMP, flows, syslog and SNMP traps." "external"
        oncall = softwareSystem "On-Call / Paging" "PagerDuty, email, and chat receivers fed by Alertmanager." "external"

        deltav = softwareSystem "Delta-V" "Network monitoring platform: discovery, polling, collection, flows, events, alarms and metrics." {
            # containers added in Task 2
        }

        # L1 relationships
        operator -> deltav "Configures and operates"
        noc -> deltav "Monitors health and outages"
        deltav -> network "Discovers, polls, collects and receives telemetry from (via Minion)"
        deltav -> oncall "Raises notifications to"
    }

    views {
        systemContext deltav "SystemContext" "Delta-V in its operating environment." {
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
