# Amalfi Theme — Delta-V C4 Diagrams

Palette inspired by the Amalfi coastline: deep Mediterranean blue vs. sun-lit
turquoise shallows, with Positano-lemon highlights. Color encodes C4 semantics.

| Tag (Structurizr) | Hex | Role |
|---|---|---|
| `person` | `#E07A5F` | Actors (terracotta / Positano rooftops) |
| `daemon` | `#06425C` | Delta-V core Spring Boot daemons (deep sea navy) |
| `messaging` | `#19C3B2` | Kafka event + RPC spine (turquoise shallows) |
| `edge` | `#7FE3D8` | Minion / minion-gateway / Envoy (aquamarine) |
| `database` | `#0E7C9D` | PostgreSQL / ClickHouse / VictoriaMetrics (cobalt) |
| `observability` | `#F6D04D` | Grafana / Alertmanager (Positano lemon) |
| `external` | `#5C8AA0` | Systems beyond the boundary (muted slate) |
| `critical` (relationship) | `#F6D04D` | The Kafka event spine + Minion I/O path |

- Canvas/background: foam off-white `#FDFCF7`.
- Element labels / ink: navy `#08313F`.
- Typeface: Inter (primary), Open Sans (fallback).
- Element text color is white on the navy/cobalt/turquoise fills; navy ink on lemon/aquamarine.
