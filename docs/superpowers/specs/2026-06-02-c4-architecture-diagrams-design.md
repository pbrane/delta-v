# Delta-V C4 Architecture Diagrams — Design

- **Date:** 2026-06-02
- **Status:** Approved (brainstorming phase)
- **Author:** David Hustace (with Claude Code)
- **Branch:** `worktree-docs+c4-architecture-diagrams` (off `develop`)

## Goal

Produce a set of beautiful, on-brand C4 architecture diagrams for Delta-V that
serve two publishing targets:

1. **The deltav.kiwi website** — embedded as crisp, resolution-independent SVG.
2. **PPTX presentations** — a set of standalone slide graphics exported as PNG.

The diagrams must reflect Delta-V's *actual* cloud-native architecture (Kafka
event spine, Minion-mediated I/O, Spring Boot daemons), not the upstream
OpenNMS / Karaf-monolith mental model that generic OpenNMS diagrams assume.

## Tooling decision

**Structurizr DSL** is the authoring tool. A single `workspace.dsl` file is the
**single source of truth** for the entire model; every diagram (view) is
generated from it.

Rationale:

- Canonical C4 tooling (authored by C4's creator), with correct C4 semantics.
- One model → all levels keeps the diagrams internally consistent and reduces
  drift as the architecture evolves.
- Native renderer produces the recognizable, clean C4 aesthetic; a custom theme
  (see below) makes it marketing-grade.

Rejected alternatives: Mermaid C4 (chosen originally for GitHub rendering, which
is not a goal here; weakest aesthetics), D2 (beautiful but not canonical C4 and
the user wants the Structurizr-native look), PlantUML/C4-PlantUML (no unified
model), bespoke hand-drawn SVG (max beauty but no maintainable model).

**Master export format is SVG** (resolution-independent, embeds natively in web
pages, imports into PowerPoint as a true vector shape). PNG @2× is the raster
fallback for slides.

## Repository layout

```
docs/architecture/c4/
  workspace.dsl          # model + views + styles — the single source of truth
  README.md              # how to render, edit, and export
  exports/
    svg/                 # vector — for deltav.kiwi embeds
    png/                 # 2x raster — for PPTX (on-brand + transparent variants)
  brand/
    amalfi-theme.md      # palette spec: hex values + tag -> style mapping
```

Delta-V `make` front-door targets (consistent with the project's build culture
where `make` is the single entry point):

- `make c4-edit` — launch Structurizr Lite in Docker for live editing/preview.
- `make c4-export` — render all views to SVG + PNG into `exports/` (automated,
  headless — see Render & export pipeline).

## The model and views

One model, three C4 zoom levels. **L4 (Code) is intentionally excluded** — C4
itself discourages it and it rots quickly.

### L1 — System Context (1 diagram)

- **Delta-V** as a single software system.
- **Actors:** Network Operator, NOC / SRE.
- **External systems:** the monitored network (devices reachable *only* via
  Minion), Grafana dashboards (consumed by operators), Alertmanager → on-call
  notification, and the inbound telemetry sources (SNMP, flows, syslog, traps).
- Purpose: the executive-summary view — website hero image and opening slide.

### L2 — Containers (1 primary diagram)

The deployable units and how they communicate:

- The ~12 Spring Boot daemons (alarmd, bsmd, collectd, discovery, enlinkd,
  eventtranslator, perspectivepollerd, pollerd, provisiond, syslogd, telemetryd,
  trapd).
- `minion-gateway` + Envoy (the IPC edge).
- Minion (mediates **all** network I/O to devices).
- Kafka (the event + RPC spine).
- Data stores: PostgreSQL, ClickHouse, VictoriaMetrics (+ vmagent).
- Observability/notification: Grafana, Alertmanager, alerts-forwarder.
- Supporting services as needed (flow-enricher, prometheus-writer /
  horizon-metric-bridge, alarms-materializer, alarms-kafka-publisher).

The **lemon accent** traces the critical path: the Kafka event spine and the
Minion I/O path.

### L3 — Components (4 diagrams)

1. **The daemon archetype** — one representative Spring Boot daemon documenting
   the shared pattern every daemon follows: Kafka event consumer → per-daemon
   EventExpander → business logic → JPA DAO → PostgreSQL, plus Kafka RPC out to
   Minion. Documents the pattern once instead of twelve times.
2. **Minion + minion-gateway** — the IPC heart: the gateway bridging Kafka
   (daemon side) ↔ gRPC (Minion side, via Envoy), and Minion mediating RPC and
   sink/telemetry traffic to/from devices.
3. **Flow & telemetry pipeline** — telemetryd (pure ingestion bridge) →
   flow-enricher (node-context enrichment) → ClickHouse, plus the Sentinel/flows
   path.
4. **Metrics & alarm pipeline** — horizon-metric-bridge / prometheus-writer →
   VictoriaMetrics → Grafana; and alarmd → alarms-materializer /
   alerts-forwarder → Alertmanager.

### Model accuracy

The exact Kafka topic wiring and container relationships will be **verified
against the actual `core/*` modules and the Docker Compose file** while writing
`workspace.dsl`. The diagrams must be correct, not merely plausible.

## The Amalfi theme

Inspired by the Amalfi coastline: the contrast between the deep Mediterranean
blue and the sun-lit turquoise/aquamarine shallows near the coast, with lemon
highlights drawn from Positano artwork.

Color encodes C4 semantics — depth from the user maps to depth of blue; edge and
transport elements take the turquoise shallows; lemon is reserved as the accent
for the critical path so the eye follows the most important flow without a
legend.

| Role (C4 tag) | Color | Meaning |
|---|---|---|
| Actors (people) | Terracotta `#E07A5F` | Positano rooftops / the human element |
| Delta-V core daemons | Deep sea navy `#06425C` | the deep water |
| Kafka / messaging spine | Turquoise `#19C3B2` | sunlit shallows — connective tissue |
| Minion-gateway / Envoy / edge | Aquamarine `#7FE3D8` | where sea meets coast |
| Data stores (cylinders) | Cobalt `#0E7C9D` | persistent depth |
| Observability (Grafana / Alertmanager) | Lemon `#F6D04D` | Positano lemons — alerting accent |
| External systems | Muted slate `#5C8AA0` | de-emphasized, beyond the boundary |
| Critical-path relationships | Lemon `#F6D04D` lines | the flow that matters |
| Canvas / background | Foam off-white `#FDFCF7` | sea foam |
| Element labels / ink | Navy ink `#08313F` | |

Typography: **Inter** as the primary typeface (clean, modern, open-source, web-safe),
falling back to **Open Sans** where Inter is unavailable; navy ink labels.

These tag → style mappings are codified in `docs/architecture/c4/brand/amalfi-theme.md`
and implemented as element/relationship styles in the `views` block of
`workspace.dsl`.

## Render & export pipeline

- **Author** in `workspace.dsl`.
- **Preview/refine** live in **Structurizr Lite** (Docker), via `make c4-edit`.
- **Export** each view to SVG (website) and PNG @2× (slides) via `make c4-export`.

**Automated export, native renderer.** The Structurizr CLI alone exports only to
intermediate formats (PlantUML / Mermaid / D2), which would lose the chosen
native Structurizr look. To keep the native aesthetic *and* automate, the
pipeline drives Structurizr Lite headlessly with a small headless-Chrome
(Playwright/Puppeteer) script that opens each view and triggers the built-in
SVG/PNG export. `make c4-export` runs this end to end so regeneration is one
command, not manual clicking.

PNG variants for slides: both a transparent-background and an on-brand
foam-background version, at slide-appropriate dimensions.

## Out of scope

- L4 (Code-level) diagrams.
- Building the actual PPTX deck (we produce the *graphics*; assembling slides is
  the consumer's job — a starter template may be added later if wanted).
- Any change to application code or runtime behavior. This is documentation only.

## Success criteria

- `workspace.dsl` renders all six views (L1 ×1, L2 ×1, L3 ×4) in Structurizr Lite
  without errors.
- The Amalfi theme is applied consistently across all views.
- `make c4-export` produces SVG + PNG for every view into `docs/architecture/c4/exports/`.
- Model content is verified accurate against `core/*` and the Compose file.
- A `README.md` documents how to edit and export.
