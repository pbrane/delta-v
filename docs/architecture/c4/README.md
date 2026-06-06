# Delta-V C4 Architecture Diagrams

C4 model (Context / Container / Component) for Delta-V, authored in Structurizr
DSL. `workspace.dsl` is the single source of truth; `brand/amalfi-theme.md`
documents the palette. Exports for the website (SVG) and slides (PNG) live in
`exports/`.

## Edit / preview

    make c4-edit        # serves Structurizr Lite at http://localhost:8080

## Export (SVG + PNG)

    make c4-export      # renders all views into exports/svg and exports/png

## Views

- L1 System Context — `SystemContext`
- L2 Containers — `Containers`
- L3 Components — `DaemonArchetype`, `MinionIpc`, `FlowPipeline`, `MetricsAlarms`
