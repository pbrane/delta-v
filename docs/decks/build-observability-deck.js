/* eslint-disable no-console */
// Builds: deltav-control-plane-observability.pptx
// Audience: DJ Gregor — long-time OpenNMS Horizon contributor evaluating delta-v.
// Goal: bridge Horizon-era mental model to delta-v reality, focused on
// observability state of the control plane.

const path = require("path");
const pptxgen = require("pptxgenjs");

const OUT = path.join(
  "/Users/david/development/src/opennms/delta-v/docs/decks",
  "deltav-control-plane-observability.pptx"
);

// --- Palette: Ocean Gradient + sharp accent ---
const COLOR = {
  bg:        "F4F7FA",   // light blue-gray for content slides
  bgDark:    "21295C",   // midnight for cover/closer
  primary:   "065A82",   // deep blue (60% weight)
  secondary: "1C7293",   // teal (supporting)
  accent:    "F26419",   // orange — sharp accent for emphasis
  ink:       "1E293B",   // body text
  muted:     "64748B",   // captions, axis labels
  rule:      "CBD5E1",   // hairlines, table borders
  white:     "FFFFFF",
  paneCtrl:  "E0F2F7",   // control-plane pane (light teal tint)
  paneData:  "FEEFE3",   // data-plane pane (light orange tint)
};

const FONT = { head: "Cambria", body: "Calibri" };

const pres = new pptxgen();
pres.layout = "LAYOUT_WIDE"; // 13.3" × 7.5"
pres.author = "Delta-V";
pres.title = "Delta-V Control Plane Observability — for DJ";

// ===== helpers =====
function header(slide, title, kicker) {
  // dark accent bar on left
  slide.addShape(pres.shapes.RECTANGLE, {
    x: 0, y: 0, w: 0.18, h: 7.5,
    fill: { color: COLOR.primary }, line: { color: COLOR.primary },
  });
  slide.addText(kicker || "DELTA-V · CONTROL PLANE OBSERVABILITY", {
    x: 0.55, y: 0.3, w: 12, h: 0.32,
    fontFace: FONT.body, fontSize: 10, bold: true,
    color: COLOR.muted, charSpacing: 4, margin: 0,
  });
  slide.addText(title, {
    x: 0.55, y: 0.55, w: 12.5, h: 0.7,
    fontFace: FONT.head, fontSize: 30, bold: true, color: COLOR.ink, margin: 0,
  });
}

function footer(slide, pageNum) {
  slide.addText("delta-v · v1.2.0-beta2 · 2026-04-25", {
    x: 0.55, y: 7.15, w: 8, h: 0.3,
    fontFace: FONT.body, fontSize: 9, color: COLOR.muted, margin: 0,
  });
  slide.addText(`${pageNum}`, {
    x: 12.4, y: 7.15, w: 0.5, h: 0.3,
    fontFace: FONT.body, fontSize: 9, color: COLOR.muted, align: "right", margin: 0,
  });
}

// =========================================================================
// Slide 1: Cover
// =========================================================================
{
  const s = pres.addSlide();
  s.background = { color: COLOR.bgDark };

  // Big "Delta-V" wordmark left
  s.addText("DELTA·V", {
    x: 0.7, y: 0.7, w: 6, h: 0.7,
    fontFace: FONT.head, fontSize: 28, bold: true,
    color: COLOR.white, charSpacing: 8, margin: 0,
  });
  s.addText("Control Plane Observability", {
    x: 0.7, y: 2.2, w: 12, h: 1.4,
    fontFace: FONT.head, fontSize: 56, bold: true, color: COLOR.white, margin: 0,
  });
  s.addText("Where Horizon ended.  Where you might come in.", {
    x: 0.7, y: 3.8, w: 12, h: 0.6,
    fontFace: FONT.body, fontSize: 22, italic: true,
    color: "9CB3D9", margin: 0,
  });

  // accent stripe
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.7, y: 4.7, w: 1.6, h: 0.06,
    fill: { color: COLOR.accent }, line: { color: COLOR.accent },
  });

  s.addText("Prepared for DJ Gregor", {
    x: 0.7, y: 4.9, w: 12, h: 0.4,
    fontFace: FONT.body, fontSize: 16, bold: true, color: COLOR.white, margin: 0,
  });
  s.addText([
    { text: "v1.2.0-beta2 just shipped", options: { color: "9CB3D9", breakLine: true } },
    { text: "13 Spring Boot daemons.  No Karaf.  No JMX MBeans.", options: { color: "9CB3D9" } },
  ], {
    x: 0.7, y: 5.3, w: 12, h: 1.2,
    fontFace: FONT.body, fontSize: 14, margin: 0,
  });

  s.addText("2026-04-25 · pbrane/delta-v", {
    x: 0.7, y: 7.0, w: 12, h: 0.3,
    fontFace: FONT.body, fontSize: 10,
    color: "6B82A6", margin: 0,
  });
}

// =========================================================================
// Slide 2: Why this deck
// =========================================================================
{
  const s = pres.addSlide();
  s.background = { color: COLOR.bg };
  header(s, "Why this deck",
    "ORIENTATION");

  // Two-column: left = "what you knew", right = "what to expect now"
  const colY = 1.6, colH = 5.0;
  const lcolX = 0.55, lcolW = 6.0;
  const rcolX = 6.85, rcolW = 6.0;

  // LEFT column
  s.addShape(pres.shapes.RECTANGLE, {
    x: lcolX, y: colY, w: lcolW, h: colH,
    fill: { color: COLOR.white }, line: { color: COLOR.rule, width: 0.75 },
    shadow: { type: "outer", blur: 6, offset: 2, angle: 90, color: "000000", opacity: 0.06 },
  });
  s.addShape(pres.shapes.RECTANGLE, {
    x: lcolX, y: colY, w: 0.08, h: colH,
    fill: { color: COLOR.secondary }, line: { color: COLOR.secondary },
  });
  s.addText("Where you left off", {
    x: lcolX + 0.3, y: colY + 0.2, w: lcolW - 0.4, h: 0.45,
    fontFace: FONT.head, fontSize: 20, bold: true, color: COLOR.primary, margin: 0,
  });
  s.addText([
    { text: "Karaf-hosted OSGi bundles", options: { bullet: true, breakLine: true } },
    { text: "Monolithic opennms-services.jar", options: { bullet: true, breakLine: true } },
    { text: "Newts / Cassandra for time-series", options: { bullet: true, breakLine: true } },
    { text: "JMX MBeans for instrumentation", options: { bullet: true, breakLine: true } },
    { text: "ActiveMQ + ServiceMix bundles for IPC", options: { bullet: true, breakLine: true } },
    { text: "Hibernate 3.x, javax.persistence", options: { bullet: true, breakLine: true } },
    { text: "Spring 4.2.x (OpenNMS-patched)", options: { bullet: true } },
  ], {
    x: lcolX + 0.3, y: colY + 0.85, w: lcolW - 0.5, h: colH - 1.0,
    fontFace: FONT.body, fontSize: 14, color: COLOR.ink,
    paraSpaceAfter: 8, margin: 0,
  });

  // RIGHT column
  s.addShape(pres.shapes.RECTANGLE, {
    x: rcolX, y: colY, w: rcolW, h: colH,
    fill: { color: COLOR.white }, line: { color: COLOR.rule, width: 0.75 },
    shadow: { type: "outer", blur: 6, offset: 2, angle: 90, color: "000000", opacity: 0.06 },
  });
  s.addShape(pres.shapes.RECTANGLE, {
    x: rcolX, y: colY, w: 0.08, h: colH,
    fill: { color: COLOR.accent }, line: { color: COLOR.accent },
  });
  s.addText("What delta-v looks like now", {
    x: rcolX + 0.3, y: colY + 0.2, w: rcolW - 0.4, h: 0.45,
    fontFace: FONT.head, fontSize: 20, bold: true, color: COLOR.primary, margin: 0,
  });
  s.addText([
    { text: "13 standalone Spring Boot 4 daemons (Java 21)", options: { bullet: true, breakLine: true } },
    { text: "Hibernate 7, jakarta.persistence", options: { bullet: true, breakLine: true } },
    { text: "Karaf, OSGi, opennms-services — all gone", options: { bullet: true, breakLine: true } },
    { text: "Kafka for inter-daemon events + Minion IPC", options: { bullet: true, breakLine: true } },
    { text: "VictoriaMetrics + Grafana for time-series", options: { bullet: true, breakLine: true } },
    { text: "Micrometer + /actuator/prometheus for metrics", options: { bullet: true, breakLine: true } },
    { text: "Horizon code consumed as pre-built JARs", options: { bullet: true } },
  ], {
    x: rcolX + 0.3, y: colY + 0.85, w: rcolW - 0.5, h: colH - 1.0,
    fontFace: FONT.body, fontSize: 14, color: COLOR.ink,
    paraSpaceAfter: 8, margin: 0,
  });

  // Bottom takeaway
  s.addText([
    { text: "TAKEAWAY  ", options: { bold: true, color: COLOR.accent, charSpacing: 4 } },
    { text: "Domain model and DAOs are recognizably OpenNMS. Everything else is a clean-room refactor.", options: { color: COLOR.ink } },
  ], {
    x: 0.55, y: 6.85, w: 12.4, h: 0.4,
    fontFace: FONT.body, fontSize: 12, italic: true, margin: 0,
  });

  footer(s, 2);
}

// =========================================================================
// Slide 3: Architecture diagram
// =========================================================================
{
  const s = pres.addSlide();
  s.background = { color: COLOR.bg };
  header(s, "The big picture", "ARCHITECTURE");

  // Three zones: Minion (left), Core daemons (middle), Observability (right)
  const zoneY = 1.5, zoneH = 5.1;

  // Zone 1: Minion (left)
  const mX = 0.55, mW = 2.4;
  s.addShape(pres.shapes.RECTANGLE, {
    x: mX, y: zoneY, w: mW, h: zoneH,
    fill: { color: COLOR.white }, line: { color: COLOR.rule, width: 0.75 },
  });
  s.addText("MINION", {
    x: mX, y: zoneY + 0.15, w: mW, h: 0.35,
    fontFace: FONT.body, fontSize: 11, bold: true, color: COLOR.muted,
    align: "center", charSpacing: 4, margin: 0,
  });
  s.addShape(pres.shapes.ROUNDED_RECTANGLE, {
    x: mX + 0.3, y: zoneY + 0.7, w: mW - 0.6, h: 1.2, rectRadius: 0.08,
    fill: { color: COLOR.secondary }, line: { color: COLOR.secondary },
  });
  s.addText("Minion", {
    x: mX + 0.3, y: zoneY + 0.7, w: mW - 0.6, h: 0.5,
    fontFace: FONT.head, fontSize: 16, bold: true, color: COLOR.white,
    align: "center", valign: "middle", margin: 0,
  });
  s.addText([
    { text: "SNMP / ICMP / Detect", options: { breakLine: true } },
    { text: "Trap + Syslog listener", options: { breakLine: true } },
    { text: "NetFlow / sFlow / IPFIX", options: {} },
  ], {
    x: mX + 0.3, y: zoneY + 1.15, w: mW - 0.6, h: 0.7,
    fontFace: FONT.body, fontSize: 9.5, color: COLOR.white,
    align: "center", margin: 0,
  });
  s.addText([
    { text: "Edge collector.", options: { italic: true, breakLine: true } },
    { text: "Owns all network I/O.", options: { italic: true, breakLine: true } },
    { text: "No daemon ever polls", options: { italic: true, breakLine: true } },
    { text: "the network directly.", options: { italic: true } },
  ], {
    x: mX + 0.2, y: zoneY + 2.0, w: mW - 0.4, h: 1.0,
    fontFace: FONT.body, fontSize: 10, color: COLOR.muted,
    align: "center", margin: 0,
  });
  // minion-boot chip: the Boot4 daemon on the Minion side
  s.addShape(pres.shapes.RECTANGLE, {
    x: mX + 0.3, y: zoneY + 3.3, w: mW - 0.6, h: 0.45,
    fill: { color: COLOR.paneCtrl }, line: { color: COLOR.secondary, width: 0.5 },
  });
  s.addText("minion-boot", {
    x: mX + 0.3, y: zoneY + 3.3, w: mW - 0.6, h: 0.45,
    fontFace: FONT.body, fontSize: 10, bold: true, color: COLOR.primary,
    align: "center", valign: "middle", margin: 0,
  });
  s.addText("Spring Boot 4 orchestrator", {
    x: mX + 0.2, y: zoneY + 3.85, w: mW - 0.4, h: 0.3,
    fontFace: FONT.body, fontSize: 9, italic: true, color: COLOR.muted,
    align: "center", margin: 0,
  });

  // Zone 2: Core daemons (middle)
  const cX = 3.4, cW = 5.7;
  s.addShape(pres.shapes.RECTANGLE, {
    x: cX, y: zoneY, w: cW, h: zoneH,
    fill: { color: COLOR.white }, line: { color: COLOR.rule, width: 0.75 },
  });
  s.addText("CORE — 12 SPRING BOOT DAEMONS", {
    x: cX, y: zoneY + 0.15, w: cW, h: 0.35,
    fontFace: FONT.body, fontSize: 11, bold: true, color: COLOR.muted,
    align: "center", charSpacing: 4, margin: 0,
  });

  // First 12 daemons in a 4×3 grid; minion-boot gets its own centered slot
  // below as a "different role" (it's the Minion-side daemon, not a Core
  // daemon in the same sense).
  const daemons = [
    "pollerd", "collectd", "alarmd", "bsmd",
    "trapd", "syslogd", "discovery", "enlinkd",
    "provisiond", "perspective", "telemetryd", "eventtrans",
  ];
  const chipW = 1.2, chipH = 0.45;
  const chipGapX = 0.18, chipGapY = 0.15;
  const chipsX0 = cX + 0.25;
  const chipsY0 = zoneY + 0.65;
  daemons.forEach((d, i) => {
    const col = i % 4, row = Math.floor(i / 4);
    const x = chipsX0 + col * (chipW + chipGapX);
    const y = chipsY0 + row * (chipH + chipGapY);
    s.addShape(pres.shapes.RECTANGLE, {
      x, y, w: chipW, h: chipH,
      fill: { color: COLOR.paneCtrl }, line: { color: COLOR.secondary, width: 0.5 },
    });
    s.addText(d, {
      x, y, w: chipW, h: chipH,
      fontFace: FONT.body, fontSize: 10, bold: true, color: COLOR.primary,
      align: "center", valign: "middle", margin: 0,
    });
  });

  // Bottom note inside Core zone
  s.addText("each = standalone JVM, /actuator/prometheus, Hibernate 7, Kafka client", {
    x: cX + 0.2, y: zoneY + 2.55, w: cW - 0.4, h: 0.35,
    fontFace: FONT.body, fontSize: 10, italic: true, color: COLOR.muted,
    align: "center", margin: 0,
  });

  // Lower band: shared substrate
  s.addShape(pres.shapes.RECTANGLE, {
    x: cX + 0.25, y: zoneY + 3.95, w: cW - 0.5, h: 1.0,
    fill: { color: COLOR.primary }, line: { color: COLOR.primary },
  });
  s.addText("Postgres  ·  Kafka  ·  Twin API  ·  Spring Boot Actuator", {
    x: cX + 0.25, y: zoneY + 3.95, w: cW - 0.5, h: 0.5,
    fontFace: FONT.body, fontSize: 12, bold: true, color: COLOR.white,
    align: "center", valign: "middle", margin: 0,
  });
  s.addText("shared infrastructure", {
    x: cX + 0.25, y: zoneY + 4.45, w: cW - 0.5, h: 0.4,
    fontFace: FONT.body, fontSize: 10, italic: true, color: "B7C9D9",
    align: "center", valign: "middle", margin: 0,
  });

  // Zone 3: Observability (right)
  const oX = 9.55, oW = 3.4;
  s.addShape(pres.shapes.RECTANGLE, {
    x: oX, y: zoneY, w: oW, h: zoneH,
    fill: { color: COLOR.white }, line: { color: COLOR.rule, width: 0.75 },
  });
  s.addText("OBSERVABILITY", {
    x: oX, y: zoneY + 0.15, w: oW, h: 0.35,
    fontFace: FONT.body, fontSize: 11, bold: true, color: COLOR.muted,
    align: "center", charSpacing: 4, margin: 0,
  });

  // stack: prometheus-writer → VM → Grafana, plus K8s-scraper future
  const stackBoxes = [
    { label: "deltav-timeseries",     sub: "Kafka topic",       fill: COLOR.paneData },
    { label: "prometheus-writer",     sub: "Kafka → remote-write", fill: COLOR.paneData },
    { label: "VictoriaMetrics",       sub: "tsdb",              fill: COLOR.paneData },
    { label: "Grafana",               sub: "dashboards",        fill: COLOR.paneData },
  ];
  let yCursor = zoneY + 0.65;
  const sboxW = oW - 0.5, sboxH = 0.55;
  stackBoxes.forEach(b => {
    s.addShape(pres.shapes.RECTANGLE, {
      x: oX + 0.25, y: yCursor, w: sboxW, h: sboxH,
      fill: { color: b.fill }, line: { color: COLOR.accent, width: 0.5 },
    });
    s.addText([
      { text: b.label, options: { bold: true, color: COLOR.ink, breakLine: true } },
      { text: b.sub, options: { fontSize: 9, color: COLOR.muted, italic: true } },
    ], {
      x: oX + 0.25, y: yCursor, w: sboxW, h: sboxH,
      fontFace: FONT.body, fontSize: 11, align: "center", valign: "middle", margin: 0,
    });
    yCursor += sboxH + 0.1;
  });

  // future K8s scraper box
  s.addShape(pres.shapes.RECTANGLE, {
    x: oX + 0.25, y: yCursor + 0.15, w: sboxW, h: 0.6,
    fill: { color: COLOR.white }, line: { color: COLOR.muted, width: 0.5, dashType: "dash" },
  });
  s.addText([
    { text: "K8s scraper / HPA  ", options: { bold: true, color: COLOR.muted, breakLine: true } },
    { text: "(v1.3 — control plane)", options: { fontSize: 9, color: COLOR.muted, italic: true } },
  ], {
    x: oX + 0.25, y: yCursor + 0.15, w: sboxW, h: 0.6,
    fontFace: FONT.body, fontSize: 11, align: "center", valign: "middle", margin: 0,
  });

  // Arrows: Minion ↔ Core, Core → Observability
  // Note: arrows span the (narrow) gap between zone boxes, but labels extend
  // past the gap into the neighboring zones so they don't wrap awkwardly.
  s.addShape(pres.shapes.LINE, {
    x: mX + mW, y: zoneY + 1.3, w: cX - (mX + mW), h: 0,
    line: { color: COLOR.secondary, width: 2.5, endArrowType: "triangle" },
  });
  s.addShape(pres.shapes.LINE, {
    x: cX, y: zoneY + 1.6, w: -(cX - (mX + mW)), h: 0,
    line: { color: COLOR.secondary, width: 2.5, endArrowType: "triangle" },
  });
  s.addText("Kafka IPC", {
    x: mX + mW - 0.5, y: zoneY + 0.85, w: (cX - (mX + mW)) + 1.0, h: 0.32,
    fontFace: FONT.body, fontSize: 11, italic: true, bold: true, color: COLOR.secondary,
    align: "center", margin: 0,
  });
  s.addText("(rc1/rc2: gRPC)", {
    x: mX + mW - 0.5, y: zoneY + 1.85, w: (cX - (mX + mW)) + 1.0, h: 0.3,
    fontFace: FONT.body, fontSize: 9, italic: true, color: COLOR.muted,
    align: "center", margin: 0,
  });

  // Core → Observability arrow (data plane)
  s.addShape(pres.shapes.LINE, {
    x: cX + cW, y: zoneY + 4.4, w: oX - (cX + cW), h: 0,
    line: { color: COLOR.accent, width: 3, endArrowType: "triangle" },
  });
  s.addText("data plane", {
    x: cX + cW - 0.5, y: zoneY + 3.95, w: (oX - (cX + cW)) + 1.0, h: 0.3,
    fontFace: FONT.body, fontSize: 10, bold: true, italic: true, color: COLOR.accent,
    align: "center", margin: 0,
  });

  // Daemons → K8s scraper (control plane, dashed). No arrow label — the
  // destination box "K8s scraper / HPA (v1.3 — control plane)" already
  // names this flow, and the gap between the Core and Observability zones
  // is too narrow for a label without colliding with zone borders.
  s.addShape(pres.shapes.LINE, {
    x: cX + cW, y: zoneY + 1.3, w: oX - (cX + cW), h: 0,
    line: { color: COLOR.muted, width: 1.5, dashType: "dash", endArrowType: "triangle" },
  });

  footer(s, 3);
}

// =========================================================================
// Slide 4: Two planes
// =========================================================================
{
  const s = pres.addSlide();
  s.background = { color: COLOR.bg };
  header(s, "Two planes, two purposes", "ARCHITECTURAL TENANT");

  const planeY = 1.5, planeH = 5.1;
  const planeW = 6.05;

  // CONTROL PLANE
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.55, y: planeY, w: planeW, h: planeH,
    fill: { color: COLOR.paneCtrl }, line: { color: COLOR.secondary, width: 1 },
  });
  s.addText("CONTROL PLANE", {
    x: 0.7, y: planeY + 0.2, w: planeW - 0.3, h: 0.4,
    fontFace: FONT.body, fontSize: 12, bold: true, color: COLOR.primary,
    charSpacing: 4, margin: 0,
  });
  s.addText("Application observability", {
    x: 0.7, y: planeY + 0.55, w: planeW - 0.3, h: 0.6,
    fontFace: FONT.head, fontSize: 24, bold: true, color: COLOR.ink, margin: 0,
  });
  s.addText("How the daemons themselves are doing", {
    x: 0.7, y: planeY + 1.1, w: planeW - 0.3, h: 0.4,
    fontFace: FONT.body, fontSize: 12, italic: true, color: COLOR.muted, margin: 0,
  });
  s.addText([
    { text: "Metric prefix: ", options: { bold: true } },
    { text: "deltav_<daemon>_*", options: { fontFace: "Consolas", color: COLOR.primary } },
    { text: " (native Micrometer)", options: { italic: true, color: COLOR.muted, breakLine: true } },
    { text: "Surface: ", options: { bold: true } },
    { text: "/actuator/prometheus", options: { fontFace: "Consolas", color: COLOR.primary } },
    { text: " on each daemon", options: { italic: true, color: COLOR.muted, breakLine: true } },
    { text: "Consumer: ", options: { bold: true } },
    { text: "future K8s scraper / HPA-Adapter", options: { italic: true, color: COLOR.muted, breakLine: true } },
    { text: "Examples: ", options: { bold: true } },
    { text: "polls dispatched, alarm-create rate, RPC duration p99, queue depth", options: { italic: true, color: COLOR.muted } },
  ], {
    x: 0.7, y: planeY + 1.7, w: planeW - 0.3, h: 2.5,
    fontFace: FONT.body, fontSize: 12, color: COLOR.ink,
    paraSpaceAfter: 6, margin: 0,
  });
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.7, y: planeY + 4.2, w: planeW - 0.3, h: 0.04,
    fill: { color: COLOR.secondary }, line: { color: COLOR.secondary },
  });
  s.addText("Drives autoscaling decisions, not user dashboards.", {
    x: 0.7, y: planeY + 4.35, w: planeW - 0.3, h: 0.6,
    fontFace: FONT.body, fontSize: 13, bold: true, color: COLOR.primary, margin: 0,
  });

  // DATA PLANE
  const dpX = 7.05;
  s.addShape(pres.shapes.RECTANGLE, {
    x: dpX, y: planeY, w: planeW, h: planeH,
    fill: { color: COLOR.paneData }, line: { color: COLOR.accent, width: 1 },
  });
  s.addText("DATA PLANE", {
    x: dpX + 0.15, y: planeY + 0.2, w: planeW - 0.3, h: 0.4,
    fontFace: FONT.body, fontSize: 12, bold: true, color: COLOR.accent,
    charSpacing: 4, margin: 0,
  });
  s.addText("Monitored network", {
    x: dpX + 0.15, y: planeY + 0.55, w: planeW - 0.3, h: 0.6,
    fontFace: FONT.head, fontSize: 24, bold: true, color: COLOR.ink, margin: 0,
  });
  s.addText("How the network being monitored is doing", {
    x: dpX + 0.15, y: planeY + 1.1, w: planeW - 0.3, h: 0.4,
    fontFace: FONT.body, fontSize: 12, italic: true, color: COLOR.muted, margin: 0,
  });
  s.addText([
    { text: "Metric prefix: ", options: { bold: true } },
    { text: "opennms_*", options: { fontFace: "Consolas", color: COLOR.accent } },
    { text: " (translated from CollectionSet)", options: { italic: true, color: COLOR.muted, breakLine: true } },
    { text: "Pipeline: ", options: { bold: true } },
    { text: "TimeseriesBatch protobuf → Kafka → VM", options: { italic: true, color: COLOR.muted, breakLine: true } },
    { text: "Consumer: ", options: { bold: true } },
    { text: "Grafana, future thresholder, replaces Newts", options: { italic: true, color: COLOR.muted, breakLine: true } },
    { text: "Examples: ", options: { bold: true } },
    { text: "ICMP response time, ifInOctets, BGP peer state, CPU load", options: { italic: true, color: COLOR.muted } },
  ], {
    x: dpX + 0.15, y: planeY + 1.7, w: planeW - 0.3, h: 2.5,
    fontFace: FONT.body, fontSize: 12, color: COLOR.ink,
    paraSpaceAfter: 6, margin: 0,
  });
  s.addShape(pres.shapes.RECTANGLE, {
    x: dpX + 0.15, y: planeY + 4.2, w: planeW - 0.3, h: 0.04,
    fill: { color: COLOR.accent }, line: { color: COLOR.accent },
  });
  s.addText("Drives operator dashboards and SLOs.", {
    x: dpX + 0.15, y: planeY + 4.35, w: planeW - 0.3, h: 0.6,
    fontFace: FONT.body, fontSize: 13, bold: true, color: COLOR.accent, margin: 0,
  });

  // Bottom rule callout
  s.addText([
    { text: "RULE  ", options: { bold: true, color: COLOR.accent, charSpacing: 4 } },
    { text: "These never share a TSDB. VictoriaMetrics holds data plane only.", options: { color: COLOR.ink } },
  ], {
    x: 0.55, y: 6.85, w: 12.4, h: 0.4,
    fontFace: FONT.body, fontSize: 12, italic: true, margin: 0,
  });

  footer(s, 4);
}

// =========================================================================
// Slide 5: The horizon-metric bridge
// =========================================================================
{
  const s = pres.addSlide();
  s.background = { color: COLOR.bg };
  header(s, "The horizon-metric bridge", "BRIDGING WHAT WE INHERITED");

  // Context paragraph
  s.addText([
    { text: "Horizon code (now consumed as pre-built JARs from ", options: {} },
    { text: "pbrane/delta-v-horizon", options: { fontFace: "Consolas", color: COLOR.primary } },
    { text: ") still emits its meters via Dropwizard ", options: {} },
    { text: "MetricRegistry", options: { fontFace: "Consolas", color: COLOR.primary } },
    { text: ". Spring Boot speaks Micrometer. Bridge between the two without editing horizon source.", options: {} },
  ], {
    x: 0.55, y: 1.55, w: 12.4, h: 0.85,
    fontFace: FONT.body, fontSize: 13, color: COLOR.ink, margin: 0,
  });

  // Three-step diagram showing the bridge flow
  const stepY = 2.7, stepH = 1.5;
  const steps = [
    {
      title: "Horizon Dropwizard meter",
      sub: "OnmsAlarm.created counter",
      detail: "MetricRegistry.counter(...).inc()  in horizon source",
      color: COLOR.secondary,
    },
    {
      title: "HorizonMetricsBridge",
      sub: "MeterBinder + MetricRegistryListener",
      detail: "Mirrors each Dropwizard meter into a Micrometer meter",
      color: COLOR.primary,
    },
    {
      title: "/actuator/prometheus",
      sub: "scrape-target",
      detail: "Surfaces as opennms_<...>_total{tag=\"...\"}",
      color: COLOR.accent,
    },
  ];
  const stepW = 3.85, gap = 0.35;
  steps.forEach((step, i) => {
    const x = 0.55 + i * (stepW + gap);
    s.addShape(pres.shapes.RECTANGLE, {
      x, y: stepY, w: stepW, h: stepH,
      fill: { color: COLOR.white }, line: { color: COLOR.rule, width: 0.75 },
      shadow: { type: "outer", blur: 6, offset: 2, angle: 90, color: "000000", opacity: 0.06 },
    });
    s.addShape(pres.shapes.RECTANGLE, {
      x, y: stepY, w: stepW, h: 0.07,
      fill: { color: step.color }, line: { color: step.color },
    });
    s.addShape(pres.shapes.OVAL, {
      x: x + 0.15, y: stepY + 0.18, w: 0.5, h: 0.5,
      fill: { color: step.color }, line: { color: step.color },
    });
    s.addText(`${i + 1}`, {
      x: x + 0.15, y: stepY + 0.18, w: 0.5, h: 0.5,
      fontFace: FONT.head, fontSize: 18, bold: true, color: COLOR.white,
      align: "center", valign: "middle", margin: 0,
    });
    s.addText(step.title, {
      x: x + 0.75, y: stepY + 0.2, w: stepW - 0.85, h: 0.4,
      fontFace: FONT.head, fontSize: 14, bold: true, color: COLOR.ink, margin: 0,
    });
    s.addText(step.sub, {
      x: x + 0.75, y: stepY + 0.55, w: stepW - 0.85, h: 0.3,
      fontFace: "Consolas", fontSize: 10.5, color: step.color, margin: 0,
    });
    s.addText(step.detail, {
      x: x + 0.2, y: stepY + 0.95, w: stepW - 0.3, h: 0.5,
      fontFace: FONT.body, fontSize: 11, italic: true, color: COLOR.muted, margin: 0,
    });
    if (i < steps.length - 1) {
      s.addShape(pres.shapes.LINE, {
        x: x + stepW + 0.02, y: stepY + stepH / 2, w: gap - 0.04, h: 0,
        line: { color: COLOR.muted, width: 2, endArrowType: "triangle" },
      });
    }
  });

  // Below: deltav_* sibling note + 5 idioms hook reference
  const noteY = 4.6, noteH = 2.0;
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.55, y: noteY, w: 12.4, h: noteH,
    fill: { color: COLOR.bgDark }, line: { color: COLOR.bgDark },
  });
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.55, y: noteY, w: 0.08, h: noteH,
    fill: { color: COLOR.accent }, line: { color: COLOR.accent },
  });
  s.addText("And in parallel:  the deltav_* native side", {
    x: 0.85, y: noteY + 0.2, w: 12, h: 0.45,
    fontFace: FONT.head, fontSize: 18, bold: true, color: COLOR.white, margin: 0,
  });
  s.addText([
    { text: "deltav_<daemon>_*", options: { fontFace: "Consolas", color: COLOR.accent, bold: true } },
    { text: " counters are Micrometer-native — added directly in delta-v code via 5 reusable hook idioms (next slide). They live alongside the bridged ", options: { color: "B7C9D9" } },
    { text: "opennms_*", options: { fontFace: "Consolas", color: "B7C9D9" } },
    { text: " meters, so the same daemon's ", options: { color: "B7C9D9" } },
    { text: "/actuator/prometheus", options: { fontFace: "Consolas", color: "B7C9D9" } },
    { text: " emits both. The two-prefix convention lets ops tell which side a meter came from at a glance.", options: { color: "B7C9D9" } },
  ], {
    x: 0.85, y: noteY + 0.75, w: 12, h: 1.2,
    fontFace: FONT.body, fontSize: 13, margin: 0,
  });

  footer(s, 5);
}

// =========================================================================
// Slide 6: 5 reusable hook idioms
// =========================================================================
{
  const s = pres.addSlide();
  s.background = { color: COLOR.bg };
  header(s, "Five idioms across thirteen daemons", "INSTRUMENTATION PATTERN LIBRARY");

  s.addText("The Boot4 daemons all wrap horizon classes. Each instrumentation hook fits one of these five patterns. Discovered empirically while shipping v1.2.0-beta2; codified afterward as a reusable cheat sheet.", {
    x: 0.55, y: 1.55, w: 12.4, h: 0.65,
    fontFace: FONT.body, fontSize: 13, italic: true, color: COLOR.muted, margin: 0,
  });

  // 5 cards in a row
  const cardY = 2.45, cardH = 4.2;
  const cardW = 2.42, cardGap = 0.13;
  const idioms = [
    {
      n: "1",
      name: "No-op listener replace",
      example: "alarmd",
      desc: "Replace anonymous no-op Listener {} with a named CountingX impl.",
      bonus: "Doubles as a null-op stub fix.",
    },
    {
      n: "2",
      name: "Lambda wrap inline",
      example: "bsmd",
      desc: "Wrap a @Bean-returned lambda with Timer.record() + counters in the @Bean method itself.",
      bonus: "Zero new files.",
    },
    {
      n: "3",
      name: "Subclass horizon class",
      example: "pollerd, trapd, syslogd",
      desc: "extends horizon class, override entry method, increment counters, call super.",
      bonus: "Best when the public seam is clean.",
    },
    {
      n: "4",
      name: "Decorator on choke point",
      example: "enlinkd, provisiond",
      desc: "When N implementations all funnel through one interface, wrap the interface once.",
      bonus: "1 wrap covers all N.",
    },
    {
      n: "5",
      name: "BeanPostProcessor",
      example: "minion-boot",
      desc: "Auto-wrap every bean of type X. For open-ended sets (RpcModules, listeners).",
      bonus: "Future-proof.",
    },
  ];
  idioms.forEach((idiom, i) => {
    const x = 0.55 + i * (cardW + cardGap);
    s.addShape(pres.shapes.RECTANGLE, {
      x, y: cardY, w: cardW, h: cardH,
      fill: { color: COLOR.white }, line: { color: COLOR.rule, width: 0.75 },
      shadow: { type: "outer", blur: 6, offset: 2, angle: 90, color: "000000", opacity: 0.07 },
    });
    s.addShape(pres.shapes.RECTANGLE, {
      x, y: cardY, w: cardW, h: 0.07,
      fill: { color: COLOR.primary }, line: { color: COLOR.primary },
    });
    s.addText(idiom.n, {
      x, y: cardY + 0.25, w: cardW, h: 0.7,
      fontFace: FONT.head, fontSize: 42, bold: true, color: COLOR.primary,
      align: "center", margin: 0,
    });
    s.addText(idiom.name, {
      x: x + 0.15, y: cardY + 1.1, w: cardW - 0.3, h: 0.7,
      fontFace: FONT.head, fontSize: 13.5, bold: true, color: COLOR.ink,
      align: "center", margin: 0,
    });
    // example chip
    s.addShape(pres.shapes.RECTANGLE, {
      x: x + 0.25, y: cardY + 1.85, w: cardW - 0.5, h: 0.32,
      fill: { color: COLOR.paneCtrl }, line: { color: COLOR.secondary, width: 0.5 },
    });
    s.addText(idiom.example, {
      x: x + 0.25, y: cardY + 1.85, w: cardW - 0.5, h: 0.32,
      fontFace: "Consolas", fontSize: 9, bold: true, color: COLOR.primary,
      align: "center", valign: "middle", margin: 0,
    });
    s.addText(idiom.desc, {
      x: x + 0.2, y: cardY + 2.3, w: cardW - 0.4, h: 1.3,
      fontFace: FONT.body, fontSize: 11, color: COLOR.ink, margin: 0,
    });
    // bottom italic
    s.addText(idiom.bonus, {
      x: x + 0.2, y: cardY + 3.7, w: cardW - 0.4, h: 0.4,
      fontFace: FONT.body, fontSize: 10, italic: true, color: COLOR.accent, margin: 0,
    });
  });

  s.addText([
    { text: "MEMORY  ", options: { bold: true, color: COLOR.accent, charSpacing: 4 } },
    { text: "feedback_daemon_instrumentation_patterns — bytecode-audit horizon's class first, then pick the idiom. No 6th idiom needed across all 13.", options: { color: COLOR.ink } },
  ], {
    x: 0.55, y: 6.85, w: 12.4, h: 0.4,
    fontFace: FONT.body, fontSize: 11, italic: true, margin: 0,
  });

  footer(s, 6);
}

// =========================================================================
// Slide 7: What the data plane looks like in flight
// =========================================================================
{
  const s = pres.addSlide();
  s.background = { color: COLOR.bg };
  header(s, "What the data plane carries today", "v1.2.0-beta2 STATE");

  // Left: producer pipeline boxes
  const lY = 1.55;
  s.addText("Producers → deltav-timeseries Kafka topic", {
    x: 0.55, y: lY, w: 6.5, h: 0.4,
    fontFace: FONT.head, fontSize: 16, bold: true, color: COLOR.primary, margin: 0,
  });

  const producers = [
    { name: "collectd", emits: "SNMP CollectionSets per scheduled poll", since: "v1.1.x" },
    { name: "pollerd", emits: "ICMP / TCP / HTTP response-time samples", since: "v1.2.0-beta2 (just shipped)" },
    { name: "perspectivepollerd", emits: "(Phase 3 deferred)", since: "post-beta2" },
    { name: "minion-snmp-collector", emits: "(future)", since: "v1.3+" },
  ];

  let py = lY + 0.5;
  producers.forEach((p, i) => {
    const fill = i < 2 ? COLOR.paneData : COLOR.bg;
    const border = i < 2 ? COLOR.accent : COLOR.muted;
    const dim = i < 2 ? false : true;
    s.addShape(pres.shapes.RECTANGLE, {
      x: 0.55, y: py, w: 6.5, h: 0.85,
      fill: { color: fill }, line: { color: border, width: 0.75, dashType: dim ? "dash" : "solid" },
    });
    s.addText(p.name, {
      x: 0.7, y: py + 0.05, w: 3, h: 0.35,
      fontFace: "Consolas", fontSize: 13, bold: true,
      color: dim ? COLOR.muted : COLOR.ink, margin: 0,
    });
    s.addText(p.since, {
      x: 4.4, y: py + 0.05, w: 2.5, h: 0.35,
      fontFace: FONT.body, fontSize: 10, italic: true,
      color: dim ? COLOR.muted : COLOR.accent, align: "right", margin: 0,
    });
    s.addText(p.emits, {
      x: 0.7, y: py + 0.4, w: 6.2, h: 0.4,
      fontFace: FONT.body, fontSize: 11,
      color: dim ? COLOR.muted : COLOR.ink, margin: 0,
    });
    py += 0.95;
  });

  // Right: live evidence query
  const rX = 7.4;
  s.addText("Live evidence — Apr 25 smoke", {
    x: rX, y: lY, w: 5.6, h: 0.4,
    fontFace: FONT.head, fontSize: 16, bold: true, color: COLOR.primary, margin: 0,
  });

  // Code block for the curl + sample
  s.addShape(pres.shapes.RECTANGLE, {
    x: rX, y: lY + 0.5, w: 5.6, h: 4.5,
    fill: { color: COLOR.bgDark }, line: { color: COLOR.bgDark },
  });
  s.addText([
    { text: "$ curl -sG 'localhost:18428/api/v1/query'", options: { color: "9CB3D9", breakLine: true } },
    { text: "    --data-urlencode 'query={producer=\"pollerd\"}'", options: { color: "9CB3D9", breakLine: true } },
    { text: " ", options: { breakLine: true } },
    { text: "{", options: { color: "C5D9F1", breakLine: true } },
    { text: "  \"status\": \"success\",", options: { color: "C5D9F1", breakLine: true } },
    { text: "  \"data\": { \"resultType\": \"vector\",", options: { color: "C5D9F1", breakLine: true } },
    { text: "    \"result\": [{", options: { color: "C5D9F1", breakLine: true } },
    { text: "      \"metric\": {", options: { color: "C5D9F1", breakLine: true } },
    { text: "        \"__name__\": \"opennms_response_time_response\",", options: { color: COLOR.accent, breakLine: true } },
    { text: "        \"producer\": \"pollerd\",", options: { color: COLOR.accent, breakLine: true } },
    { text: "        \"location\": \"l8opensim-lab\",", options: { color: "C5D9F1", breakLine: true } },
    { text: "        \"node_label\": \"lab-01-core-rtr-01\",", options: { color: "C5D9F1", breakLine: true } },
    { text: "        \"foreign_source\": \"l8opensim-lab\",", options: { color: "C5D9F1", breakLine: true } },
    { text: "        \"collection_package\": \"ICMP\",", options: { color: "C5D9F1", breakLine: true } },
    { text: "        \"resource_type\": \"monitoredService\"", options: { color: "C5D9F1", breakLine: true } },
    { text: "      },", options: { color: "C5D9F1", breakLine: true } },
    { text: "      \"value\": [ 1745604821, \"3.2\" ]", options: { color: "C5D9F1", breakLine: true } },
    { text: "    }] } }", options: { color: "C5D9F1" } },
  ], {
    x: rX + 0.2, y: lY + 0.65, w: 5.4, h: 4.2,
    fontFace: "Consolas", fontSize: 10, margin: 0,
  });

  s.addText([
    { text: "INVARIANT  ", options: { bold: true, color: COLOR.accent, charSpacing: 4 } },
    { text: "Same metric name across producers; ", options: { color: COLOR.ink } },
    { text: "producer=", options: { fontFace: "Consolas", color: COLOR.primary } },
    { text: " label distinguishes them.", options: { color: COLOR.ink } },
  ], {
    x: 0.55, y: 6.85, w: 12.4, h: 0.4,
    fontFace: FONT.body, fontSize: 12, italic: true, margin: 0,
  });

  footer(s, 7);
}

// =========================================================================
// Slide 8: What's still horizon, what isn't
// =========================================================================
{
  const s = pres.addSlide();
  s.background = { color: COLOR.bg };
  header(s, "Familiar / unfamiliar", "FOR YOUR HORIZON MENTAL MODEL");

  const tableData = [
    [
      { text: "Layer", options: { bold: true, color: COLOR.white, fill: { color: COLOR.primary } } },
      { text: "Status in delta-v", options: { bold: true, color: COLOR.white, fill: { color: COLOR.primary } } },
      { text: "Notes for you", options: { bold: true, color: COLOR.white, fill: { color: COLOR.primary } } },
    ],
    [
      { text: "Domain model (OnmsNode, OnmsAlarm, ...)", options: { bold: true } },
      "Same classes, jakarta.persistence",
      "Annotations updated; semantics unchanged",
    ],
    [
      { text: "DAOs", options: { bold: true } },
      "Same interfaces, JPA-based impls",
      "Hibernate 7 / Jakarta; HQL queries preserved",
    ],
    [
      { text: "Daemon classes (Pollerd, Alarmd, ...)", options: { bold: true } },
      "Used as horizon JARs; subclassed in Boot4",
      "AbstractServiceDaemon → SmartLifecycle adapter",
    ],
    [
      { text: "EventBuilder, EventForwarder", options: { bold: true } },
      "Still the API; Kafka is the transport",
      "No more EventBus, no more Eventd",
    ],
    [
      { text: "OSGi / Karaf / Blueprint", options: { bold: true, color: "B91C1C" } },
      { text: "Gone", options: { bold: true, color: "B91C1C" } },
      "Spring Boot 4 @Configuration replaces blueprint XML",
    ],
    [
      { text: "JMX MBeans for instrumentation", options: { bold: true, color: "B91C1C" } },
      { text: "Largely replaced", options: { bold: true, color: "B91C1C" } },
      "Spring Boot Actuator + Micrometer",
    ],
    [
      { text: "Newts / Cassandra", options: { bold: true, color: "B91C1C" } },
      { text: "Gone", options: { bold: true, color: "B91C1C" } },
      "VictoriaMetrics + Kafka pipeline",
    ],
    [
      { text: "ActiveMQ / ServiceMix / camel", options: { bold: true, color: "B91C1C" } },
      { text: "Going (rc1/rc2)", options: { bold: true, color: "B91C1C" } },
      "gRPC migration removes the last refs",
    ],
    [
      { text: "Spring 4.2.x (OpenNMS-patched)", options: { bold: true, color: "B91C1C" } },
      { text: "Gone", options: { bold: true, color: "B91C1C" } },
      "Spring 7 / Spring Boot 4.0.3, Java 21",
    ],
  ];

  s.addTable(tableData, {
    x: 0.55, y: 1.55, w: 12.4,
    colW: [3.6, 3.6, 5.2],
    fontFace: FONT.body, fontSize: 12, color: COLOR.ink,
    rowH: 0.5,
    border: { pt: 0.5, color: COLOR.rule },
    fill: { color: COLOR.white },
  });

  s.addText([
    { text: "Quick scan: ", options: { bold: true, color: COLOR.primary } },
    { text: "if a class lives in ", options: {} },
    { text: "org.opennms.netmgt.*", options: { fontFace: "Consolas", color: COLOR.primary } },
    { text: ", treat it as familiar. If it lives in ", options: {} },
    { text: "org.deltav.*", options: { fontFace: "Consolas", color: COLOR.accent } },
    { text: ", it's new code we own.", options: {} },
  ], {
    x: 0.55, y: 6.85, w: 12.4, h: 0.4,
    fontFace: FONT.body, fontSize: 12, italic: true, margin: 0,
  });

  footer(s, 8);
}

// =========================================================================
// Slide 9: Where it's headed
// =========================================================================
{
  const s = pres.addSlide();
  s.background = { color: COLOR.bg };
  header(s, "Where v1.2 closes, where v1.3 begins", "ROADMAP");

  // Timeline horizontal across slide
  const tlY = 2.0;
  const tlX0 = 0.85, tlW = 11.5;
  s.addShape(pres.shapes.RECTANGLE, {
    x: tlX0, y: tlY, w: tlW, h: 0.05,
    fill: { color: COLOR.muted }, line: { color: COLOR.muted },
  });

  const milestones = [
    { x: tlX0,                label: "alpha1-3", date: "Apr 23",   note: "self-contained images",          state: "done" },
    { x: tlX0 + tlW * 0.18,   label: "beta1",    date: "Apr 24",   note: "horizon-metric bridge",          state: "done" },
    { x: tlX0 + tlW * 0.34,   label: "beta2",    date: "Apr 25",   note: "domain metrics + Pollerd P3",    state: "done" },
    { x: tlX0 + tlW * 0.55,   label: "rc1",      date: "TBD",      note: "Minion gRPC midpoint",           state: "next" },
    { x: tlX0 + tlW * 0.75,   label: "rc2",      date: "TBD",      note: "gRPC complete · Kafka removed",  state: "future" },
    { x: tlX0 + tlW - 0.05,   label: "v1.2.0",   date: "GA",       note: "",                                state: "future" },
  ];

  milestones.forEach(m => {
    const isDone = m.state === "done";
    const isNext = m.state === "next";
    const dotColor = isDone ? COLOR.primary : (isNext ? COLOR.accent : COLOR.muted);
    s.addShape(pres.shapes.OVAL, {
      x: m.x - 0.12, y: tlY - 0.105, w: 0.26, h: 0.26,
      fill: { color: dotColor }, line: { color: dotColor },
    });
    if (isDone) {
      s.addShape(pres.shapes.OVAL, {
        x: m.x - 0.05, y: tlY - 0.035, w: 0.12, h: 0.12,
        fill: { color: COLOR.white }, line: { color: COLOR.white },
      });
    }
    s.addText(m.label, {
      x: m.x - 0.7, y: tlY + 0.25, w: 1.4, h: 0.3,
      fontFace: FONT.head, fontSize: 13, bold: true, color: COLOR.ink,
      align: "center", margin: 0,
    });
    s.addText(m.date, {
      x: m.x - 0.7, y: tlY + 0.55, w: 1.4, h: 0.3,
      fontFace: FONT.body, fontSize: 10, color: COLOR.muted,
      align: "center", margin: 0,
    });
    if (m.note) {
      s.addText(m.note, {
        x: m.x - 1.1, y: tlY - 0.95, w: 2.2, h: 0.6,
        fontFace: FONT.body, fontSize: 10, italic: true, color: COLOR.muted,
        align: "center", margin: 0,
      });
    }
  });

  // Two side-by-side "next" boxes
  const nextY = 3.6, nextH = 3.05;
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.55, y: nextY, w: 6.1, h: nextH,
    fill: { color: COLOR.white }, line: { color: COLOR.rule, width: 0.75 },
    shadow: { type: "outer", blur: 6, offset: 2, angle: 90, color: "000000", opacity: 0.07 },
  });
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.55, y: nextY, w: 6.1, h: 0.07,
    fill: { color: COLOR.accent }, line: { color: COLOR.accent },
  });
  s.addText("rc1 / rc2 — Minion gRPC migration", {
    x: 0.7, y: nextY + 0.2, w: 5.8, h: 0.5,
    fontFace: FONT.head, fontSize: 18, bold: true, color: COLOR.ink, margin: 0,
  });
  s.addText([
    { text: "Replace Kafka-based RPC + Sink with gRPC, ", options: {} },
    { text: "fronted by Spring Cloud Gateway", options: { italic: true, color: COLOR.primary } },
    { text: ".", options: { breakLine: true } },
    { text: "Heartbeat-first migration in rc1 (parallel Kafka + gRPC). ", options: {} },
    { text: "rc2", options: { bold: true } },
    { text: " removes Kafka IPC entirely, purges 32 ActiveMQ + ServiceMix bundles, retires the ", options: {} },
    { text: "Default", options: { fontFace: "Consolas" } },
    { text: " Minion location.", options: { breakLine: true } },
    { text: " ", options: { breakLine: true } },
    { text: "Gateway becomes the single ingress for any future HTTP UI too.", options: { italic: true, color: COLOR.muted } },
  ], {
    x: 0.7, y: nextY + 0.85, w: 5.8, h: 2.0,
    fontFace: FONT.body, fontSize: 12, color: COLOR.ink, paraSpaceAfter: 6, margin: 0,
  });

  s.addShape(pres.shapes.RECTANGLE, {
    x: 6.85, y: nextY, w: 6.1, h: nextH,
    fill: { color: COLOR.white }, line: { color: COLOR.rule, width: 0.75 },
    shadow: { type: "outer", blur: 6, offset: 2, angle: 90, color: "000000", opacity: 0.07 },
  });
  s.addShape(pres.shapes.RECTANGLE, {
    x: 6.85, y: nextY, w: 6.1, h: 0.07,
    fill: { color: COLOR.primary }, line: { color: COLOR.primary },
  });
  s.addText("v1.3 — K8s operator + HPA", {
    x: 7.0, y: nextY + 0.2, w: 5.8, h: 0.5,
    fontFace: FONT.head, fontSize: 18, bold: true, color: COLOR.ink, margin: 0,
  });
  s.addText([
    { text: "The control plane built in v1.2 finally gets a consumer.", options: { breakLine: true } },
    { text: " ", options: { breakLine: true } },
    { text: "An operator scrapes ", options: {} },
    { text: "deltav_<daemon>_*", options: { fontFace: "Consolas", color: COLOR.primary } },
    { text: " from each pod's actuator and feeds HPA v2.  ", options: {} },
    { text: "Autoscale on queue depth and RPC p99,", options: { italic: true, bold: true } },
    { text: " not CPU and memory (which lag the actual signal).", options: { breakLine: true } },
    { text: " ", options: { breakLine: true } },
    { text: "Replaces Nephron with Spring Cloud Stream consumers; YAML config migration.", options: { italic: true, color: COLOR.muted } },
  ], {
    x: 7.0, y: nextY + 0.85, w: 5.8, h: 2.0,
    fontFace: FONT.body, fontSize: 12, color: COLOR.ink, paraSpaceAfter: 6, margin: 0,
  });

  footer(s, 9);
}

// =========================================================================
// Slide 10: Where DJ could plug in
// =========================================================================
{
  const s = pres.addSlide();
  s.background = { color: COLOR.bgDark };

  // Cover-ish format for closer
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0, y: 0, w: 0.18, h: 7.5,
    fill: { color: COLOR.accent }, line: { color: COLOR.accent },
  });
  s.addText([
    { text: "WHERE YOU MIGHT PLUG IN", options: { color: "9CB3D9" } },
    { text: "      ·      ", options: { color: "6B82A6" } },
    { text: "Welcome back, DJ", options: { color: COLOR.accent, italic: true } },
  ], {
    x: 0.55, y: 0.4, w: 12, h: 0.4,
    fontFace: FONT.body, fontSize: 12, bold: true,
    charSpacing: 4, margin: 0,
  });
  s.addText("Concrete observability surfaces with room", {
    x: 0.55, y: 0.85, w: 12.5, h: 0.7,
    fontFace: FONT.head, fontSize: 30, bold: true, color: COLOR.white, margin: 0,
  });

  // 3 cards
  const cardY = 2.0, cardH = 4.7, cardW = 4.05, gap = 0.2;
  const cards = [
    {
      tag: "OBSERVABILITY",
      title: "Define HPA-grade metric semantics",
      body: "We have ~80 deltav_* counters across 13 daemons. Which 5 should HPA-Adapter expose to a K8s scaling policy? What's the right histogram bucket choice for RPC duration? What thresholds matter and which are noise?",
      hook: "Your call: bring the operator perspective to a metric set that's currently a developer's-eye view.",
    },
    {
      tag: "NEPHRON REPLACEMENT",
      title: "Spring Cloud Stream Thresholder",
      body: "The kafka-timeseries pipeline lands in VictoriaMetrics today; nothing yet consumes it for thresholding. Design as an SCS consumer with standard-deviation rules, replacing horizon's inline ThresholdingVisitor.",
      hook: "Greenfield design space; existing TimeseriesBatch protobuf is the input contract.",
    },
    {
      tag: "TRACING",
      title: "OpenTelemetry on the gRPC migration",
      body: "Minion → Core gRPC migration in v1.2-rc1/rc2 gives us per-call tracing for free. The deployment-layer question (Tempo? Jaeger? Grafana Cloud?) is open. So is the span model: which propagation context flows through TimeseriesBatch?",
      hook: "Lands on a v1.2-rc1 schedule, so the timing is right.",
    },
  ];
  cards.forEach((c, i) => {
    const x = 0.55 + i * (cardW + gap);
    s.addShape(pres.shapes.RECTANGLE, {
      x, y: cardY, w: cardW, h: cardH,
      fill: { color: "2C3674" }, line: { color: "3B4889", width: 0.5 },
    });
    s.addShape(pres.shapes.RECTANGLE, {
      x, y: cardY, w: cardW, h: 0.07,
      fill: { color: COLOR.accent }, line: { color: COLOR.accent },
    });
    s.addText(c.tag, {
      x: x + 0.25, y: cardY + 0.25, w: cardW - 0.5, h: 0.4,
      fontFace: FONT.body, fontSize: 11, bold: true, color: COLOR.accent,
      charSpacing: 4, margin: 0,
    });
    s.addText(c.title, {
      x: x + 0.25, y: cardY + 0.7, w: cardW - 0.5, h: 1.3,
      fontFace: FONT.head, fontSize: 18, bold: true, color: COLOR.white, margin: 0,
    });
    s.addText(c.body, {
      x: x + 0.25, y: cardY + 2.05, w: cardW - 0.5, h: 1.85,
      fontFace: FONT.body, fontSize: 12, color: "C5D9F1", margin: 0,
    });
    s.addShape(pres.shapes.RECTANGLE, {
      x: x + 0.25, y: cardY + 3.95, w: cardW - 0.5, h: 0.04,
      fill: { color: "3B4889" }, line: { color: "3B4889" },
    });
    s.addText(c.hook, {
      x: x + 0.25, y: cardY + 4.05, w: cardW - 0.5, h: 0.6,
      fontFace: FONT.body, fontSize: 10, italic: true, color: "9CB3D9", margin: 0,
    });
  });

  // Footer for consistency with content slides (lighter colors against dark bg)
  s.addText("delta-v · v1.2.0-beta2 · 2026-04-25", {
    x: 0.55, y: 7.15, w: 8, h: 0.3,
    fontFace: FONT.body, fontSize: 9, color: "6B82A6", margin: 0,
  });
  s.addText("10", {
    x: 12.4, y: 7.15, w: 0.5, h: 0.3,
    fontFace: FONT.body, fontSize: 9, color: "6B82A6", align: "right", margin: 0,
  });
}

// ===== write =====
pres.writeFile({ fileName: OUT })
  .then(name => console.log("Wrote:", name))
  .catch(err => { console.error(err); process.exit(1); });
