// Exports all Structurizr Lite diagrams as SVG + PNG.
// Usage: node export-diagrams.js <structurizrUrl> <format: svg|png|both> <outDir>
const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

const url = process.argv[2] || 'http://localhost:8080/workspace/diagrams';
const format = process.argv[3] || 'both';
const outDir = process.argv[4] || '../exports';
const svgDir = path.join(outDir, 'svg');
const pngDir = path.join(outDir, 'png');
fs.mkdirSync(svgDir, { recursive: true });
fs.mkdirSync(pngDir, { recursive: true });

const wantSvg = format === 'svg' || format === 'both';
const wantPng = format === 'png' || format === 'both';

(async () => {
  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
  });
  const page = await browser.newPage();
  await page.setViewport({ width: 2400, height: 1600, deviceScaleFactor: 2 });

  await page.goto(url, { waitUntil: 'networkidle0' });
  await page.waitForFunction('typeof structurizr !== "undefined" && structurizr.scripting && structurizr.scripting.isDiagramRendered() === true', { timeout: 60000 });

  const views = await page.evaluate(() => structurizr.scripting.getViews().map(v => v.key));
  console.log('Views: ' + views.join(', '));

  for (const key of views) {
    await page.evaluate((k) => structurizr.scripting.changeView(k), key);
    await page.waitForFunction('structurizr.scripting.isDiagramRendered() === true', { timeout: 60000 });

    if (wantSvg) {
      const svg = await page.evaluate(() => structurizr.scripting.exportCurrentDiagramToSVG({ includeMetadata: true }));
      fs.writeFileSync(path.join(svgDir, key + '.svg'), svg);
      console.log('SVG  ' + key);
    }
    if (wantPng) {
      // exportCurrentDiagramToPNG is asynchronous: it delivers the PNG data URI
      // via a callback (options, callback) rather than a return value.
      const png = await page.evaluate(() => new Promise((resolve) => {
        structurizr.scripting.exportCurrentDiagramToPNG({ includeMetadata: true, crop: false }, (data) => resolve(data));
      }));
      const base64 = png.replace(/^data:image\/png;base64,/, '');
      fs.writeFileSync(path.join(pngDir, key + '.png'), Buffer.from(base64, 'base64'));
      console.log('PNG  ' + key);
    }
  }

  await browser.close();
})().catch(e => { console.error(e); process.exit(1); });
