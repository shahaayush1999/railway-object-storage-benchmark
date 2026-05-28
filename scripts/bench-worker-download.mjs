import { createReadStream, createWriteStream } from "node:fs";
import { randomBytes } from "node:crypto";
import { mkdir, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { DeleteObjectCommand, GetObjectCommand, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import dotenv from "dotenv";

const rootDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
dotenv.config({ path: path.join(rootDir, ".env.local") });

const args = parseArgs(process.argv.slice(2));
const targets = createTargets();
const startedAt = new Date();
const runId = `railway-worker-download-${startedAt.toISOString().replace(/[:.]/g, "-")}`;
const tempDir = path.join(rootDir, "temp", runId);
const fixtureSizesMb = parseList(args.fixtureSizesMb || "1,25,50,100");
if (fixtureSizesMb.length === 0) {
  throw new Error("At least one positive integer fixture size is required.");
}

const workerUrl = process.env.WORKER_URL || "";
if (!workerUrl) {
  throw new Error("Missing Worker URL. Set WORKER_URL in .env.local.");
}

const concurrencyLevels = parseList(args.concurrency || "1,10,25,50,75,100");
if (concurrencyLevels.length === 0) {
  throw new Error("At least one positive integer concurrency level is required.");
}

const rounds = parsePositiveInteger(args.rounds || "3", "rounds");
const signedUrlTtlSeconds = 3600;
const outputDate = startedAt.toISOString().slice(0, "YYYY-MM-DD".length);
const outputDir = path.join(rootDir, "output", outputDate);
const reportOutputPath = path.join(outputDir, `${runId}.html`);

await mkdir(outputDir, { recursive: true });

const fileCases = await createSyntheticFixtures({ fixtureSizesMb, fixtureDir: tempDir });

console.log("Railway Object Storage / Worker download benchmark");
console.log(`Files: ${fileCases.map((fileCase) => `${fileCase.fileName} (${formatBytes(fileCase.fileSizeBytes)})`).join(", ")}`);
console.log(`Providers: ${targets.map((target) => `${target.name} (${target.bucket})`).join(", ")}`);
console.log(`Worker URL: ${workerUrl}`);
console.log(`Concurrency levels: ${concurrencyLevels.join(", ")}`);
console.log(`Rounds: ${rounds}`);
console.log("");

const measurements = [];

try {
  for (const fileCase of fileCases) {
    console.log(`File ${fileCase.fileName} (${formatBytes(fileCase.fileSizeBytes)})`);
    for (const target of targets) {
      let uploaded = false;
      try {
        const objectKey = getObjectKey({ target, fileCase });
        console.log(`${target.name}`);
        console.log(`Object key: ${objectKey}`);
        await uploadObject({ target, fileCase, objectKey });
        uploaded = true;

        const signedUrl = await getSignedUrl(
          target.client,
          new GetObjectCommand({
            Bucket: target.bucket,
            Key: objectKey,
          }),
          { expiresIn: signedUrlTtlSeconds },
        );

        for (let round = 1; round <= rounds; round += 1) {
          console.log(`Round ${round}/${rounds}`);
          for (const concurrency of concurrencyLevels) {
            console.log(`Concurrency ${concurrency}`);
            const batch = await runBatch({ target, fileCase, round, concurrency, signedUrl });
            measurements.push(...batch);
            printBatchSummary({ fileCase, round, concurrency, batch });
          }
          console.log("");
        }
      } finally {
        if (uploaded) {
          await cleanupObject({ target, objectKey: getObjectKey({ target, fileCase }) });
        }
      }
    }
  }

  const report = buildReport();
  await writeFile(reportOutputPath, renderHtmlReport(report));

  console.log(`Wrote ${reportOutputPath}`);
  console.log("");
  console.table(
    report.summaries.map((summary) => ({
      provider: summary.providerName,
      concurrency: summary.concurrency,
      file: summary.fileName,
      size_mb: roundNumber(summary.fileSizeBytes / 1024 / 1024, 2),
      ok: summary.ok,
      error: summary.error,
      download_p50_ms: summary.workerDownloadMs.p50,
      download_p95_ms: summary.workerDownloadMs.p95,
      download_p99_ms: summary.workerDownloadMs.p99,
      max_ms: summary.workerDownloadMs.max,
      req_per_sec: summary.requestsPerSecond,
      per_file_mb_per_sec: summary.perFileMegabytesPerSecond,
    })),
  );
} finally {
  await rm(tempDir, { recursive: true, force: true });
  console.log(`Deleted local temporary fixtures from ${tempDir}`);
}

async function createSyntheticFixtures({ fixtureSizesMb, fixtureDir }) {
  await mkdir(fixtureDir, { recursive: true });
  const cases = [];
  for (const sizeMb of fixtureSizesMb) {
    const fileName = `synthetic-${sizeMb}mb.bin`;
    const filePath = path.join(fixtureDir, fileName);
    const fileSizeBytes = sizeMb * 1024 * 1024;
    await writeSyntheticFile({ filePath, fileSizeBytes });
    cases.push({
      filePath,
      fileSizeBytes,
      fileName,
      sizeMb,
    });
    console.log(`Created local synthetic fixture ${fileName} (${formatBytes(fileSizeBytes)})`);
  }
  return cases;
}

async function writeSyntheticFile({ filePath, fileSizeBytes }) {
  const chunkSize = 1024 * 1024;
  await new Promise((resolve, reject) => {
    const stream = createWriteStream(filePath);
    stream.on("error", reject);
    stream.on("finish", resolve);

    let remaining = fileSizeBytes;
    function writeNext() {
      while (remaining > 0) {
        const bytes = Math.min(chunkSize, remaining);
        remaining -= bytes;
        if (!stream.write(randomBytes(bytes))) {
          stream.once("drain", writeNext);
          return;
        }
      }
      stream.end();
    }

    writeNext();
  });
}

function getObjectKey({ target, fileCase }) {
  return `benchmark/worker-download/${target.slug}/${runId}/${fileCase.fileName}`;
}

async function uploadObject({ target, fileCase, objectKey }) {
  const started = performance.now();
  const maxAttempts = 3;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      await target.client.send(
        new PutObjectCommand({
          Bucket: target.bucket,
          Key: objectKey,
          Body: createReadStream(fileCase.filePath),
          ContentType: "application/octet-stream",
          CacheControl: "no-store",
        }),
      );
      break;
    } catch (error) {
      if (attempt === maxAttempts) throw error;
      console.warn(`Upload attempt ${attempt}/${maxAttempts} failed; retrying with a fresh stream`);
      await sleep(1000 * attempt);
    }
  }
  console.log(`Uploaded benchmark object in ${Math.round(performance.now() - started)}ms`);
}

async function cleanupObject({ target, objectKey }) {
  try {
    await target.client.send(
      new DeleteObjectCommand({
        Bucket: target.bucket,
        Key: objectKey,
      }),
    );
    console.log("Deleted benchmark object");
  } catch (error) {
    console.error("Failed to delete benchmark object", error instanceof Error ? error.message : String(error));
  }
}

async function runBatch({ target, fileCase, round, concurrency, signedUrl }) {
  const tasks = Array.from({ length: concurrency }, () => {
    return runSingleWorkerRequest({ target, fileCase, round, concurrency, signedUrl });
  });
  return Promise.all(tasks);
}

async function runSingleWorkerRequest({ target, fileCase, round, concurrency, signedUrl }) {
  const clientStarted = performance.now();
  let workerPayload = null;
  let error = null;
  let responseStatus = null;
  let usedProxyBodyTiming = false;

  try {
    const response = await fetch(workerUrl, {
      method: "POST",
      headers: {
        "content-type": "text/plain",
      },
      body: signedUrl,
    });
    responseStatus = response.status;

    if (response.status === 204) {
      workerPayload = { ok: true, status: 200, downloadMs: null, bytesRead: fileCase.fileSizeBytes };
    } else if (isWorkerPayloadResponse(response)) {
      const text = await response.text();
      workerPayload = parseWorkerPayload(text);
    } else {
      const bytesRead = await drainResponseBody(response);
      usedProxyBodyTiming = true;
      workerPayload = {
        ok: response.ok,
        status: response.status,
        downloadMs: null,
        bytesRead,
        ttfbMs: null,
        colo: null,
        error: response.ok ? null : `Origin request failed with status ${response.status}`,
      };
    }

    if (!response.ok || !workerPayload?.ok) {
      error = workerPayload?.error || `Worker request failed with status ${response.status}`;
    } else if (Number.isFinite(workerPayload.bytesRead) && workerPayload.bytesRead !== fileCase.fileSizeBytes) {
      error = `Worker read ${workerPayload.bytesRead} bytes; expected ${fileCase.fileSizeBytes} bytes`;
    }
  } catch (caught) {
    error = caught instanceof Error ? caught.message : String(caught);
  }

  const clientFinished = performance.now();
  const clientTotalMs = roundNumber(clientFinished - clientStarted, 2);
  const workerDownloadMs = usedProxyBodyTiming ? clientTotalMs : toFiniteNumber(workerPayload?.downloadMs);

  return {
    providerName: target.name,
    providerSlug: target.slug,
    fileName: fileCase.fileName,
    fileSizeBytes: fileCase.fileSizeBytes,
    round,
    concurrency,
    ok: !error && Boolean(workerPayload?.ok),
    workerStatus: workerPayload?.status ?? null,
    workerDownloadMs,
    workerTtfbMs: toFiniteNumber(workerPayload?.ttfbMs),
    workerBytesRead: Number.isFinite(workerPayload?.bytesRead) ? workerPayload.bytesRead : workerPayload?.ok ? fileCase.fileSizeBytes : null,
    workerColo: workerPayload?.colo ?? null,
    clientStarted,
    clientFinished,
    clientTotalMs,
    workerHttpStatus: responseStatus,
    error,
  };
}

function isWorkerPayloadResponse(response) {
  const contentType = response.headers.get("content-type") || "";
  return contentType.startsWith("application/json") || contentType.startsWith("text/plain");
}

async function drainResponseBody(response) {
  if (!response.body) return 0;

  const reader = response.body.getReader();
  let bytesRead = 0;

  while (true) {
    const { done, value } = await reader.read();
    if (done) return bytesRead;
    bytesRead += value?.byteLength ?? value?.length ?? 0;
  }
}

function printBatchSummary({ fileCase, round, concurrency, batch }) {
  const ok = batch.filter((item) => item.ok).length;
  const error = batch.length - ok;
  const okItems = batch.filter((item) => item.ok);
  const download = percentileStats(okItems.map((item) => item.workerDownloadMs).filter(Number.isFinite));
  const batchWallMs = getBatchWallMs(batch);
  const successBytes = okItems.reduce((sum, item) => sum + (Number.isFinite(item.workerBytesRead) ? item.workerBytesRead : fileCase.fileSizeBytes), 0);
  const reqPerSec = batchWallMs ? roundNumber((ok / batchWallMs) * 1000, 3) : null;
  const perFileMbPerSec = batchWallMs && ok > 0 ? roundNumber((successBytes / ok / 1024 / 1024 / batchWallMs) * 1000, 3) : null;
  const originStatuses = countBy(batch.map((item) => item.workerStatus ?? "unknown"));
  const workerHttpStatuses = countBy(batch.map((item) => item.workerHttpStatus ?? "unknown"));
  const errorsByMessage = countBy(batch.filter((item) => item.error).map((item) => item.error));

  console.log(
    `  r=${round} c=${concurrency}: ok=${ok}/${batch.length}, errors=${error}, download p50=${formatMs(download.p50)} p95=${formatMs(download.p95)} p99=${formatMs(download.p99)}, req/s=${formatNumber(reqPerSec)}, per-file MB/s=${formatNumber(perFileMbPerSec)}, origin=${JSON.stringify(originStatuses)}, worker=${JSON.stringify(workerHttpStatuses)}, errorMessages=${JSON.stringify(errorsByMessage)}`,
  );
}

function buildReport() {
  return {
    runId,
    targets: targets.map((target) => ({
      name: target.name,
      slug: target.slug,
      bucket: target.bucket,
      endpoint: target.endpoint,
    })),
    createdAt: new Date().toISOString(),
    summaries: summarizeMeasurements(measurements),
  };
}

function summarizeMeasurements(items) {
  const groups = new Map();
  for (const measurement of items) {
    const key = `${measurement.providerSlug}\t${measurement.fileName}\t${measurement.concurrency}`;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(measurement);
  }

  return [...groups.entries()].map(([key, group]) => {
    const [, fileName, concurrency] = key.split("\t");
    const okItems = group.filter((item) => item.ok);
    const roundSummaries = summarizeRounds(group);
    const first = group[0];

    return {
      fileName,
      providerName: first.providerName,
      providerSlug: first.providerSlug,
      fileSizeBytes: first.fileSizeBytes,
      concurrency: Number(concurrency),
      count: group.length,
      ok: okItems.length,
      error: group.length - okItems.length,
      successRate: roundNumber(okItems.length / group.length, 4),
      workerDownloadMs: percentileStats(okItems.map((item) => item.workerDownloadMs).filter(Number.isFinite)),
      clientTotalMs: percentileStats(group.map((item) => item.clientTotalMs).filter(Number.isFinite)),
      originStatuses: countBy(group.map((item) => item.workerStatus ?? "unknown")),
      workerHttpStatuses: countBy(group.map((item) => item.workerHttpStatus ?? "unknown")),
      batchWallMs: percentileStats(roundSummaries.map((item) => item.batchWallMs).filter(Number.isFinite)),
      requestsPerSecond: roundNumber(mean(roundSummaries.map((item) => item.requestsPerSecond).filter(Number.isFinite)), 3),
      perFileMegabytesPerSecond: roundNumber(mean(roundSummaries.map((item) => item.perFileMegabytesPerSecond).filter(Number.isFinite)), 3),
    };
  });
}

function summarizeRounds(items) {
  const groups = new Map();
  for (const measurement of items) {
    const key = `${measurement.providerSlug}\t${measurement.fileName}\t${measurement.round}\t${measurement.concurrency}`;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(measurement);
  }

  return [...groups.entries()].map(([key, group]) => {
    const [, fileName, roundText, concurrencyText] = key.split("\t");
    const round = Number(roundText);
    const concurrency = Number(concurrencyText);
    const okItems = group.filter((item) => item.ok);
    const batchWallMs = getBatchWallMs(group);
    const first = group[0];
    const successBytes = okItems.reduce((sum, item) => sum + (Number.isFinite(item.workerBytesRead) ? item.workerBytesRead : first.fileSizeBytes), 0);

    return {
      fileName,
      fileSizeBytes: first.fileSizeBytes,
      round,
      concurrency,
      count: group.length,
      ok: okItems.length,
      error: group.length - okItems.length,
      successRate: roundNumber(okItems.length / group.length, 4),
      workerDownloadMs: percentileStats(okItems.map((item) => item.workerDownloadMs).filter(Number.isFinite)),
      batchWallMs,
      requestsPerSecond: batchWallMs ? roundNumber((okItems.length / batchWallMs) * 1000, 3) : null,
      perFileMegabytesPerSecond:
        batchWallMs && okItems.length > 0 ? roundNumber((successBytes / okItems.length / 1024 / 1024 / batchWallMs) * 1000, 3) : null,
    };
  });
}

function getBatchWallMs(items) {
  const starts = items.map((item) => item.clientStarted).filter(Number.isFinite);
  const finishes = items.map((item) => item.clientFinished).filter(Number.isFinite);
  if (starts.length === 0 || finishes.length === 0) return null;
  return Math.max(...finishes) - Math.min(...starts);
}

function renderHtmlReport(report) {
  const summariesByFile = groupBy(report.summaries, (summary) => summary.fileName);

  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Railway benchmark - ${escapeHtml(report.runId)}</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f7f8fa;
      --panel: #ffffff;
      --text: #17202a;
      --muted: #5f6b7a;
      --line: #d9dee7;
      --railway: #7c3aed;
      --bad: #b42318;
      --good: #067647;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    header, main { max-width: 1240px; margin: 0 auto; padding: 24px; }
    header { padding-top: 32px; padding-bottom: 8px; }
    main { padding-top: 0; }
    h1 { margin: 0 0 8px; font-size: 28px; line-height: 1.15; }
    h2 { margin: 12px 0 12px; font-size: 20px; }
    h3 { margin: 24px 0 10px; font-size: 16px; }
    p { color: var(--muted); margin: 0 0 12px; }
    .table-wrap {
      overflow-x: auto;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
    }
    table { width: 100%; border-collapse: collapse; min-width: 920px; }
    th, td { padding: 9px 10px; border: 1px solid var(--line); text-align: right; white-space: nowrap; }
    th:first-child, td:first-child { text-align: center; }
    th { font-size: 12px; color: var(--muted); background: #fbfcfe; position: sticky; top: 0; }
    thead tr:first-child th { text-align: center; }
    .ok { color: var(--good); font-weight: 650; }
    .err { color: var(--bad); font-weight: 650; }
    code { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
    .meta { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; }
    .pill { border: 1px solid var(--line); border-radius: 999px; background: var(--panel); padding: 6px 10px; color: var(--muted); }
    .legend { display: flex; gap: 14px; align-items: center; margin: -4px 0 10px; color: var(--muted); font-size: 12px; }
    .legend-item { display: inline-flex; gap: 6px; align-items: center; }
    .swatch { display: inline-block; width: 18px; height: 3px; border-radius: 999px; background: var(--railway); }
    .chart {
      width: 100%;
      height: auto;
      display: block;
      margin: 0 0 12px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
    }
    .axis, .grid { stroke: var(--line); stroke-width: 1; }
    .series { fill: none; stroke: var(--railway); stroke-width: 2.5; }
    .point { fill: var(--railway); stroke: #ffffff; stroke-width: 1.5; }
    .tick { fill: var(--muted); font-size: 11px; }
    .chart-label { fill: var(--text); font-size: 12px; font-weight: 650; }
  </style>
</head>
<body>
  <header>
    <h1>Railway Object Storage Benchmark</h1>
    <p>Generated ${escapeHtml(formatCreatedAt(report.createdAt))}. Each chart compares per-file download speed as concurrency increases.</p>
  </header>
  <main>
    <section>
      <h2>Summary By File Size</h2>
      ${[...summariesByFile.entries()].map(([fileName, summaries]) => `
        <h3>${escapeHtml(formatFileLabel(summaries[0].fileSizeBytes))}</h3>
        ${renderSpeedChart({ summaries, targets: report.targets })}
        ${renderSummaryTable({ summaries, targets: report.targets })}
      `).join("")}
    </section>
  </main>
</body>
</html>
`;
}

function renderSpeedChart({ summaries, targets }) {
  const width = 920;
  const height = 300;
  const margin = { top: 28, right: 28, bottom: 52, left: 64 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const points = summaries
    .map((summary) => ({
      providerName: summary.providerName,
      providerSlug: summary.providerSlug,
      concurrency: summary.concurrency,
      value: summary.perFileMegabytesPerSecond,
    }))
    .filter((point) => Number.isFinite(point.value))
    .sort((a, b) => a.concurrency - b.concurrency);

  if (points.length === 0) {
    return `<p>No successful downloads to plot.</p>`;
  }

  const minX = Math.min(...points.map((point) => point.concurrency));
  const maxX = Math.max(...points.map((point) => point.concurrency));
  const maxValue = Math.max(...points.map((point) => point.value));
  const maxY = maxValue > 0 ? maxValue * 1.08 : 1;
  const xPadding = Math.min(72, plotWidth * 0.12);
  const innerPlotWidth = Math.max(1, plotWidth - xPadding * 2);
  const xScale =
    minX === maxX
      ? () => margin.left + plotWidth / 2
      : (value) => margin.left + xPadding + ((value - minX) / (maxX - minX)) * innerPlotWidth;
  const yScale = (value) => margin.top + plotHeight - (value / Math.max(1, maxY)) * plotHeight;
  const yTicks = Array.from({ length: 5 }, (_, index) => (maxY / 4) * index);
  const xTicks = [...new Set(summaries.map((summary) => summary.concurrency))].sort((a, b) => a - b);

  return `<svg class="chart" viewBox="0 0 ${width} ${height}" role="img" aria-label="Per-file MB/s by concurrency">
    <text x="${margin.left}" y="18" class="chart-label">Per-file MB/s by concurrency</text>
    ${yTicks.map((tick) => {
      const y = yScale(tick);
      return `<line class="grid" x1="${margin.left}" x2="${width - margin.right}" y1="${y}" y2="${y}"></line>
        <text class="tick" x="${margin.left - 10}" y="${y + 4}" text-anchor="end">${formatNumber(roundNumber(tick, 2))}</text>`;
    }).join("")}
    <line class="axis" x1="${margin.left}" x2="${width - margin.right}" y1="${margin.top + plotHeight}" y2="${margin.top + plotHeight}"></line>
    <line class="axis" x1="${margin.left}" x2="${margin.left}" y1="${margin.top}" y2="${margin.top + plotHeight}"></line>
    ${xTicks.map((tick) => {
      const x = xScale(tick);
      return `<line class="grid" x1="${x}" x2="${x}" y1="${margin.top}" y2="${margin.top + plotHeight}"></line>
        <text class="tick" x="${x}" y="${height - 24}" text-anchor="middle">${tick}</text>`;
    }).join("")}
    ${targets.map((target) => {
      const targetPoints = points.filter((point) => point.providerSlug === target.slug).sort((a, b) => a.concurrency - b.concurrency);
      if (targetPoints.length === 0) return "";
      const path = targetPoints.map((point) => `${xScale(point.concurrency)},${yScale(point.value)}`).join(" ");
      return `<polyline class="series ${escapeHtml(target.slug)}" points="${path}"></polyline>
        ${targetPoints.map((point) => `<circle class="point ${escapeHtml(target.slug)}" cx="${xScale(point.concurrency)}" cy="${yScale(point.value)}" r="4">
          <title>Concurrency ${point.concurrency}: ${formatNumber(point.value)} MB/s per file</title>
        </circle>`).join("")}`;
    }).join("")}
    <text class="tick" x="${margin.left + plotWidth / 2}" y="${height - 6}" text-anchor="middle">Concurrency</text>
    <text class="tick" x="16" y="${margin.top + plotHeight / 2}" text-anchor="middle" transform="rotate(-90 16 ${margin.top + plotHeight / 2})">Per-file MB/s</text>
  </svg>
  `;
}

function renderSummaryTable({ summaries, targets }) {
  const summariesByConcurrency = groupBy(summaries, (summary) => summary.concurrency);

  return `<div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Concurrency</th>
          ${targets.map(() => `
            <th>Errors</th>
            <th>P50</th>
            <th>P95</th>
            <th>P99</th>
            <th>Max</th>
            <th>MB/s</th>
          `).join("")}
        </tr>
      </thead>
      <tbody>
        ${[...summariesByConcurrency.entries()].sort(([a], [b]) => Number(a) - Number(b)).map(([concurrency, concurrencySummaries]) => `
          <tr>
            <td>${escapeHtml(concurrency)}</td>
            ${targets.map((target) => renderProviderCells(concurrencySummaries.find((summary) => summary.providerSlug === target.slug))).join("")}
          </tr>
        `).join("")}
      </tbody>
    </table>
  </div>`;
}

function renderProviderCells(summary) {
  if (!summary) {
    return `<td></td><td></td><td></td><td></td><td></td><td></td>`;
  }

  return `
    <td class="${summary.error > 0 ? "err" : ""}">${escapeHtml(summary.error)}</td>
    <td>${escapeHtml(formatMs(summary.workerDownloadMs.p50))}</td>
    <td>${escapeHtml(formatMs(summary.workerDownloadMs.p95))}</td>
    <td>${escapeHtml(formatMs(summary.workerDownloadMs.p99))}</td>
    <td>${escapeHtml(formatMs(summary.workerDownloadMs.max))}</td>
    <td>${escapeHtml(formatNumber(summary.perFileMegabytesPerSecond))}</td>
  `;
}

function groupBy(items, keyFn) {
  const groups = new Map();
  for (const item of items) {
    const key = keyFn(item);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(item);
  }
  return groups;
}

function niceCeil(value) {
  if (!Number.isFinite(value) || value <= 0) return 1;
  const magnitude = 10 ** Math.floor(Math.log10(value));
  const normalized = value / magnitude;
  if (normalized <= 1) return magnitude;
  if (normalized <= 2) return 2 * magnitude;
  if (normalized <= 5) return 5 * magnitude;
  return 10 * magnitude;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function percentileStats(values) {
  if (values.length === 0) {
    return { min: null, p50: null, p75: null, p90: null, p95: null, p99: null, max: null, mean: null, stddev: null };
  }
  const sorted = [...values].sort((a, b) => a - b);
  const avg = mean(sorted);
  return {
    min: roundNumber(sorted[0], 2),
    p50: roundNumber(percentile(sorted, 0.5), 2),
    p75: roundNumber(percentile(sorted, 0.75), 2),
    p90: roundNumber(percentile(sorted, 0.9), 2),
    p95: roundNumber(percentile(sorted, 0.95), 2),
    p99: roundNumber(percentile(sorted, 0.99), 2),
    max: roundNumber(sorted[sorted.length - 1], 2),
    mean: roundNumber(avg, 2),
    stddev: roundNumber(Math.sqrt(mean(sorted.map((value) => (value - avg) ** 2))), 2),
  };
}

function percentile(sorted, p) {
  const index = Math.ceil(sorted.length * p) - 1;
  return sorted[Math.max(0, Math.min(sorted.length - 1, index))];
}

function countBy(values) {
  return values.reduce((counts, value) => {
    counts[value] = (counts[value] || 0) + 1;
    return counts;
  }, {});
}

function mean(values) {
  if (values.length === 0) return null;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function roundNumber(value, digits = 2) {
  if (value === null || !Number.isFinite(value)) return null;
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function toFiniteNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function parseWorkerPayload(text) {
  const trimmed = text.trim();
  if (trimmed.startsWith("{")) {
    const payload = JSON.parse(trimmed);
    const ok = Boolean(payload.ok);
    const status = Number(payload.status);
    const downloadMs = Number(payload.downloadMs);
    const ttfbMs = Number(payload.ttfbMs);
    const bytesRead = Number(payload.bytesRead);

    return {
      ok,
      status: Number.isFinite(status) ? status : null,
      downloadMs: Number.isFinite(downloadMs) ? roundNumber(downloadMs, 2) : null,
      ttfbMs: Number.isFinite(ttfbMs) ? roundNumber(ttfbMs, 2) : null,
      bytesRead: Number.isFinite(bytesRead) ? bytesRead : null,
      colo: payload.colo ?? null,
      error: ok ? null : payload.error || `Worker download failed with origin status ${Number.isFinite(status) ? status : "unknown"}`,
    };
  }

  const parts = text.split(",");
  if (parts.length < 3) {
    throw new Error(`Worker returned unexpected response: ${text.slice(0, 240)}`);
  }

  const ok = parts[0] === "1";
  const status = Number(parts[1]);
  const downloadMs = Number(parts[2]);
  return {
    ok,
    status: Number.isFinite(status) ? status : null,
    downloadMs: Number.isFinite(downloadMs) ? roundNumber(downloadMs, 2) : null,
    ttfbMs: null,
    bytesRead: null,
    colo: null,
    error: ok ? null : `Worker download failed with origin status ${Number.isFinite(status) ? status : "unknown"}`,
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function createTargets() {
  return [
    createTarget({
      name: "Railway Object Storage",
      slug: "railway",
      envPrefix: "RAILWAY_S3",
    }),
  ];
}

function createTarget(config) {
  const endpoint = mustGet(`${config.envPrefix}_ENDPOINT`);
  const region = mustGet(`${config.envPrefix}_REGION`);
  const bucket = mustGet(`${config.envPrefix}_BUCKET`);
  const sessionToken = process.env[`${config.envPrefix}_SESSION_TOKEN`];
  const credentials = {
    accessKeyId: mustGet(`${config.envPrefix}_ACCESS_KEY_ID`),
    secretAccessKey: mustGet(`${config.envPrefix}_SECRET_ACCESS_KEY`),
    ...(sessionToken ? { sessionToken } : {}),
  };

  return {
    ...config,
    endpoint,
    region,
    bucket,
    client: new S3Client({
      endpoint,
      region,
      forcePathStyle: true,
      credentials,
    }),
  };
}

function parseList(value) {
  return value
    .split(",")
    .map((item) => Number(item.trim()))
    .filter((item) => Number.isInteger(item) && item > 0);
}

function parseArgs(argv) {
  const parsed = {
    concurrency: "",
    rounds: "",
    fixtureSizesMb: "",
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];

    if (arg === "--concurrency" || arg === "-c") {
      parsed.concurrency = argv[index + 1] || "";
      index += 1;
      continue;
    }

    if (arg.startsWith("--concurrency=")) {
      parsed.concurrency = arg.slice("--concurrency=".length);
      continue;
    }

    if (arg === "--rounds" || arg === "-r") {
      parsed.rounds = argv[index + 1] || "";
      index += 1;
      continue;
    }

    if (arg.startsWith("--rounds=")) {
      parsed.rounds = arg.slice("--rounds=".length);
      continue;
    }

    if (arg === "--fixture-sizes-mb" || arg === "--fixture-sizes") {
      parsed.fixtureSizesMb = argv[index + 1] || "";
      index += 1;
      continue;
    }

    if (arg.startsWith("--fixture-sizes-mb=")) {
      parsed.fixtureSizesMb = arg.slice("--fixture-sizes-mb=".length);
      continue;
    }

    if (arg.startsWith("--fixture-sizes=")) {
      parsed.fixtureSizesMb = arg.slice("--fixture-sizes=".length);
      continue;
    }

  }

  return parsed;
}

function mustGet(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function parsePositiveInteger(value, label) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${label} must be a positive integer.`);
  }
  return parsed;
}

function formatMs(ms) {
  if (ms === null || ms === undefined) return "";
  return `${ms}ms`;
}

function formatNumber(value) {
  if (value === null || value === undefined) return "";
  return String(value);
}

function formatPercent(value) {
  if (value === null || value === undefined) return "";
  return `${roundNumber(value * 100, 2)}%`;
}

function formatFileLabel(bytes) {
  return `${roundNumber(bytes / 1024 / 1024, 2)} MB file`;
}

function formatCreatedAt(value) {
  return new Date(value).toLocaleString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function formatBytes(bytes) {
  return `${(bytes / 1024 / 1024).toFixed(2)} MB`;
}
