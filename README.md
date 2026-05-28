# Object Storage Download Benchmark

Benchmarks Railway Object Storage / Tigris and Cloudflare R2 download behavior by generating synthetic files, uploading each file, signing a GET URL, and asking Cloudflare Workers to download the object concurrently.

Cloudflare Workers act as isolated remote download clients. The Worker consumes the full response body through Cloudflare's Cache API as a short-lived native sink, then returns timing data. The local script measures Worker request wall time, aggregates stats, writes one self-contained HTML report, deletes uploaded objects in `finally` blocks, and removes local temporary fixtures at the end of the run.

## Setup

```bash
npm install
cp /Users/aayush/Documents/shahaayush1999/railway-tigris-vs-cf-r2/.env.local .env.local
```

`.env.local` is ignored by git. It needs credentials for both providers:

```bash
RAILWAY_S3_ENDPOINT=https://t3.storageapi.dev
RAILWAY_S3_REGION=auto
RAILWAY_S3_BUCKET=...
RAILWAY_S3_ACCESS_KEY_ID=...
RAILWAY_S3_SECRET_ACCESS_KEY=...

CLOUDFLARE_R2_ENDPOINT=https://<account-id>.r2.cloudflarestorage.com
CLOUDFLARE_R2_REGION=auto
CLOUDFLARE_R2_BUCKET=...
CLOUDFLARE_R2_ACCESS_KEY_ID=...
CLOUDFLARE_R2_SECRET_ACCESS_KEY=...

WORKER_URL=https://railway-object-storage-download-benchmark.<account>.workers.dev
```

For Cloudflare R2 tokens created through the dashboard/API token flow, the S3 access key ID is the token id and the S3 secret access key is the SHA-256 hash of the one-time token value.

Generated fixtures are also ignored by git. The benchmark creates them under `temp/<run-id>/` and deletes them when the run finishes.

Benchmark sizing, concurrency, and rounds are configured only with command-line flags.

## Deploy Worker

```bash
npx wrangler deploy
```

Set `WORKER_URL` in `.env.local` to the deployed Worker URL.

## Run

```bash
npm run bench
```

By default, the benchmark runs both Railway Object Storage and Cloudflare R2 for every configured file size, concurrency level, and round, then writes one side-by-side HTML comparison report. It generates temporary `1 MB`, `25 MB`, `50 MB`, and `100 MB` synthetic files. Each file is tested at concurrency `1,10,25,50,75,100`.

Override concurrency for a smoke test:

```bash
npm run bench -- --concurrency 1,10 --rounds 1
```

Override fixture sizes:

```bash
npm run bench -- --fixture-sizes-mb 1,5 --concurrency 1,10 --rounds 1
```

Multiple files run sequentially. For each file, multiple concurrency levels run sequentially. Within one level, all Worker requests are launched concurrently with the same signed URL.

## Output

Each run writes:

- HTML report: `output/<date>/<run-id>.html`

The HTML report is the only generated result artifact. It includes a high-level summary, one speed chart per file size, and one summary table per file size.

The HTML summary includes one section per file size. Each section has a per-file MB/s chart and a side-by-side provider comparison table:

| Concurrency | Railway Errors | Railway P50 | Railway P95 | Railway P99 | Railway Max | Railway MB/s | Cloudflare Errors | Cloudflare P50 | Cloudflare P95 | Cloudflare P99 | Cloudflare Max | Cloudflare MB/s |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|

`Per-file MB/s` is aggregate successful bytes read by Workers divided by the successful download count and the client-observed batch wall time. It is meant to show the effective download speed each file gets under that concurrency level.

Latency percentiles aggregate all successful measurements for that concurrency level. `Per-file MB/s` is averaged across repeated samples, so idle time between sequential samples is not counted.

## Worker Contract

The Worker accepts the signed URL as a `POST` `text/plain` body. It returns compact comma-separated metrics:

```text
ok,status,downloadMs
```

Example:

```text
1,200,1234.56
```

The Worker consumes the full object by writing the response stream to a unique Cache API key with a one-second lifetime. This avoids the Free-plan CPU failures caused by JS body drains like `arrayBuffer()` while still waiting for the full origin response before returning. The local runner already knows the fixture size, so the Worker does not count bytes.
