#!/usr/bin/env node

const DEFAULT_BASE_URL = "https://dee-dashboard-mjxxki4snq-uc.a.run.app";
const SMOKE_TOKEN_HEADER = "X-Dashboard-Smoke-Token";

const baseUrl = stripTrailingSlash(process.env.DASHBOARD_URL || DEFAULT_BASE_URL);
const smokeToken = process.env.DASHBOARD_SMOKE_TOKEN || "";
const baseHeaders = smokeToken ? { [SMOKE_TOKEN_HEADER]: smokeToken } : {};

const args = parseArgs(process.argv.slice(2));
const injectFail = args["inject-fail"] || process.env.DASHBOARD_FAIL_QUERY || "";
const range = args.range || "7d";
const expectTier = args["expect-tier"] || (injectFail ? guessTier(injectFail) : "");

const T3_QUERIES = new Set([
  "speed_to_lead_unmatched_truth_audit",
  "speed_to_lead_reached_examples",
  "speed_to_lead_typeform_coverage",
  "speed_to_lead_typeform_outbound_opportunities",
  "speed_to_lead_unmatched_calendly_summary",
  "speed_to_lead_unmatched_calendly_invitees",
  "speed_to_lead_ghl_message_coverage",
  "speed_to_lead_ghl_outbound_message_breakdown",
]);

try {
  const result = await runSmokeCheck();
  console.log(JSON.stringify(result, null, 2));
  if (result.status !== "pass") process.exitCode = 1;
} catch (error) {
  console.error(JSON.stringify({ status: "fail", error: messageFrom(error) }, null, 2));
  process.exitCode = 1;
}

async function runSmokeCheck() {
  const apiUrl = `${baseUrl}/api/speed-to-lead?range=${encodeURIComponent(range)}`;
  const api = await getJson(apiUrl);

  if (api.status !== 200) {
    throw new Error(`Speed-to-Lead API returned ${api.status}; expected 200 in degraded smoke.`);
  }

  const overallRows = api.body?.rows?.speed_to_lead_overall ?? [];
  if (!Array.isArray(overallRows) || overallRows.length === 0) {
    throw new Error("speed_to_lead_overall returned no rows; tier-1 critical query is empty.");
  }

  const queryErrors = api.body?.queryErrors ?? {};

  if (!injectFail) {
    if (Object.keys(queryErrors).length > 0) {
      return {
        status: "pass",
        mode: "live-baseline",
        baseUrl,
        range,
        nonFatalErrors: queryErrors,
        note: "Live baseline returned 200 with non-critical queryErrors. Page is degraded but survivable.",
      };
    }
    return {
      status: "pass",
      mode: "live-baseline",
      baseUrl,
      range,
      note: "Live baseline returned 200 with no query errors.",
    };
  }

  // Injected-failure mode: expect the configured Cloud Run revision to run with
  // ALLOW_FAIL_INJECTION=1 DASHBOARD_FAIL_QUERY=<injectFail>.
  if (!queryErrors[injectFail]) {
    throw new Error(
      `Expected queryErrors["${injectFail}"] to be set in injected-failure mode but was missing. ` +
        `Verify the target revision has ALLOW_FAIL_INJECTION=1 and DASHBOARD_FAIL_QUERY=${injectFail}.`,
    );
  }

  const failedRows = api.body?.rows?.[injectFail];
  if (!Array.isArray(failedRows) || failedRows.length !== 0) {
    throw new Error(`Expected rows["${injectFail}"] to be []; got ${JSON.stringify(failedRows)}.`);
  }

  // Page HTML must surface the failure: T2 → chip, T3 → Data Health disclosure.
  const page = await getText(`${baseUrl}/speed-to-lead?range=${encodeURIComponent(range)}`);
  if (page.status !== 200) {
    throw new Error(`Page returned ${page.status}; expected 200 even with a degraded section.`);
  }

  const tier = expectTier || guessTier(injectFail);
  const chipFound = page.body.includes(`data-query-error="${injectFail}"`);
  const healthFound = page.body.includes("data-data-health");
  const nameInHealth = page.body.includes(injectFail);

  if (tier === "audit") {
    if (!healthFound || !nameInHealth) {
      throw new Error(`T3 query ${injectFail} did not appear in Data Health disclosure.`);
    }
  } else {
    if (!chipFound) {
      throw new Error(`T2 query ${injectFail} did not render a data-query-error chip.`);
    }
  }

  return {
    status: "pass",
    mode: "injected-failure",
    baseUrl,
    range,
    injectedQuery: injectFail,
    inferredTier: tier,
    queryErrorPresent: true,
    rowsEmpty: true,
    chipRendered: chipFound,
    dataHealthRendered: healthFound,
    note:
      tier === "audit"
        ? "T3 audit query failed; panel hidden + Data Health surfaced the name."
        : "T2 section query failed; inline chip rendered above the owning band.",
  };
}

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (!arg.startsWith("--")) continue;
    const eq = arg.indexOf("=");
    if (eq > -1) {
      out[arg.slice(2, eq)] = arg.slice(eq + 1);
    } else {
      const next = argv[i + 1];
      if (next && !next.startsWith("--")) {
        out[arg.slice(2)] = next;
        i += 1;
      } else {
        out[arg.slice(2)] = "true";
      }
    }
  }
  return out;
}

function guessTier(name) {
  if (T3_QUERIES.has(name)) return "audit";
  if (name === "speed_to_lead_overall") return "critical";
  return "section";
}

async function getJson(url) {
  const response = await fetch(url, { headers: baseHeaders, redirect: "manual" });
  if (response.status >= 300 && response.status < 400) {
    return {
      status: response.status,
      body: {
        error: `Unexpected redirect to ${response.headers.get("location") ?? "(unknown)"}.`,
      },
    };
  }
  const body = await response.json().catch(() => ({}));
  return { status: response.status, body };
}

async function getText(url) {
  const response = await fetch(url, { headers: baseHeaders, redirect: "manual" });
  const body = await response.text();
  return { status: response.status, body };
}

function stripTrailingSlash(value) {
  return value.replace(/\/+$/, "");
}

function messageFrom(error) {
  return error instanceof Error ? error.message : "Unknown degraded-smoke error.";
}
