import os

from flask import Flask, jsonify, request

from sources.fanbasis.fanbasis_pipeline import (
    FANBASIS_PER_PAGE,
    TXN_TABLE,
    build_txn_rows,
    ensure_tables,
    fetch_transactions_page,
    refresh_models_from_file,
    upsert_transactions,
)
from sources.calendly.calendly_pipeline import ensure_tables as ensure_calendly_tables
from sources.calendly.calendly_pipeline import ingest_webhook_event as ingest_calendly_webhook_event
from sources.calendly.calendly_pipeline import run_incremental_sync as run_calendly_incremental_sync
from sources.calendly.calendly_pipeline import run_models as run_calendly_models
from sources.fathom.fathom_pipeline import ensure_tables as ensure_fathom_tables
from sources.fathom.fathom_pipeline import run_incremental_sync as run_fathom_incremental_sync
from sources.fathom.fathom_pipeline import run_models as run_fathom_models
from sources.ghl.ghl_pipeline import ensure_tables as ensure_ghl_tables
from sources.ghl.ghl_pipeline import run_incremental_sync
from sources.ghl.ghl_pipeline import run_models as run_ghl_models
from sources.ghl.ghl_pipeline import snapshot_pipeline_stages_daily
from sources.shared.phase1_release_gate import run_phase1_release_gate
from sources.typeform.typeform_pipeline import ensure_tables as ensure_typeform_tables
from sources.typeform.typeform_pipeline import run_incremental_sync as run_typeform_incremental_sync
from sources.typeform.typeform_pipeline import run_models as run_typeform_models
from sources.shared.analyst import ask_analyst
from sources.shared.data_quality import run_dq
from ops.runner.tasks import run_task
from sources.shared.warehouse_healthcheck import run_healthcheck
from sources.shared.warehouse_queries import AVAILABLE_QUERIES, run_named_query

app = Flask(__name__)

FANBASIS_PAGE = int(os.getenv("FANBASIS_PAGE", "1"))


@app.route("/", methods=["GET"])
def health():
    return jsonify({"ok": True, "service": "fanbasis-ingest"}), 200


@app.route("/routes", methods=["GET"])
def routes():
    # Lists every registered route so an operator can verify post-deploy that
    # the running image matches expected source. Catches stale-deploy drift
    # (image built from an older local clone) at curl-time rather than days
    # later when a scheduled invocation 404s silently.
    registered = sorted(
        f"{','.join(sorted(r.methods - {'HEAD', 'OPTIONS'}))} {r.rule}"
        for r in app.url_map.iter_rules()
        if r.endpoint != "static"
    )
    return jsonify({"ok": True, "count": len(registered), "routes": registered}), 200


@app.route("/ingest", methods=["GET", "POST"])
def ingest():
    try:
        ensure_tables()
        payload, resp = fetch_transactions_page(page=FANBASIS_PAGE, per_page=FANBASIS_PER_PAGE)
        rows = build_txn_rows(payload=payload, is_backfill=False, backfill_run_id=None)
        upsert_transactions(rows)
        return (
            jsonify(
                {
                    "ok": True,
                    "target_table": TXN_TABLE,
                    "page": FANBASIS_PAGE,
                    "per_page": FANBASIS_PER_PAGE,
                    "upstream_status": resp.status_code,
                    "records_upserted": len(rows),
                }
            ),
            200,
        )
    except Exception as exc:  # Keep endpoint resilient with clear diagnostics.
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/refresh-models", methods=["GET", "POST"])
def refresh_models():
    try:
        statements_executed = refresh_models_from_file()
        return (
            jsonify(
                {
                    "ok": True,
                    "statements_executed": statements_executed,
                }
            ),
            200,
        )
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/ingest-ghl", methods=["GET", "POST"])
def ingest_ghl():
    try:
        ensure_ghl_tables()
        result = run_incremental_sync()
        return jsonify({"ok": True, **result}), 200
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/refresh-ghl-models", methods=["GET", "POST"])
def refresh_ghl_models():
    try:
        statements_executed = run_ghl_models()
        return jsonify({"ok": True, "statements_executed": statements_executed}), 200
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/snapshot-pipeline-stages", methods=["GET", "POST"])
def snapshot_pipeline_stages():
    try:
        result = snapshot_pipeline_stages_daily()
        return jsonify({"ok": True, **result}), 200
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/ingest-fathom", methods=["GET", "POST"])
def ingest_fathom():
    try:
        ensure_fathom_tables()
        result = run_fathom_incremental_sync()
        return jsonify({"ok": True, **result}), 200
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/refresh-fathom-models", methods=["GET", "POST"])
def refresh_fathom_models():
    try:
        statements_executed = run_fathom_models()
        return jsonify({"ok": True, "statements_executed": statements_executed}), 200
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/ingest-calendly", methods=["GET", "POST"])
def ingest_calendly():
    try:
        ensure_calendly_tables()
        result = run_calendly_incremental_sync()
        return jsonify({"ok": True, **result}), 200
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/refresh-calendly-models", methods=["GET", "POST"])
def refresh_calendly_models():
    try:
        statements_executed = run_calendly_models()
        return jsonify({"ok": True, "statements_executed": statements_executed}), 200
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


# Standard Typeform ingestion + model refresh endpoints.
@app.route("/ingest-typeform", methods=["GET", "POST"])
def ingest_typeform():
    try:
        ensure_typeform_tables()
        result = run_typeform_incremental_sync()
        return jsonify({"ok": True, **result}), 200
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/refresh-typeform-models", methods=["GET", "POST"])
def refresh_typeform_models():
    try:
        statements_executed = run_typeform_models()
        return jsonify({"ok": True, "statements_executed": statements_executed}), 200
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/refresh-marts", methods=["GET", "POST"])
def refresh_marts():
    try:
        result = run_task("pipeline.marts_refresh_hourly")
        mart_statements_executed = result.get("model.marts") if isinstance(result, dict) else result
        return jsonify(
            {
                "ok": True,
                "mart_statements_executed": mart_statements_executed,
                "result": result,
            }
        ), 200
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/query", methods=["GET", "POST"])
def query():
    """Run a named analytical query on GCP and return a compact JSON summary.

    Pass the query name via:
      - POST body:   {"query": "closer_summary"}
      - GET param:   /query?name=closer_summary

    Available queries are listed in the catalog response at GET /query/catalog.
    """
    name = None
    if request.method == "POST":
        body = request.get_json(silent=True) or {}
        name = body.get("query") or body.get("name")
    if not name:
        name = request.args.get("name") or request.args.get("query")
    if not name:
        return jsonify({"ok": False, "error": "Provide 'query' in POST body or ?name= param",
                        "available": AVAILABLE_QUERIES}), 400
    try:
        result = run_named_query(name)
        return jsonify({"ok": True, **result}), 200
    except KeyError as exc:
        return jsonify({"ok": False, "error": str(exc), "available": AVAILABLE_QUERIES}), 404
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/query/catalog", methods=["GET"])
def query_catalog():
    return jsonify({"ok": True, "available_queries": AVAILABLE_QUERIES}), 200


@app.route("/run-data-quality", methods=["GET", "POST"])
def run_data_quality():
    try:
        result = run_dq()
        status_code = 200 if result.get("ok") else 422
        return jsonify(result), status_code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/refresh-marts-and-validate", methods=["GET", "POST"])
def refresh_marts_and_validate():
    try:
        refresh_result = run_task("pipeline.marts_refresh_hourly")
        mart_statements_executed = refresh_result.get("model.marts") if isinstance(refresh_result, dict) else refresh_result
        dq_result = run_dq()
        status_code = 200 if dq_result.get("ok") else 422
        return jsonify({
            "ok": dq_result.get("ok"),
            "mart_statements_executed": mart_statements_executed,
            "refresh_result": refresh_result,
            **dq_result,
        }), status_code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/healthcheck-warehouse", methods=["GET", "POST"])
def healthcheck_warehouse():
    try:
        result = run_healthcheck()
        status_code = 200 if result.get("ok") else 500
        return jsonify(result), status_code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/healthcheck-phase1-release-gate", methods=["GET", "POST"])
def healthcheck_phase1_release_gate():
    try:
        result = run_phase1_release_gate()
        status_code = 200 if result.get("ok") else 500
        return jsonify(result), status_code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/ask", methods=["POST", "OPTIONS"])
def ask():
    """
    AI data analyst endpoint. Accepts a plain-English question and returns
    a structured answer backed by Gemini 1.5 Flash + BigQuery.

    Request:
        POST /ask
        Headers:
            Content-Type: application/json
            Authorization: Bearer <ANALYST_API_KEY>   (if env var is set)
        Body:
            {"question": "How did Jordan do last week?"}

    Response:
        {"ok": true, "answer": "...", "sql": "...", "data": [...], "row_count": N}
    """
    # CORS preflight
    if request.method == "OPTIONS":
        resp = app.make_default_options_response()
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
        resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        return resp

    # Optional bearer token auth
    analyst_key = os.getenv("ANALYST_API_KEY")
    if analyst_key:
        auth_header = request.headers.get("Authorization", "")
        token = auth_header.removeprefix("Bearer ").strip()
        if token != analyst_key:
            return jsonify({"ok": False, "error": "Unauthorized"}), 401

    body = request.get_json(silent=True) or {}
    question = (body.get("question") or "").strip()
    if not question:
        return jsonify({"ok": False, "error": "Provide a 'question' in the request body"}), 400

    result = ask_analyst(question)
    status_code = 200 if result.get("ok") else 500

    resp = jsonify(result)
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp, status_code


@app.route("/webhooks/calendly", methods=["POST"])
def calendly_webhook():
    try:
        ensure_calendly_tables()
        body_bytes = request.get_data() or b""
        payload = request.get_json(silent=True) or {}
        headers = {k: v for k, v in request.headers.items()}
        result = ingest_calendly_webhook_event(
            payload=payload,
            headers=headers,
            body_bytes=body_bytes,
        )
        status_code = 202 if result.get("ignored") else 200
        return jsonify({"ok": True, **result}), status_code
    except PermissionError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 401
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
