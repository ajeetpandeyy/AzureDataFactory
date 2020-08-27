from datetime import datetime, timedelta
from flask import render_template, redirect, url_for, request, jsonify
from app import appvar
from .adf import query_all_pipes, query_single_pipe, ISO8601_FORMAT

@appvar.route('/', methods = ["GET"])
@appvar.route('/index', methods = ["GET"])
def index():
    start_iso = (datetime.now() - timedelta(days=7)).strftime(ISO8601_FORMAT)
    results = query_all_pipes(start=start_iso, include_activities=False)
    failed_count = sum([p["status"] == "Failed" for p in results])
    total_count = len(results)
    return render_template('index.html', 
        results = results, 
        failed_count = failed_count,
        total_count = total_count)


@appvar.route('/all', methods = ["GET"])
def all():
    start_iso = (datetime.now() - timedelta(days=7)).strftime(ISO8601_FORMAT)
    results = query_all_pipes(start=start_iso, include_activities=False, latest_only=False)
    failed_count = sum([p["status"] == "Failed" for p in results])
    total_count = len(results)
    return render_template('index.html', 
        results = results, 
        failed_count = failed_count,
        total_count = total_count)


@appvar.route('/failed', methods = ["GET"])
def failed():
    start_iso = (datetime.now() - timedelta(days=7)).strftime(ISO8601_FORMAT)
    results = query_all_pipes(start=start_iso, include_activities=False)
    failed_results = [p for p in results if p["status"] == "Failed"]
    failed_count = len(failed_results)
    total_count = len(results)
    return render_template('index.html', 
        results = failed_results, 
        failed_count = failed_count,
        total_count = total_count)


@appvar.route('/api/pipelines/<start>/sparse', methods = ["GET"])
def api_pipelines_sparse(start):
    results = query_all_pipes(start=start, include_activities=False)
    return jsonify(results)


@appvar.route('/api/pipelines/<start>/full', methods = ["GET"])
def api_pipelines_full(start):
    results = query_all_pipes(start=start, include_activities=True)
    return jsonify(results)


@appvar.route('/api/pipelines/<start>/full/all', methods = ["GET"])
def api_pipelines_full_all(start):
    results = query_all_pipes(start=start, include_activities=True,latest_only=False)
    return jsonify(results)


@appvar.route('/api/pipeline/<run_id>/', methods = ["GET"])
def api_pipelines_single(run_id):
    results = query_single_pipe(run_id=run_id)
    return jsonify(results)