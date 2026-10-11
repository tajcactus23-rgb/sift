"""
BM23 API Server — feeds BM23_COMMAND_CENTRE.html with live data.

Run:  python bm23_api_server.py
Port: 5521  (http://localhost:5521)

Endpoints
---------
GET  /api/ping          health check
GET  /api/events        recent TG-ingest events + summary counts
GET  /api/llm-status    local model port checks (1234/1235)
POST /api/command       {agent, action} — start/stop agents
"""

from __future__ import annotations
import json
import os
import socket
import sqlite3
import subprocess
import sys
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

BASE_DIR   = Path.home() / '.email_archive'
DB_PATH    = BASE_DIR / 'tg_ingest.db'
SIFT_PY    = Path(__file__).parent / 'SIFT-STABLE.py'
TG_PY      = Path(__file__).parent / 'bm23_tg_ingest.py'
PORT       = 5521

_procs: dict[str, subprocess.Popen] = {}


# ── DB helpers ─────────────────────────────────────────

def _db():
    conn = sqlite3.connect(str(DB_PATH), timeout=10, isolation_level=None)
    conn.execute('PRAGMA journal_mode=WAL')
    return conn


def _get_events(limit: int = 50) -> list:
    if not DB_PATH.exists():
        return []
    try:
        with _db() as c:
            rows = c.execute(
                'SELECT id,ts,chat_name,file_name,file_kind,matched_kws,'
                'combo_count,processed FROM events ORDER BY id DESC LIMIT ?',
                (limit,)).fetchall()
        return [{'id':r[0],'ts':r[1],'chat_name':r[2],'file_name':r[3],
                 'file_kind':r[4],'matched_kws':r[5],'combo_count':r[6],
                 'processed':r[7]} for r in rows]
    except Exception:
        return []


def _get_summary() -> dict:
    if not DB_PATH.exists():
        return {'total_combos':0,'total_hits':0,'queued':0}
    try:
        with _db() as c:
            combos = c.execute(
                'SELECT COALESCE(SUM(combo_count),0) FROM events').fetchone()[0]
            hits   = c.execute(
                'SELECT COUNT(*) FROM events').fetchone()[0]
            try:
                queued = c.execute(
                    "SELECT COUNT(*) FROM queue WHERE status='queued'").fetchone()[0]
            except Exception:
                queued = 0
        return {'total_combos': int(combos), 'total_hits': int(hits), 'queued': int(queued)}
    except Exception:
        return {'total_combos':0,'total_hits':0,'queued':0}


# ── Port probe ─────────────────────────────────────────

def _port_open(port: int, timeout: float = 0.4) -> bool:
    try:
        with socket.create_connection(('127.0.0.1', port), timeout=timeout):
            return True
    except Exception:
        return False


# ── Agent control ──────────────────────────────────────

def _proc_alive(key: str) -> bool:
    p = _procs.get(key)
    return bool(p and p.poll() is None)


def _start_agent(key: str, script: Path) -> str:
    if _proc_alive(key):
        return f'{key} already running.'
    if not script.exists():
        return f'{script.name} not found.'
    p = subprocess.Popen([sys.executable, str(script), 'run'],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    _procs[key] = p
    return f'{key} started (pid={p.pid}).'


def _stop_agent(key: str) -> str:
    p = _procs.get(key)
    if not p or p.poll() is not None:
        return f'{key} not running.'
    p.terminate()
    try: p.wait(timeout=4)
    except subprocess.TimeoutExpired: p.kill()
    return f'{key} stopped.'


# ── HTTP handler ───────────────────────────────────────

CORS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # quiet

    def _send(self, code: int, body: dict | str, ctype='application/json'):
        data = (json.dumps(body) if isinstance(body, dict) else body).encode()
        self.send_response(code)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(len(data)))
        for k, v in CORS.items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def do_OPTIONS(self):
        self.send_response(204)
        for k, v in CORS.items():
            self.send_header(k, v)
        self.end_headers()

    def do_GET(self):
        p = urlparse(self.path)
        qs = parse_qs(p.query)
        path = p.path

        if path == '/api/ping':
            self._send(200, {'ok': True, 'ts': int(time.time())})

        elif path == '/api/events':
            limit = int(qs.get('limit', ['50'])[0])
            evts  = _get_events(limit)
            summ  = _get_summary()
            payload = {**summ, 'events': evts}
            # agent runtime status
            payload['sift'] = {
                'running': _proc_alive('sift'),
                'status': 'RUNNING' if _proc_alive('sift') else 'IDLE',
                'metric': f'{summ["total_hits"]} hits',
            }
            payload['tg'] = {
                'running': _proc_alive('tg'),
                'status': 'POLLING' if _proc_alive('tg') else 'STOPPED',
                'metric': f'{summ["queued"]} queued',
            }
            self._send(200, payload)

        elif path == '/api/llm-status':
            status = {}
            for key, port in [('local7b', 1234), ('local13b', 1235)]:
                status[key] = {'online': _port_open(port), 'load': f':{port}'}
            # cloud models — just return unknown (no key check here)
            for key in ('openai','claude','gemini','mistral'):
                status[key] = {'online': None, 'load': '?'}
            self._send(200, status)

        else:
            self._send(404, {'error': 'not found'})

    def do_POST(self):
        if self.path == '/api/command':
            length = int(self.headers.get('Content-Length', 0))
            body   = json.loads(self.rfile.read(length) or b'{}')
            agent  = body.get('agent', '').lower()
            action = body.get('action', '').lower()
            if action == 'start':
                if agent == 'sift':
                    msg = _start_agent('sift', SIFT_PY)
                elif agent == 'tg':
                    msg = _start_agent('tg', TG_PY)
                else:
                    msg = f'Unknown agent: {agent}'
            elif action == 'stop':
                msg = _stop_agent(agent)
            else:
                msg = f'Unknown action: {action}'
            self._send(200, {'message': msg})
        else:
            self._send(404, {'error': 'not found'})


def main():
    print(f'BM23 API Server · http://localhost:{PORT}')
    print(f'DB: {DB_PATH}')
    print('Press Ctrl-C to stop.\n')
    try:
        srv = HTTPServer(('127.0.0.1', PORT), Handler)
        srv.serve_forever()
    except KeyboardInterrupt:
        print('\nstopped.')


if __name__ == '__main__':
    main()
