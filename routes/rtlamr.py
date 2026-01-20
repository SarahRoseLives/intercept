"""RTLAMR utility meter monitoring routes."""

from __future__ import annotations

import json
import queue
import subprocess
import threading
import time
from datetime import datetime
from typing import Generator

from flask import Blueprint, jsonify, request, Response

import app as app_module
from utils.logging import sensor_logger as logger
from utils.validation import (
    validate_frequency, validate_device_index, validate_gain, validate_ppm
)
from utils.sse import format_sse
from utils.process import safe_terminate, register_process

rtlamr_bp = Blueprint('rtlamr', __name__)

# Store rtl_tcp process separately
rtl_tcp_process = None
rtl_tcp_lock = threading.Lock()


def stream_rtlamr_output(process: subprocess.Popen[bytes]) -> None:
    """Stream rtlamr JSON output to queue."""
    try:
        app_module.rtlamr_queue.put({'type': 'status', 'text': 'started'})

        for line in iter(process.stdout.readline, b''):
            line = line.decode('utf-8', errors='replace').strip()
            if not line:
                continue

            try:
                # rtlamr outputs JSON objects, one per line
                data = json.loads(line)
                data['type'] = 'rtlamr'
                app_module.rtlamr_queue.put(data)

                # Log if enabled
                if app_module.logging_enabled:
                    try:
                        with open(app_module.log_file_path, 'a') as f:
                            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            f.write(f"{timestamp} | RTLAMR | {json.dumps(data)}\n")
                    except Exception:
                        pass
            except json.JSONDecodeError:
                # Not JSON, send as raw
                app_module.rtlamr_queue.put({'type': 'raw', 'text': line})

    except Exception as e:
        app_module.rtlamr_queue.put({'type': 'error', 'text': str(e)})
    finally:
        process.wait()
        app_module.rtlamr_queue.put({'type': 'status', 'text': 'stopped'})
        with app_module.rtlamr_lock:
            app_module.rtlamr_process = None


@rtlamr_bp.route('/start_rtlamr', methods=['POST'])
def start_rtlamr() -> Response:
    global rtl_tcp_process

    with app_module.rtlamr_lock:
        if app_module.rtlamr_process:
            return jsonify({'status': 'error', 'message': 'RTLAMR already running'}), 409

        data = request.json or {}

        # Validate inputs
        try:
            freq = validate_frequency(data.get('frequency', '912.0'))
            gain = validate_gain(data.get('gain', '0'))
            ppm = validate_ppm(data.get('ppm', '0'))
            device = validate_device_index(data.get('device', '0'))
        except ValueError as e:
            return jsonify({'status': 'error', 'message': str(e)}), 400

        # Clear queue
        while not app_module.rtlamr_queue.empty():
            try:
                app_module.rtlamr_queue.get_nowait()
            except queue.Empty:
                break

        # Get message type (default to scm)
        msgtype = data.get('msgtype', 'scm')
        output_format = data.get('format', 'json')
        
        # Start rtl_tcp first
        with rtl_tcp_lock:
            if not rtl_tcp_process:
                logger.info("Starting rtl_tcp server...")
                try:
                    rtl_tcp_cmd = ['rtl_tcp', '-a', '0.0.0.0']
                    
                    # Add device index if not 0
                    if device and device != '0':
                        rtl_tcp_cmd.extend(['-d', str(device)])
                    
                    # Add gain if not auto
                    if gain and gain != '0':
                        rtl_tcp_cmd.extend(['-g', str(gain)])
                    
                    # Add PPM correction if not 0
                    if ppm and ppm != '0':
                        rtl_tcp_cmd.extend(['-p', str(ppm)])
                    
                    rtl_tcp_process = subprocess.Popen(
                        rtl_tcp_cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE
                    )
                    
                    # Wait a moment for rtl_tcp to start
                    time.sleep(1)
                    
                    logger.info(f"rtl_tcp started: {' '.join(rtl_tcp_cmd)}")
                    app_module.rtlamr_queue.put({'type': 'info', 'text': f'rtl_tcp: {" ".join(rtl_tcp_cmd)}'})
                except Exception as e:
                    logger.error(f"Failed to start rtl_tcp: {e}")
                    return jsonify({'status': 'error', 'message': f'Failed to start rtl_tcp: {e}'}), 500

        # Build rtlamr command
        cmd = [
            'rtlamr',
            '-server=127.0.0.1:1234',
            f'-msgtype={msgtype}',
            f'-format={output_format}',
            f'-centerfreq={int(float(freq) * 1e6)}'
        ]
        
        # Add filter options if provided
        filterid = data.get('filterid')
        if filterid:
            cmd.append(f'-filterid={filterid}')
        
        filtertype = data.get('filtertype')
        if filtertype:
            cmd.append(f'-filtertype={filtertype}')
        
        # Unique messages only
        if data.get('unique', True):
            cmd.append('-unique=true')

        full_cmd = ' '.join(cmd)
        logger.info(f"Running: {full_cmd}")

        try:
            app_module.rtlamr_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

            # Start output thread
            thread = threading.Thread(target=stream_rtlamr_output, args=(app_module.rtlamr_process,))
            thread.daemon = True
            thread.start()

            # Monitor stderr
            def monitor_stderr():
                for line in app_module.rtlamr_process.stderr:
                    err = line.decode('utf-8', errors='replace').strip()
                    if err:
                        logger.debug(f"[rtlamr] {err}")
                        app_module.rtlamr_queue.put({'type': 'info', 'text': f'[rtlamr] {err}'})

            stderr_thread = threading.Thread(target=monitor_stderr)
            stderr_thread.daemon = True
            stderr_thread.start()

            app_module.rtlamr_queue.put({'type': 'info', 'text': f'Command: {full_cmd}'})

            return jsonify({'status': 'started', 'command': full_cmd})

        except FileNotFoundError:
            # If rtlamr fails, clean up rtl_tcp
            with rtl_tcp_lock:
                if rtl_tcp_process:
                    rtl_tcp_process.terminate()
                    rtl_tcp_process.wait(timeout=2)
                    rtl_tcp_process = None
            return jsonify({'status': 'error', 'message': 'rtlamr not found. Install from https://github.com/bemasher/rtlamr'})
        except Exception as e:
            # If rtlamr fails, clean up rtl_tcp
            with rtl_tcp_lock:
                if rtl_tcp_process:
                    rtl_tcp_process.terminate()
                    rtl_tcp_process.wait(timeout=2)
                    rtl_tcp_process = None
            return jsonify({'status': 'error', 'message': str(e)})


@rtlamr_bp.route('/stop_rtlamr', methods=['POST'])
def stop_rtlamr() -> Response:
    global rtl_tcp_process
    
    with app_module.rtlamr_lock:
        if app_module.rtlamr_process:
            app_module.rtlamr_process.terminate()
            try:
                app_module.rtlamr_process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                app_module.rtlamr_process.kill()
            app_module.rtlamr_process = None
    
    # Also stop rtl_tcp
    with rtl_tcp_lock:
        if rtl_tcp_process:
            rtl_tcp_process.terminate()
            try:
                rtl_tcp_process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                rtl_tcp_process.kill()
            rtl_tcp_process = None
            logger.info("rtl_tcp stopped")
    
    return jsonify({'status': 'stopped'})


@rtlamr_bp.route('/stream_rtlamr')
def stream_rtlamr() -> Response:
    def generate() -> Generator[str, None, None]:
        last_keepalive = time.time()
        keepalive_interval = 30.0

        while True:
            try:
                msg = app_module.rtlamr_queue.get(timeout=1)
                last_keepalive = time.time()
                yield format_sse(msg)
            except queue.Empty:
                now = time.time()
                if now - last_keepalive >= keepalive_interval:
                    yield format_sse({'type': 'keepalive'})
                    last_keepalive = now

    response = Response(generate(), mimetype='text/event-stream')
    response.headers['Cache-Control'] = 'no-cache'
    response.headers['X-Accel-Buffering'] = 'no'
    response.headers['Connection'] = 'keep-alive'
    return response
