"""ACARS aircraft messaging routes."""

from __future__ import annotations

import io
import json
import os
import platform
import pty
import queue
import shutil
import subprocess
import threading
import time
from datetime import datetime
from typing import Generator

from flask import Blueprint, jsonify, request, Response

import app as app_module
from utils.logging import sensor_logger as logger
from utils.validation import validate_device_index, validate_gain, validate_ppm
from utils.sse import format_sse
from utils.constants import (
    PROCESS_TERMINATE_TIMEOUT,
    SSE_KEEPALIVE_INTERVAL,
    SSE_QUEUE_TIMEOUT,
    PROCESS_START_WAIT,
)

acars_bp = Blueprint('acars', __name__, url_prefix='/acars')

# Default VHF ACARS frequencies (MHz) - common worldwide
DEFAULT_ACARS_FREQUENCIES = [
    '131.550',  # Primary worldwide
    '130.025',  # Secondary USA/Canada
    '129.125',  # USA
    '131.525',  # Europe
    '131.725',  # Europe secondary
]

# Message counter for statistics
acars_message_count = 0
acars_last_message_time = None


def find_acarsdec():
    """Find acarsdec binary."""
    return shutil.which('acarsdec')


def get_acarsdec_json_flag(acarsdec_path: str) -> str:
    """Detect which JSON output flag acarsdec supports.

    Version 4.0+ uses -j for JSON stdout.
    Version 3.x uses -o 4 for JSON stdout.
    """
    try:
        # Get version by running acarsdec with no args (shows usage with version)
        result = subprocess.run(
            [acarsdec_path],
            capture_output=True,
            text=True,
            timeout=5
        )
        output = result.stdout + result.stderr

        # Parse version from output like "Acarsdec v4.3.1" or "Acarsdec/acarsserv 3.7"
        import re
        version_match = re.search(r'acarsdec[^\d]*v?(\d+)\.(\d+)', output, re.IGNORECASE)
        if version_match:
            major = int(version_match.group(1))
            # Version 4.0+ uses -j for JSON stdout
            if major >= 4:
                return '-j'
            # Version 3.x uses -o for output mode
            else:
                return '-o'
    except Exception as e:
        logger.debug(f"Could not detect acarsdec version: {e}")

    # Default to -j (modern standard for current builds from source)
    return '-j'


def stream_acars_output(process: subprocess.Popen, is_text_mode: bool = False) -> None:
    """Stream acarsdec JSON output to queue."""
    global acars_message_count, acars_last_message_time

    try:
        app_module.acars_queue.put({'type': 'status', 'status': 'started'})

        # Use appropriate sentinel based on mode (text mode for pty on macOS)
        sentinel = '' if is_text_mode else b''
        for line in iter(process.stdout.readline, sentinel):
            if is_text_mode:
                line = line.strip()
            else:
                line = line.decode('utf-8', errors='replace').strip()
            if not line:
                continue

            try:
                # acarsdec -o 4 outputs JSON, one message per line
                data = json.loads(line)

                # Add our metadata
                data['type'] = 'acars'
                data['timestamp'] = datetime.utcnow().isoformat() + 'Z'

                # Update stats
                acars_message_count += 1
                acars_last_message_time = time.time()

                app_module.acars_queue.put(data)

                # Log if enabled
                if app_module.logging_enabled:
                    try:
                        with open(app_module.log_file_path, 'a') as f:
                            ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            f.write(f"{ts} | ACARS | {json.dumps(data)}\n")
                    except Exception:
                        pass

            except json.JSONDecodeError:
                # Not JSON - could be status message
                if line:
                    logger.debug(f"acarsdec non-JSON: {line[:100]}")

    except Exception as e:
        logger.error(f"ACARS stream error: {e}")
        app_module.acars_queue.put({'type': 'error', 'message': str(e)})
    finally:
        app_module.acars_queue.put({'type': 'status', 'status': 'stopped'})
        with app_module.acars_lock:
            app_module.acars_process = None


@acars_bp.route('/tools')
def check_acars_tools() -> Response:
    """Check for ACARS decoding tools."""
    has_acarsdec = find_acarsdec() is not None

    return jsonify({
        'acarsdec': has_acarsdec,
        'ready': has_acarsdec
    })


@acars_bp.route('/status')
def acars_status() -> Response:
    """Get ACARS decoder status."""
    running = False
    if app_module.acars_process:
        running = app_module.acars_process.poll() is None

    return jsonify({
        'running': running,
        'message_count': acars_message_count,
        'last_message_time': acars_last_message_time,
        'queue_size': app_module.acars_queue.qsize()
    })


@acars_bp.route('/start', methods=['POST'])
def start_acars() -> Response:
    """Start ACARS decoder."""
    global acars_message_count, acars_last_message_time

    with app_module.acars_lock:
        if app_module.acars_process and app_module.acars_process.poll() is None:
            return jsonify({
                'status': 'error',
                'message': 'ACARS decoder already running'
            }), 409

    # Check for acarsdec
    acarsdec_path = find_acarsdec()
    if not acarsdec_path:
        return jsonify({
            'status': 'error',
            'message': 'acarsdec not found. Install with: sudo apt install acarsdec'
        }), 400

    data = request.json or {}

    # Validate inputs
    try:
        device = validate_device_index(data.get('device', '0'))
        gain = validate_gain(data.get('gain', '40'))
        ppm = validate_ppm(data.get('ppm', '0'))
    except ValueError as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400

    # Get frequencies - use provided or defaults
    frequencies = data.get('frequencies', DEFAULT_ACARS_FREQUENCIES)
    if isinstance(frequencies, str):
        frequencies = [f.strip() for f in frequencies.split(',')]

    # Clear queue
    while not app_module.acars_queue.empty():
        try:
            app_module.acars_queue.get_nowait()
        except queue.Empty:
            break

    # Reset stats
    acars_message_count = 0
    acars_last_message_time = None

    # Build acarsdec command
    # acarsdec -j -g <gain> -p <ppm> -r <device> <freq1> <freq2> ...
    # Note: -j is JSON stdout (newer forks), -o 4 was the old syntax
    # gain/ppm must come BEFORE -r
    json_flag = get_acarsdec_json_flag(acarsdec_path)
    cmd = [acarsdec_path]
    if json_flag == '-j':
        cmd.append('-j')         # JSON output (newer TLeconte fork)
    else:
        cmd.extend(['-o', '4'])  # JSON output (older versions)

    # Add gain if not auto (must be before -r)
    if gain and str(gain) != '0':
        cmd.extend(['-g', str(gain)])

    # Add PPM correction if specified (must be before -r)
    if ppm and str(ppm) != '0':
        cmd.extend(['-p', str(ppm)])

    # Add device and frequencies (-r takes device, remaining args are frequencies)
    cmd.extend(['-r', str(device)])
    cmd.extend(frequencies)

    logger.info(f"Starting ACARS decoder: {' '.join(cmd)}")

    try:
        is_text_mode = False

        # On macOS, use pty to avoid stdout buffering issues
        if platform.system() == 'Darwin':
            master_fd, slave_fd = pty.openpty()
            process = subprocess.Popen(
                cmd,
                stdout=slave_fd,
                stderr=subprocess.PIPE,
                start_new_session=True
            )
            os.close(slave_fd)
            # Wrap master_fd as a text file for line-buffered reading
            process.stdout = io.open(master_fd, 'r', buffering=1)
            is_text_mode = True
        else:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True
            )

        # Wait briefly to check if process started
        time.sleep(PROCESS_START_WAIT)

        if process.poll() is not None:
            # Process died
            stderr = ''
            if process.stderr:
                stderr = process.stderr.read().decode('utf-8', errors='replace')
            error_msg = f'acarsdec failed to start'
            if stderr:
                error_msg += f': {stderr[:200]}'
            logger.error(error_msg)
            return jsonify({'status': 'error', 'message': error_msg}), 500

        app_module.acars_process = process

        # Start output streaming thread
        thread = threading.Thread(
            target=stream_acars_output,
            args=(process, is_text_mode),
            daemon=True
        )
        thread.start()

        return jsonify({
            'status': 'started',
            'frequencies': frequencies,
            'device': device,
            'gain': gain
        })

    except Exception as e:
        logger.error(f"Failed to start ACARS decoder: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


@acars_bp.route('/stop', methods=['POST'])
def stop_acars() -> Response:
    """Stop ACARS decoder."""
    with app_module.acars_lock:
        if not app_module.acars_process:
            return jsonify({
                'status': 'error',
                'message': 'ACARS decoder not running'
            }), 400

        try:
            app_module.acars_process.terminate()
            app_module.acars_process.wait(timeout=PROCESS_TERMINATE_TIMEOUT)
        except subprocess.TimeoutExpired:
            app_module.acars_process.kill()
        except Exception as e:
            logger.error(f"Error stopping ACARS: {e}")

        app_module.acars_process = None

    return jsonify({'status': 'stopped'})


@acars_bp.route('/stream')
def stream_acars() -> Response:
    """SSE stream for ACARS messages."""
    def generate() -> Generator[str, None, None]:
        last_keepalive = time.time()

        while True:
            try:
                msg = app_module.acars_queue.get(timeout=SSE_QUEUE_TIMEOUT)
                last_keepalive = time.time()
                yield format_sse(msg)
            except queue.Empty:
                now = time.time()
                if now - last_keepalive >= SSE_KEEPALIVE_INTERVAL:
                    yield format_sse({'type': 'keepalive'})
                    last_keepalive = now

    response = Response(generate(), mimetype='text/event-stream')
    response.headers['Cache-Control'] = 'no-cache'
    response.headers['X-Accel-Buffering'] = 'no'
    return response


@acars_bp.route('/frequencies')
def get_frequencies() -> Response:
    """Get default ACARS frequencies."""
    return jsonify({
        'default': DEFAULT_ACARS_FREQUENCIES,
        'regions': {
            'north_america': ['129.125', '130.025', '130.450', '131.550'],
            'europe': ['131.525', '131.725', '131.550'],
            'asia_pacific': ['131.550', '131.450'],
        }
    })
