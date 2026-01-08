"""RTLAMR smart meter monitoring routes."""

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
from utils.process import safe_terminate

rtlamr_bp = Blueprint('rtlamr', __name__)


def stream_rtlamr_output(rtl_tcp_process: subprocess.Popen[bytes], 
                         rtlamr_process: subprocess.Popen[bytes]) -> None:
    """Stream rtlamr JSON output to queue."""
    try:
        app_module.rtlamr_queue.put({'type': 'status', 'text': 'started'})

        # Read stderr in a separate thread for error messages
        def read_stderr():
            if rtlamr_process.stderr:
                for line in iter(rtlamr_process.stderr.readline, b''):
                    error_line = line.decode('utf-8', errors='replace').strip()
                    if error_line:
                        logger.error(f"rtlamr stderr: {error_line}")
                        app_module.rtlamr_queue.put({'type': 'error', 'text': error_line})
        
        stderr_thread = threading.Thread(target=read_stderr, daemon=True)
        stderr_thread.start()

        for line in iter(rtlamr_process.stdout.readline, b''):
            line = line.decode('utf-8', errors='replace').strip()
            if not line:
                continue

            try:
                # rtlamr outputs JSON objects, one per line
                data = json.loads(line)
                data['type'] = 'smartmeter'
                app_module.rtlamr_queue.put(data)

                # Log if enabled
                if app_module.logging_enabled:
                    try:
                        with open(app_module.log_file_path, 'a') as f:
                            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            f.write(f"{timestamp} | SmartMeter | {json.dumps(data)}\n")
                    except Exception:
                        pass
            except json.JSONDecodeError:
                # Not JSON, send as raw
                app_module.rtlamr_queue.put({'type': 'raw', 'text': line})

    except Exception as e:
        logger.error(f"rtlamr stream error: {e}")
        app_module.rtlamr_queue.put({'type': 'error', 'text': str(e)})
    finally:
        # Clean up both processes
        safe_terminate(rtlamr_process)
        safe_terminate(rtl_tcp_process)
        app_module.rtlamr_queue.put({'type': 'status', 'text': 'stopped'})
        with app_module.rtlamr_lock:
            app_module.rtlamr_process = None
            app_module.rtl_tcp_process = None


@rtlamr_bp.route('/start_rtlamr', methods=['POST'])
def start_rtlamr() -> Response:
    logger.warning("=== start_rtlamr endpoint called ===")
    with app_module.rtlamr_lock:
        if app_module.rtlamr_process:
            logger.warning("Smart meter monitoring already running")
            return jsonify({'status': 'error', 'message': 'Smart meter monitoring already running'}), 409

        data = request.json or {}
        logger.warning(f"Request data: {data}")

        # Validate inputs
        try:
            freq = validate_frequency(data.get('frequency', '912.6'))  # Default frequency for smart meters
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

        # Message types to filter (default to all)
        msg_types = data.get('msgTypes', ['scm', 'scm+', 'idm', 'netidm', 'r900'])
        filter_id = data.get('filterID', '')  # Optional meter ID filter
        
        # Kill any existing rtl_tcp processes to free port 1234
        try:
            import psutil
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if proc.info['name'] == 'rtl_tcp' or (proc.info['cmdline'] and 'rtl_tcp' in ' '.join(proc.info['cmdline'] or [])):
                        logger.warning(f"Terminating existing rtl_tcp process {proc.info['pid']}")
                        proc.terminate()
                        try:
                            proc.wait(timeout=2)
                        except psutil.TimeoutExpired:
                            proc.kill()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
        except ImportError:
            logger.warning("psutil not available, cannot check for existing rtl_tcp processes")
        except Exception as e:
            logger.warning(f"Error checking for existing rtl_tcp: {e}")
        
        # Start rtl_tcp without arguments - it will use defaults and rtlamr will configure it
        rtl_tcp_cmd = ['rtl_tcp']
        
        # Only add device selection if not default
        if device != '0' and device != 0:
            rtl_tcp_cmd.extend(['-d', str(device)])

        logger.warning(f"Starting rtl_tcp: {' '.join(rtl_tcp_cmd)}")

        try:
            rtl_tcp_process = subprocess.Popen(
                rtl_tcp_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True
            )
            
            # Give rtl_tcp time to start and begin listening
            logger.warning("Waiting for rtl_tcp to initialize...")
            time.sleep(2)  # Increased wait time for rtl_tcp to fully start
            
            # Check if rtl_tcp is still running
            if rtl_tcp_process.poll() is not None:
                # Process died - capture error output
                stderr_output = rtl_tcp_process.stderr.read().decode('utf-8', errors='replace') if rtl_tcp_process.stderr else ''
                logger.error(f"rtl_tcp failed to start. stderr: {stderr_output}")
                return jsonify({
                    'status': 'error',
                    'message': f'rtl_tcp failed to start. {stderr_output[:200] if stderr_output else "Check if RTL-SDR device is connected."}'
                }), 500

        except (subprocess.SubprocessError, OSError) as e:
            logger.error(f"Failed to start rtl_tcp: {e}")
            return jsonify({'status': 'error', 'message': f'Failed to start rtl_tcp: {e}'}), 500

        # Now start rtlamr connected to rtl_tcp
        rtlamr_cmd = [
            'rtlamr',
            '-server=127.0.0.1:1234',
            '-format=json',
            f'-msgtype={",".join(msg_types)}'
        ]
        
        # Add rtltcp-specific configuration flags
        if freq and freq != '912.6':
            # Convert MHz to Hz for rtlamr
            rtlamr_cmd.append(f'-centerfreq={int(float(freq) * 1e6)}')
        
        if ppm and ppm != '0' and ppm != 0:
            rtlamr_cmd.append(f'-freqcorrection={ppm}')
        
        if gain and gain != '0' and gain != 0:
            rtlamr_cmd.append(f'-gainbyindex={int(float(gain))}')
        
        if filter_id:
            rtlamr_cmd.append(f'-filterid={filter_id}')

        logger.warning(f"Starting rtlamr: {' '.join(rtlamr_cmd)}")

        try:
            rtlamr_process = subprocess.Popen(
                rtlamr_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True
            )
            
            app_module.rtl_tcp_process = rtl_tcp_process
            app_module.rtlamr_process = rtlamr_process

            # Start output streaming in background thread
            thread = threading.Thread(
                target=stream_rtlamr_output,
                args=(rtl_tcp_process, rtlamr_process),
                daemon=True
            )
            thread.start()
            
            # Give rtlamr a moment to start and check if it's running
            time.sleep(0.5)
            if rtlamr_process.poll() is not None:
                # rtlamr died immediately
                stderr_output = rtlamr_process.stderr.read().decode('utf-8', errors='replace') if rtlamr_process.stderr else ''
                logger.error(f"rtlamr failed to start. stderr: {stderr_output}")
                safe_terminate(rtl_tcp_process)
                app_module.rtlamr_process = None
                app_module.rtl_tcp_process = None
                return jsonify({
                    'status': 'error',
                    'message': f'rtlamr failed: {stderr_output[:200] if stderr_output else "Unknown error"}'
                }), 500

            return jsonify({
                'status': 'started',
                'frequency': freq,
                'gain': gain,
                'ppm': ppm,
                'device': device,
                'msgTypes': msg_types
            })

        except (subprocess.SubprocessError, OSError) as e:
            logger.error(f"Failed to start rtlamr: {e}")
            safe_terminate(rtl_tcp_process)
            return jsonify({'status': 'error', 'message': f'Failed to start rtlamr: {e}'}), 500


@rtlamr_bp.route('/stop_rtlamr', methods=['POST'])
def stop_rtlamr() -> Response:
    with app_module.rtlamr_lock:
        if not app_module.rtlamr_process:
            return jsonify({'status': 'error', 'message': 'No smart meter monitoring running'}), 400

        logger.warning("Stopping rtlamr and rtl_tcp")
        
        # Stop rtlamr first
        if app_module.rtlamr_process:
            safe_terminate(app_module.rtlamr_process)
            app_module.rtlamr_process = None
        
        # Then stop rtl_tcp
        if app_module.rtl_tcp_process:
            safe_terminate(app_module.rtl_tcp_process)
            app_module.rtl_tcp_process = None

        return jsonify({'status': 'stopped'})


@rtlamr_bp.route('/rtlamr_stream')
def rtlamr_stream() -> Response:
    """SSE endpoint for rtlamr output."""
    def generate() -> Generator[str, None, None]:
        try:
            while True:
                try:
                    msg = app_module.rtlamr_queue.get(timeout=30)
                    yield format_sse(msg)
                except queue.Empty:
                    yield format_sse({'type': 'heartbeat'})
        except GeneratorExit:
            pass

    return Response(generate(), mimetype='text/event-stream')


@rtlamr_bp.route('/rtlamr_status')
def rtlamr_status() -> Response:
    """Get current rtlamr status."""
    with app_module.rtlamr_lock:
        running = app_module.rtlamr_process is not None and app_module.rtlamr_process.poll() is None
        rtl_tcp_running = app_module.rtl_tcp_process is not None and app_module.rtl_tcp_process.poll() is None
        
        return jsonify({
            'running': running,
            'rtl_tcp_running': rtl_tcp_running
        })
