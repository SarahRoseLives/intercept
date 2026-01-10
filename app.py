"""
INTERCEPT - Signal Intelligence Platform

Flask application and shared state.
"""

from __future__ import annotations

import sys
import site

# Ensure user site-packages is available (may be disabled when running as root/sudo)
if not site.ENABLE_USER_SITE:
    user_site = site.getusersitepackages()
    if user_site and user_site not in sys.path:
        sys.path.insert(0, user_site)

import os
import queue
import threading
import platform
import subprocess

from typing import Any

from flask import Flask, render_template, jsonify, send_file, Response, request

from config import VERSION
from utils.dependencies import check_tool, check_all_dependencies, TOOL_DEPENDENCIES
from utils.process import cleanup_stale_processes
from utils.sdr import SDRFactory
from utils.cleanup import DataStore, cleanup_manager
from utils.constants import (
    MAX_AIRCRAFT_AGE_SECONDS,
    MAX_WIFI_NETWORK_AGE_SECONDS,
    MAX_BT_DEVICE_AGE_SECONDS,
    QUEUE_MAX_SIZE,
)

# Track application start time for uptime calculation
import time as _time
_app_start_time = _time.time()


# Create Flask app
app = Flask(__name__)

# Disable Werkzeug debugger PIN (not needed for local development tool)
os.environ['WERKZEUG_DEBUG_PIN'] = 'off'


# ============================================
# SECURITY HEADERS
# ============================================

@app.after_request
def add_security_headers(response):
    """Add security headers to all responses."""
    # Prevent MIME type sniffing
    response.headers['X-Content-Type-Options'] = 'nosniff'
    # Prevent clickjacking
    response.headers['X-Frame-Options'] = 'SAMEORIGIN'
    # Enable XSS filter
    response.headers['X-XSS-Protection'] = '1; mode=block'
    # Referrer policy
    response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
    # Permissions policy (disable unnecessary features)
    response.headers['Permissions-Policy'] = 'geolocation=(self), microphone=()'
    return response


# ============================================
# GLOBAL PROCESS MANAGEMENT
# ============================================

# Pager decoder
current_process = None
output_queue = queue.Queue(maxsize=QUEUE_MAX_SIZE)
process_lock = threading.Lock()

# RTL_433 sensor
sensor_process = None
sensor_queue = queue.Queue(maxsize=QUEUE_MAX_SIZE)
sensor_lock = threading.Lock()

# WiFi
wifi_process = None
wifi_queue = queue.Queue(maxsize=QUEUE_MAX_SIZE)
wifi_lock = threading.Lock()

# Bluetooth
bt_process = None
bt_queue = queue.Queue(maxsize=QUEUE_MAX_SIZE)
bt_lock = threading.Lock()

# ADS-B aircraft
adsb_process = None
adsb_queue = queue.Queue(maxsize=QUEUE_MAX_SIZE)
adsb_lock = threading.Lock()

# Satellite/Iridium
satellite_process = None
satellite_queue = queue.Queue(maxsize=QUEUE_MAX_SIZE)
satellite_lock = threading.Lock()

# RTLAMR smart meter
rtlamr_process = None
rtl_tcp_process = None
rtlamr_queue = queue.Queue(maxsize=QUEUE_MAX_SIZE)
rtlamr_lock = threading.Lock()

# ============================================
# GLOBAL STATE DICTIONARIES
# ============================================

# Logging settings
logging_enabled = False
log_file_path = 'pager_messages.log'

# WiFi state - using DataStore for automatic cleanup
wifi_monitor_interface = None
wifi_networks = DataStore(max_age_seconds=MAX_WIFI_NETWORK_AGE_SECONDS, name='wifi_networks')
wifi_clients = DataStore(max_age_seconds=MAX_WIFI_NETWORK_AGE_SECONDS, name='wifi_clients')
wifi_handshakes = []  # Captured handshakes (list, not auto-cleaned)

# Bluetooth state - using DataStore for automatic cleanup
bt_interface = None
bt_devices = DataStore(max_age_seconds=MAX_BT_DEVICE_AGE_SECONDS, name='bt_devices')
bt_beacons = DataStore(max_age_seconds=MAX_BT_DEVICE_AGE_SECONDS, name='bt_beacons')
bt_services = {}     # MAC -> list of services (not auto-cleaned, user-requested)

# Aircraft (ADS-B) state - using DataStore for automatic cleanup
adsb_aircraft = DataStore(max_age_seconds=MAX_AIRCRAFT_AGE_SECONDS, name='adsb_aircraft')

# Satellite state
satellite_passes = []  # Predicted satellite passes (not auto-cleaned, calculated)

# Register data stores with cleanup manager
cleanup_manager.register(wifi_networks)
cleanup_manager.register(wifi_clients)
cleanup_manager.register(bt_devices)
cleanup_manager.register(bt_beacons)
cleanup_manager.register(adsb_aircraft)


# ============================================
# MAIN ROUTES
# ============================================

@app.route('/')
def index() -> str:
    tools = {
        'rtl_fm': check_tool('rtl_fm'),
        'multimon': check_tool('multimon-ng'),
        'rtl_433': check_tool('rtl_433'),
        'rtlamr': check_tool('rtlamr')
    }
    devices = [d.to_dict() for d in SDRFactory.detect_devices()]
    return render_template('index.html', tools=tools, devices=devices, version=VERSION)


@app.route('/favicon.svg')
def favicon() -> Response:
    return send_file('favicon.svg', mimetype='image/svg+xml')


@app.route('/devices')
def get_devices() -> Response:
    """Get all detected SDR devices with hardware type info."""
    devices = SDRFactory.detect_devices()
    return jsonify([d.to_dict() for d in devices])


@app.route('/dependencies')
def get_dependencies() -> Response:
    """Get status of all tool dependencies."""
    results = check_all_dependencies()

    # Determine OS for install instructions
    system = platform.system().lower()
    if system == 'darwin':
        pkg_manager = 'brew'
    elif system == 'linux':
        pkg_manager = 'apt'
    else:
        pkg_manager = 'manual'

    return jsonify({
        'status': 'success',
        'os': system,
        'pkg_manager': pkg_manager,
        'modes': results
    })


@app.route('/export/aircraft', methods=['GET'])
def export_aircraft() -> Response:
    """Export aircraft data as JSON or CSV."""
    import csv
    import io

    format_type = request.args.get('format', 'json').lower()

    if format_type == 'csv':
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['icao', 'callsign', 'altitude', 'speed', 'heading', 'lat', 'lon', 'squawk', 'last_seen'])

        for icao, ac in adsb_aircraft.items():
            writer.writerow([
                icao,
                ac.get('callsign', '') if isinstance(ac, dict) else '',
                ac.get('altitude', '') if isinstance(ac, dict) else '',
                ac.get('speed', '') if isinstance(ac, dict) else '',
                ac.get('heading', '') if isinstance(ac, dict) else '',
                ac.get('lat', '') if isinstance(ac, dict) else '',
                ac.get('lon', '') if isinstance(ac, dict) else '',
                ac.get('squawk', '') if isinstance(ac, dict) else '',
                ac.get('lastSeen', '') if isinstance(ac, dict) else ''
            ])

        response = Response(output.getvalue(), mimetype='text/csv')
        response.headers['Content-Disposition'] = 'attachment; filename=aircraft.csv'
        return response
    else:
        return jsonify({
            'timestamp': __import__('datetime').datetime.utcnow().isoformat(),
            'aircraft': adsb_aircraft.values()
        })


@app.route('/export/wifi', methods=['GET'])
def export_wifi() -> Response:
    """Export WiFi networks as JSON or CSV."""
    import csv
    import io

    format_type = request.args.get('format', 'json').lower()

    if format_type == 'csv':
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['bssid', 'ssid', 'channel', 'signal', 'encryption', 'clients'])

        for bssid, net in wifi_networks.items():
            writer.writerow([
                bssid,
                net.get('ssid', '') if isinstance(net, dict) else '',
                net.get('channel', '') if isinstance(net, dict) else '',
                net.get('signal', '') if isinstance(net, dict) else '',
                net.get('encryption', '') if isinstance(net, dict) else '',
                net.get('clients', 0) if isinstance(net, dict) else 0
            ])

        response = Response(output.getvalue(), mimetype='text/csv')
        response.headers['Content-Disposition'] = 'attachment; filename=wifi_networks.csv'
        return response
    else:
        return jsonify({
            'timestamp': __import__('datetime').datetime.utcnow().isoformat(),
            'networks': wifi_networks.values(),
            'clients': wifi_clients.values()
        })


@app.route('/export/bluetooth', methods=['GET'])
def export_bluetooth() -> Response:
    """Export Bluetooth devices as JSON or CSV."""
    import csv
    import io

    format_type = request.args.get('format', 'json').lower()

    if format_type == 'csv':
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['mac', 'name', 'rssi', 'type', 'manufacturer', 'last_seen'])

        for mac, dev in bt_devices.items():
            writer.writerow([
                mac,
                dev.get('name', '') if isinstance(dev, dict) else '',
                dev.get('rssi', '') if isinstance(dev, dict) else '',
                dev.get('type', '') if isinstance(dev, dict) else '',
                dev.get('manufacturer', '') if isinstance(dev, dict) else '',
                dev.get('lastSeen', '') if isinstance(dev, dict) else ''
            ])

        response = Response(output.getvalue(), mimetype='text/csv')
        response.headers['Content-Disposition'] = 'attachment; filename=bluetooth_devices.csv'
        return response
    else:
        return jsonify({
            'timestamp': __import__('datetime').datetime.utcnow().isoformat(),
            'devices': bt_devices.values(),
            'beacons': bt_beacons.values()
        })


@app.route('/health')
def health_check() -> Response:
    """Health check endpoint for monitoring."""
    import time
    return jsonify({
        'status': 'healthy',
        'version': VERSION,
        'uptime_seconds': round(time.time() - _app_start_time, 2),
        'processes': {
            'pager': current_process is not None and (current_process.poll() is None if current_process else False),
            'sensor': sensor_process is not None and (sensor_process.poll() is None if sensor_process else False),
            'adsb': adsb_process is not None and (adsb_process.poll() is None if adsb_process else False),
            'wifi': wifi_process is not None and (wifi_process.poll() is None if wifi_process else False),
            'bluetooth': bt_process is not None and (bt_process.poll() is None if bt_process else False),
            'rtlamr': rtlamr_process is not None and (rtlamr_process.poll() is None if rtlamr_process else False),
        },
        'data': {
            'aircraft_count': len(adsb_aircraft),
            'wifi_networks_count': len(wifi_networks),
            'wifi_clients_count': len(wifi_clients),
            'bt_devices_count': len(bt_devices),
        }
    })


@app.route('/killall', methods=['POST'])
def kill_all() -> Response:
    """Kill all decoder and WiFi processes."""
    global current_process, sensor_process, wifi_process, adsb_process, rtlamr_process, rtl_tcp_process

    # Import adsb module to reset its state
    from routes import adsb as adsb_module

    killed = []
    processes_to_kill = [
        'rtl_fm', 'multimon-ng', 'rtl_433',
        'airodump-ng', 'aireplay-ng', 'airmon-ng',
        'dump1090', 'rtlamr', 'rtl_tcp'
    ]

    for proc in processes_to_kill:
        try:
            result = subprocess.run(['pkill', '-f', proc], capture_output=True)
            if result.returncode == 0:
                killed.append(proc)
        except (subprocess.SubprocessError, OSError):
            pass

    with process_lock:
        current_process = None

    with sensor_lock:
        sensor_process = None

    with wifi_lock:
        wifi_process = None

    # Reset ADS-B state
    with adsb_lock:
        adsb_process = None
        adsb_module.adsb_using_service = False

    # Reset RTLAMR state
    with rtlamr_lock:
        rtlamr_process = None
        rtl_tcp_process = None

    return jsonify({'status': 'killed', 'processes': killed})


def main() -> None:
    """Main entry point."""
    import argparse
    import config

    parser = argparse.ArgumentParser(
        description='INTERCEPT - Signal Intelligence Platform',
        epilog='Environment variables: INTERCEPT_HOST, INTERCEPT_PORT, INTERCEPT_DEBUG, INTERCEPT_LOG_LEVEL'
    )
    parser.add_argument(
        '-p', '--port',
        type=int,
        default=config.PORT,
        help=f'Port to run server on (default: {config.PORT})'
    )
    parser.add_argument(
        '-H', '--host',
        default=config.HOST,
        help=f'Host to bind to (default: {config.HOST})'
    )
    parser.add_argument(
        '-d', '--debug',
        action='store_true',
        default=config.DEBUG,
        help='Enable debug mode'
    )
    parser.add_argument(
        '--check-deps',
        action='store_true',
        help='Check dependencies and exit'
    )
    args = parser.parse_args()

    # Check dependencies only
    if args.check_deps:
        results = check_all_dependencies()
        print("Dependency Status:")
        print("-" * 40)
        for mode, info in results.items():
            status = "✓" if info['ready'] else "✗"
            print(f"\n{status} {info['name']}:")
            for tool, tool_info in info['tools'].items():
                tool_status = "✓" if tool_info['installed'] else "✗"
                req = " (required)" if tool_info['required'] else ""
                print(f"    {tool_status} {tool}{req}")
        sys.exit(0)

    print("=" * 50)
    print("  INTERCEPT // Signal Intelligence")
    print("  Pager / 433MHz / Aircraft / Satellite / WiFi / BT / Smart Meters")
    print("=" * 50)
    print()

    # Clean up any stale processes from previous runs
    cleanup_stale_processes()

    # Initialize database for settings storage
    from utils.database import init_db
    init_db()

    # Start automatic cleanup of stale data entries
    cleanup_manager.start()

    # Register blueprints
    from routes import register_blueprints
    register_blueprints(app)

    # Initialize WebSocket for audio streaming
    try:
        from routes.audio_websocket import init_audio_websocket
        init_audio_websocket(app)
        print("WebSocket audio streaming enabled")
    except ImportError as e:
        print(f"WebSocket audio disabled (install flask-sock): {e}")

    print(f"Open http://localhost:{args.port} in your browser")
    print()
    print("Press Ctrl+C to stop")
    print()

# Avoid loading a global ~/.env when running the script directly.
    app.run(
        host=args.host,
        port=args.port,
        debug=args.debug,
        threaded=True,
        load_dotenv=False,
    )