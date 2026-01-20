"""
INTERCEPT - Signal Intelligence Platform

Flask application and shared state.
"""

from __future__ import annotations

import sys
import site

from utils.database import get_db

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

from flask import Flask, render_template, jsonify, send_file, Response, request,redirect, url_for, flash, session
from werkzeug.security import check_password_hash
from config import VERSION, CHANGELOG
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
import logging
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
# Track application start time for uptime calculation
import time as _time
_app_start_time = _time.time()
logger = logging.getLogger('intercept.database')

# Create Flask app
app = Flask(__name__)
app.secret_key = "signals_intelligence_secret" # Required for flash messages

# Set up rate limiting
limiter = Limiter(
    key_func=get_remote_address, # Identifies the user by their IP
    app=app,
    storage_uri="memory://", # Use RAM memory (change to redis:// etc. for distributed setups)
)

# Disable Werkzeug debugger PIN (not needed for local development tool)
os.environ['WERKZEUG_DEBUG_PIN'] = 'off'

# ============================================
# ERROR HANDLERS    
# ============================================
@app.errorhandler(429)
def ratelimit_handler(e):
    logger.warning(f"Rate limit exceeded for IP: {request.remote_addr}")
    flash("Too many login attempts. Please wait one minute before trying again.", "error")
    return render_template('login.html', version=VERSION), 429

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

# ACARS aircraft messaging
acars_process = None
acars_queue = queue.Queue(maxsize=QUEUE_MAX_SIZE)
acars_lock = threading.Lock()

# APRS amateur radio tracking
aprs_process = None
aprs_rtl_process = None
aprs_queue = queue.Queue(maxsize=QUEUE_MAX_SIZE)
aprs_lock = threading.Lock()

# RTLAMR utility meter reading
rtlamr_process = None
rtlamr_queue = queue.Queue(maxsize=QUEUE_MAX_SIZE)
rtlamr_lock = threading.Lock()

# TSCM (Technical Surveillance Countermeasures)
tscm_queue = queue.Queue(maxsize=QUEUE_MAX_SIZE)
tscm_lock = threading.Lock()

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

@app.before_request
def require_login():
    # Routes that don't require login (to avoid infinite redirect loop)
    allowed_routes = ['login', 'static', 'favicon', 'health']

    # If user is not logged in and the current route is not allowed...
    if 'logged_in' not in session and request.endpoint not in allowed_routes:
        return redirect(url_for('login'))
    
@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
@limiter.limit("5 per minute")  # Limit to 5 login attempts per minute per IP
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        # Connect to DB and find user
        with get_db() as conn:
            cursor = conn.execute(
                'SELECT password_hash, role FROM users WHERE username = ?',
                (username,)
            )
            user = cursor.fetchone()

        # Verify user exists and password is correct
        if user and check_password_hash(user['password_hash'], password):
            # Store data in session
            session['logged_in'] = True
            session['username'] = username
            session['role'] = user['role']
            
            logger.info(f"User '{username}' logged in successfully.")
            return redirect(url_for('index'))
        else:
            logger.warning(f"Failed login attempt for username: {username}")
            flash("ACCESS DENIED: INVALID CREDENTIALS", "error")
            
    return render_template('login.html', version=VERSION)

@app.route('/')
def index() -> str:
    tools = {
        'rtl_fm': check_tool('rtl_fm'),
        'multimon': check_tool('multimon-ng'),
        'rtl_433': check_tool('rtl_433'),
        'rtlamr': check_tool('rtlamr')
    }
    devices = [d.to_dict() for d in SDRFactory.detect_devices()]
    return render_template('index.html', tools=tools, devices=devices, version=VERSION, changelog=CHANGELOG)


@app.route('/favicon.svg')
def favicon() -> Response:
    return send_file('favicon.svg', mimetype='image/svg+xml')


@app.route('/devices')
def get_devices() -> Response:
    """Get all detected SDR devices with hardware type info."""
    devices = SDRFactory.detect_devices()
    return jsonify([d.to_dict() for d in devices])


@app.route('/devices/debug')
def get_devices_debug() -> Response:
    """Get detailed SDR device detection diagnostics."""
    import shutil

    diagnostics = {
        'tools': {},
        'rtl_test': {},
        'soapy': {},
        'usb': {},
        'kernel_modules': {},
        'detected_devices': [],
        'suggestions': []
    }

    # Check for required tools
    diagnostics['tools']['rtl_test'] = shutil.which('rtl_test') is not None
    diagnostics['tools']['SoapySDRUtil'] = shutil.which('SoapySDRUtil') is not None
    diagnostics['tools']['lsusb'] = shutil.which('lsusb') is not None

    # Run rtl_test and capture full output
    if diagnostics['tools']['rtl_test']:
        try:
            result = subprocess.run(
                ['rtl_test', '-t'],
                capture_output=True,
                text=True,
                timeout=5
            )
            diagnostics['rtl_test'] = {
                'returncode': result.returncode,
                'stdout': result.stdout[:2000] if result.stdout else '',
                'stderr': result.stderr[:2000] if result.stderr else ''
            }

            # Check for common errors
            combined = (result.stdout or '') + (result.stderr or '')
            if 'No supported devices found' in combined:
                diagnostics['suggestions'].append('No RTL-SDR device detected. Check USB connection.')
            if 'usb_claim_interface error' in combined:
                diagnostics['suggestions'].append('Device busy - kernel DVB driver may have claimed it. Run: sudo modprobe -r dvb_usb_rtl28xxu')
            if 'Permission denied' in combined.lower():
                diagnostics['suggestions'].append('USB permission denied. Add udev rules or run as root.')

        except subprocess.TimeoutExpired:
            diagnostics['rtl_test'] = {'error': 'Timeout after 5 seconds'}
        except Exception as e:
            diagnostics['rtl_test'] = {'error': str(e)}
    else:
        diagnostics['suggestions'].append('rtl_test not found. Install rtl-sdr package.')

    # Run SoapySDRUtil
    if diagnostics['tools']['SoapySDRUtil']:
        try:
            result = subprocess.run(
                ['SoapySDRUtil', '--find'],
                capture_output=True,
                text=True,
                timeout=10
            )
            diagnostics['soapy'] = {
                'returncode': result.returncode,
                'stdout': result.stdout[:2000] if result.stdout else '',
                'stderr': result.stderr[:2000] if result.stderr else ''
            }
        except subprocess.TimeoutExpired:
            diagnostics['soapy'] = {'error': 'Timeout after 10 seconds'}
        except Exception as e:
            diagnostics['soapy'] = {'error': str(e)}

    # Check USB devices (Linux)
    if diagnostics['tools']['lsusb']:
        try:
            result = subprocess.run(
                ['lsusb'],
                capture_output=True,
                text=True,
                timeout=5
            )
            # Filter for common SDR vendor IDs
            sdr_vendors = ['0bda', '1d50', '1df7', '0403']  # Realtek, OpenMoko/HackRF, SDRplay, FTDI
            usb_lines = [l for l in result.stdout.split('\n')
                        if any(v in l.lower() for v in sdr_vendors) or 'rtl' in l.lower() or 'sdr' in l.lower()]
            diagnostics['usb']['devices'] = usb_lines if usb_lines else ['No SDR-related USB devices found']
        except Exception as e:
            diagnostics['usb'] = {'error': str(e)}

    # Check for loaded kernel modules that conflict (Linux)
    if platform.system() == 'Linux':
        try:
            result = subprocess.run(
                ['lsmod'],
                capture_output=True,
                text=True,
                timeout=5
            )
            conflicting = ['dvb_usb_rtl28xxu', 'rtl2832', 'rtl2830']
            loaded = [m for m in conflicting if m in result.stdout]
            diagnostics['kernel_modules']['conflicting_loaded'] = loaded
            if loaded:
                diagnostics['suggestions'].append(f"Conflicting kernel modules loaded: {', '.join(loaded)}. Run: sudo modprobe -r {' '.join(loaded)}")
        except Exception as e:
            diagnostics['kernel_modules'] = {'error': str(e)}

    # Get detected devices
    devices = SDRFactory.detect_devices()
    diagnostics['detected_devices'] = [d.to_dict() for d in devices]

    if not devices and not diagnostics['suggestions']:
        diagnostics['suggestions'].append('No devices detected. Check USB connection and driver installation.')

    return jsonify(diagnostics)


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
            'acars': acars_process is not None and (acars_process.poll() is None if acars_process else False),
            'aprs': aprs_process is not None and (aprs_process.poll() is None if aprs_process else False),
            'wifi': wifi_process is not None and (wifi_process.poll() is None if wifi_process else False),
            'bluetooth': bt_process is not None and (bt_process.poll() is None if bt_process else False),
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
    global current_process, sensor_process, wifi_process, adsb_process, acars_process
    global aprs_process, aprs_rtl_process

    # Import adsb module to reset its state
    from routes import adsb as adsb_module

    killed = []
    processes_to_kill = [
        'rtl_fm', 'multimon-ng', 'rtl_433',
        'airodump-ng', 'aireplay-ng', 'airmon-ng',
        'dump1090', 'acarsdec', 'direwolf'
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

    # Reset ACARS state
    with acars_lock:
        acars_process = None

    # Reset APRS state
    with aprs_lock:
        aprs_process = None
        aprs_rtl_process = None

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
    print("  Pager / 433MHz / Aircraft / ACARS / Satellite / WiFi / BT")
    print("=" * 50)
    print()

    # Check if running as root (required for WiFi monitor mode, some BT operations)
    import os
    if os.geteuid() != 0:
        print("\033[93m" + "=" * 50)
        print("  ⚠️  WARNING: Not running as root/sudo")
        print("=" * 50)
        print("  Some features require root privileges:")
        print("    - WiFi monitor mode and scanning")
        print("    - Bluetooth low-level operations")
        print("    - RTL-SDR access (on some systems)")
        print()
        print("  To run with full capabilities:")
        print("    sudo -E venv/bin/python intercept.py")
        print("=" * 50 + "\033[0m")
        print()
        # Store for API access
        app.config['RUNNING_AS_ROOT'] = False
    else:
        app.config['RUNNING_AS_ROOT'] = True
        print("Running as root - full capabilities enabled")
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