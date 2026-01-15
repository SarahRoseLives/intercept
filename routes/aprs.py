"""APRS amateur radio position reporting routes."""

from __future__ import annotations

import json
import queue
import re
import shutil
import subprocess
import threading
import time
from datetime import datetime
from typing import Generator, Optional

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

aprs_bp = Blueprint('aprs', __name__, url_prefix='/aprs')

# APRS frequencies by region (MHz)
APRS_FREQUENCIES = {
    'north_america': '144.390',
    'europe': '144.800',
    'australia': '145.175',
    'new_zealand': '144.575',
    'argentina': '144.930',
    'brazil': '145.570',
    'japan': '144.640',
    'china': '144.640',
}

# Statistics
aprs_packet_count = 0
aprs_station_count = 0
aprs_last_packet_time = None
aprs_stations = {}  # callsign -> station data


def find_direwolf() -> Optional[str]:
    """Find direwolf binary."""
    return shutil.which('direwolf')


def find_multimon_ng() -> Optional[str]:
    """Find multimon-ng binary."""
    return shutil.which('multimon-ng')


def find_rtl_fm() -> Optional[str]:
    """Find rtl_fm binary."""
    return shutil.which('rtl_fm')


def parse_aprs_packet(raw_packet: str) -> Optional[dict]:
    """Parse APRS packet into structured data."""
    try:
        # Basic APRS packet format: CALLSIGN>PATH:DATA
        # Example: N0CALL-9>APRS,TCPIP*:@092345z4903.50N/07201.75W_090/000g005t077

        match = re.match(r'^([A-Z0-9-]+)>([^:]+):(.+)$', raw_packet, re.IGNORECASE)
        if not match:
            return None

        callsign = match.group(1).upper()
        path = match.group(2)
        data = match.group(3)

        packet = {
            'type': 'aprs',
            'callsign': callsign,
            'path': path,
            'raw': raw_packet,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
        }

        # Determine packet type and parse accordingly
        if data.startswith('!') or data.startswith('='):
            # Position without timestamp
            packet['packet_type'] = 'position'
            pos = parse_position(data[1:])
            if pos:
                packet.update(pos)

        elif data.startswith('/') or data.startswith('@'):
            # Position with timestamp
            packet['packet_type'] = 'position'
            # Skip timestamp (7 chars) and parse position
            if len(data) > 8:
                pos = parse_position(data[8:])
                if pos:
                    packet.update(pos)

        elif data.startswith('>'):
            # Status message
            packet['packet_type'] = 'status'
            packet['status'] = data[1:]

        elif data.startswith(':'):
            # Message
            packet['packet_type'] = 'message'
            msg_match = re.match(r'^:([A-Z0-9 -]{9}):(.*)$', data, re.IGNORECASE)
            if msg_match:
                packet['addressee'] = msg_match.group(1).strip()
                packet['message'] = msg_match.group(2)

        elif data.startswith('_'):
            # Weather report (Positionless)
            packet['packet_type'] = 'weather'
            packet['weather'] = parse_weather(data)

        elif data.startswith(';'):
            # Object
            packet['packet_type'] = 'object'

        elif data.startswith(')'):
            # Item
            packet['packet_type'] = 'item'

        elif data.startswith('T'):
            # Telemetry
            packet['packet_type'] = 'telemetry'

        else:
            packet['packet_type'] = 'other'
            packet['data'] = data

        return packet

    except Exception as e:
        logger.debug(f"Failed to parse APRS packet: {e}")
        return None


def parse_position(data: str) -> Optional[dict]:
    """Parse APRS position data."""
    try:
        # Format: DDMM.mmN/DDDMM.mmW (or similar with symbols)
        # Example: 4903.50N/07201.75W

        pos_match = re.match(
            r'^(\d{2})(\d{2}\.\d+)([NS])(.)(\d{3})(\d{2}\.\d+)([EW])(.)?',
            data
        )

        if pos_match:
            lat_deg = int(pos_match.group(1))
            lat_min = float(pos_match.group(2))
            lat_dir = pos_match.group(3)
            symbol_table = pos_match.group(4)
            lon_deg = int(pos_match.group(5))
            lon_min = float(pos_match.group(6))
            lon_dir = pos_match.group(7)
            symbol_code = pos_match.group(8) or ''

            lat = lat_deg + lat_min / 60.0
            if lat_dir == 'S':
                lat = -lat

            lon = lon_deg + lon_min / 60.0
            if lon_dir == 'W':
                lon = -lon

            result = {
                'lat': round(lat, 6),
                'lon': round(lon, 6),
                'symbol': symbol_table + symbol_code,
            }

            # Parse additional data after position (course/speed, altitude, etc.)
            remaining = data[18:] if len(data) > 18 else ''

            # Course/Speed: CCC/SSS
            cs_match = re.search(r'(\d{3})/(\d{3})', remaining)
            if cs_match:
                result['course'] = int(cs_match.group(1))
                result['speed'] = int(cs_match.group(2))  # knots

            # Altitude: /A=NNNNNN
            alt_match = re.search(r'/A=(-?\d+)', remaining)
            if alt_match:
                result['altitude'] = int(alt_match.group(1))  # feet

            return result

    except Exception as e:
        logger.debug(f"Failed to parse position: {e}")

    return None


def parse_weather(data: str) -> dict:
    """Parse APRS weather data."""
    weather = {}

    # Wind direction: cCCC
    match = re.search(r'c(\d{3})', data)
    if match:
        weather['wind_direction'] = int(match.group(1))

    # Wind speed: sSSS (mph)
    match = re.search(r's(\d{3})', data)
    if match:
        weather['wind_speed'] = int(match.group(1))

    # Wind gust: gGGG (mph)
    match = re.search(r'g(\d{3})', data)
    if match:
        weather['wind_gust'] = int(match.group(1))

    # Temperature: tTTT (Fahrenheit)
    match = re.search(r't(-?\d{2,3})', data)
    if match:
        weather['temperature'] = int(match.group(1))

    # Rain last hour: rRRR (hundredths of inch)
    match = re.search(r'r(\d{3})', data)
    if match:
        weather['rain_1h'] = int(match.group(1)) / 100.0

    # Rain last 24h: pPPP
    match = re.search(r'p(\d{3})', data)
    if match:
        weather['rain_24h'] = int(match.group(1)) / 100.0

    # Humidity: hHH (%)
    match = re.search(r'h(\d{2})', data)
    if match:
        h = int(match.group(1))
        weather['humidity'] = 100 if h == 0 else h

    # Barometric pressure: bBBBBB (tenths of millibars)
    match = re.search(r'b(\d{5})', data)
    if match:
        weather['pressure'] = int(match.group(1)) / 10.0

    return weather


def stream_aprs_output(rtl_process: subprocess.Popen, decoder_process: subprocess.Popen) -> None:
    """Stream decoded APRS packets to queue."""
    global aprs_packet_count, aprs_station_count, aprs_last_packet_time, aprs_stations

    try:
        app_module.aprs_queue.put({'type': 'status', 'status': 'started'})

        for line in iter(decoder_process.stdout.readline, b''):
            line = line.decode('utf-8', errors='replace').strip()
            if not line:
                continue

            # direwolf outputs decoded packets, multimon-ng outputs "AFSK1200: ..."
            if line.startswith('AFSK1200:'):
                line = line[9:].strip()

            # Skip non-packet lines
            if '>' not in line or ':' not in line:
                continue

            packet = parse_aprs_packet(line)
            if packet:
                aprs_packet_count += 1
                aprs_last_packet_time = time.time()

                # Track unique stations
                callsign = packet.get('callsign')
                if callsign and callsign not in aprs_stations:
                    aprs_station_count += 1

                # Update station data
                if callsign:
                    aprs_stations[callsign] = {
                        'callsign': callsign,
                        'lat': packet.get('lat'),
                        'lon': packet.get('lon'),
                        'symbol': packet.get('symbol'),
                        'last_seen': packet.get('timestamp'),
                        'packet_type': packet.get('packet_type'),
                    }

                app_module.aprs_queue.put(packet)

                # Log if enabled
                if app_module.logging_enabled:
                    try:
                        with open(app_module.log_file_path, 'a') as f:
                            ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            f.write(f"{ts} | APRS | {json.dumps(packet)}\n")
                    except Exception:
                        pass

    except Exception as e:
        logger.error(f"APRS stream error: {e}")
        app_module.aprs_queue.put({'type': 'error', 'message': str(e)})
    finally:
        app_module.aprs_queue.put({'type': 'status', 'status': 'stopped'})
        # Cleanup processes
        for proc in [rtl_process, decoder_process]:
            try:
                proc.terminate()
                proc.wait(timeout=2)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass


@aprs_bp.route('/tools')
def check_aprs_tools() -> Response:
    """Check for APRS decoding tools."""
    has_rtl_fm = find_rtl_fm() is not None
    has_direwolf = find_direwolf() is not None
    has_multimon = find_multimon_ng() is not None

    return jsonify({
        'rtl_fm': has_rtl_fm,
        'direwolf': has_direwolf,
        'multimon_ng': has_multimon,
        'ready': has_rtl_fm and (has_direwolf or has_multimon),
        'decoder': 'direwolf' if has_direwolf else ('multimon-ng' if has_multimon else None)
    })


@aprs_bp.route('/status')
def aprs_status() -> Response:
    """Get APRS decoder status."""
    running = False
    if app_module.aprs_process:
        running = app_module.aprs_process.poll() is None

    return jsonify({
        'running': running,
        'packet_count': aprs_packet_count,
        'station_count': aprs_station_count,
        'last_packet_time': aprs_last_packet_time,
        'queue_size': app_module.aprs_queue.qsize()
    })


@aprs_bp.route('/stations')
def get_stations() -> Response:
    """Get all tracked APRS stations."""
    return jsonify({
        'stations': list(aprs_stations.values()),
        'count': len(aprs_stations)
    })


@aprs_bp.route('/start', methods=['POST'])
def start_aprs() -> Response:
    """Start APRS decoder."""
    global aprs_packet_count, aprs_station_count, aprs_last_packet_time, aprs_stations

    with app_module.aprs_lock:
        if app_module.aprs_process and app_module.aprs_process.poll() is None:
            return jsonify({
                'status': 'error',
                'message': 'APRS decoder already running'
            }), 409

    # Check for required tools
    rtl_fm_path = find_rtl_fm()
    if not rtl_fm_path:
        return jsonify({
            'status': 'error',
            'message': 'rtl_fm not found. Install with: sudo apt install rtl-sdr'
        }), 400

    # Check for decoder (prefer direwolf, fallback to multimon-ng)
    direwolf_path = find_direwolf()
    multimon_path = find_multimon_ng()

    if not direwolf_path and not multimon_path:
        return jsonify({
            'status': 'error',
            'message': 'No APRS decoder found. Install direwolf or multimon-ng'
        }), 400

    data = request.json or {}

    # Validate inputs
    try:
        device = validate_device_index(data.get('device', '0'))
        gain = validate_gain(data.get('gain', '40'))
        ppm = validate_ppm(data.get('ppm', '0'))
    except ValueError as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400

    # Get frequency for region
    region = data.get('region', 'north_america')
    frequency = APRS_FREQUENCIES.get(region, '144.390')

    # Allow custom frequency override
    if data.get('frequency'):
        frequency = data.get('frequency')

    # Clear queue and reset stats
    while not app_module.aprs_queue.empty():
        try:
            app_module.aprs_queue.get_nowait()
        except queue.Empty:
            break

    aprs_packet_count = 0
    aprs_station_count = 0
    aprs_last_packet_time = None
    aprs_stations = {}

    # Build rtl_fm command
    freq_hz = f"{float(frequency)}M"
    rtl_cmd = [
        rtl_fm_path,
        '-f', freq_hz,
        '-M', 'fm',              # FM demodulation
        '-s', '22050',           # Sample rate for AFSK1200
        '-d', str(device),
    ]

    if gain and str(gain) != '0':
        rtl_cmd.extend(['-g', str(gain)])
    if ppm and str(ppm) != '0':
        rtl_cmd.extend(['-p', str(ppm)])

    # Explicitly output to stdout
    rtl_cmd.append('-')

    # Build decoder command
    if direwolf_path:
        # direwolf flags for receiving AFSK1200 from stdin:
        # -n 1 = mono audio channel
        # -r 22050 = sample rate
        # -b 16 = 16-bit samples
        # -t 0 = disable PTT (receive only)
        # -q d = quiet mode (less debug output)
        # - = read from stdin
        decoder_cmd = [direwolf_path, '-n', '1', '-r', '22050', '-b', '16', '-t', '0', '-q', 'd', '-']
        decoder_name = 'direwolf'
    else:
        decoder_cmd = [multimon_path, '-t', 'raw', '-a', 'AFSK1200', '-']
        decoder_name = 'multimon-ng'

    logger.info(f"Starting APRS decoder: {' '.join(rtl_cmd)} | {' '.join(decoder_cmd)}")

    try:
        # Start rtl_fm
        rtl_process = subprocess.Popen(
            rtl_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True
        )

        # Start decoder with rtl_fm output
        decoder_process = subprocess.Popen(
            decoder_cmd,
            stdin=rtl_process.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True
        )

        # Allow rtl_fm stdout to be consumed by decoder
        rtl_process.stdout.close()

        # Wait briefly to check if processes started
        time.sleep(PROCESS_START_WAIT)

        if rtl_process.poll() is not None:
            stderr = rtl_process.stderr.read().decode('utf-8', errors='replace') if rtl_process.stderr else ''
            error_msg = f'rtl_fm failed to start'
            if stderr:
                error_msg += f': {stderr[:200]}'
            logger.error(error_msg)
            decoder_process.kill()
            return jsonify({'status': 'error', 'message': error_msg}), 500

        # Store reference to decoder process (for status checks)
        app_module.aprs_process = decoder_process
        app_module.aprs_rtl_process = rtl_process

        # Start output streaming thread
        thread = threading.Thread(
            target=stream_aprs_output,
            args=(rtl_process, decoder_process),
            daemon=True
        )
        thread.start()

        return jsonify({
            'status': 'started',
            'frequency': frequency,
            'region': region,
            'device': device,
            'decoder': decoder_name
        })

    except Exception as e:
        logger.error(f"Failed to start APRS decoder: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


@aprs_bp.route('/stop', methods=['POST'])
def stop_aprs() -> Response:
    """Stop APRS decoder."""
    with app_module.aprs_lock:
        processes_to_stop = []

        if hasattr(app_module, 'aprs_rtl_process') and app_module.aprs_rtl_process:
            processes_to_stop.append(app_module.aprs_rtl_process)

        if app_module.aprs_process:
            processes_to_stop.append(app_module.aprs_process)

        if not processes_to_stop:
            return jsonify({
                'status': 'error',
                'message': 'APRS decoder not running'
            }), 400

        for proc in processes_to_stop:
            try:
                proc.terminate()
                proc.wait(timeout=PROCESS_TERMINATE_TIMEOUT)
            except subprocess.TimeoutExpired:
                proc.kill()
            except Exception as e:
                logger.error(f"Error stopping APRS process: {e}")

        app_module.aprs_process = None
        if hasattr(app_module, 'aprs_rtl_process'):
            app_module.aprs_rtl_process = None

    return jsonify({'status': 'stopped'})


@aprs_bp.route('/stream')
def stream_aprs() -> Response:
    """SSE stream for APRS packets."""
    def generate() -> Generator[str, None, None]:
        last_keepalive = time.time()

        while True:
            try:
                msg = app_module.aprs_queue.get(timeout=SSE_QUEUE_TIMEOUT)
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


@aprs_bp.route('/frequencies')
def get_frequencies() -> Response:
    """Get APRS frequencies by region."""
    return jsonify(APRS_FREQUENCIES)
