"""Listening Post routes for radio monitoring and frequency scanning."""

from __future__ import annotations

import json
import os
import queue
import select
import signal
import shutil
import subprocess
import threading
import time
from datetime import datetime
from typing import Generator, Optional, List, Dict

from flask import Blueprint, jsonify, request, Response

from utils.logging import get_logger
from utils.sse import format_sse
from utils.constants import (
    SSE_QUEUE_TIMEOUT,
    SSE_KEEPALIVE_INTERVAL,
    PROCESS_TERMINATE_TIMEOUT,
)
from utils.sdr import SDRFactory, SDRType

logger = get_logger('intercept.listening_post')

listening_post_bp = Blueprint('listening_post', __name__, url_prefix='/listening')

# ============================================
# GLOBAL STATE
# ============================================

# Audio demodulation state
audio_process = None
audio_rtl_process = None
audio_lock = threading.Lock()
audio_running = False
audio_frequency = 0.0
audio_modulation = 'fm'

# Scanner state
scanner_thread: Optional[threading.Thread] = None
scanner_running = False
scanner_lock = threading.Lock()
scanner_paused = False
scanner_current_freq = 0.0
scanner_config = {
    'start_freq': 88.0,
    'end_freq': 108.0,
    'step': 0.1,
    'modulation': 'wfm',
    'squelch': 20,
    'dwell_time': 10.0,  # Seconds to stay on active frequency
    'scan_delay': 0.1,  # Seconds between frequency hops (keep low for fast scanning)
    'device': 0,
    'gain': 40,
    'bias_t': False,  # Bias-T power for external LNA
    'sdr_type': 'rtlsdr',  # SDR type: rtlsdr, hackrf, airspy, limesdr, sdrplay
}

# Activity log
activity_log: List[Dict] = []
activity_log_lock = threading.Lock()
MAX_LOG_ENTRIES = 500

# SSE queue for scanner events
scanner_queue: queue.Queue = queue.Queue(maxsize=100)


# ============================================
# HELPER FUNCTIONS
# ============================================

def find_rtl_fm() -> str | None:
    """Find rtl_fm binary."""
    return shutil.which('rtl_fm')


def find_rx_fm() -> str | None:
    """Find rx_fm binary (SoapySDR FM demodulator for HackRF/Airspy/LimeSDR)."""
    return shutil.which('rx_fm')


def find_ffmpeg() -> str | None:
    """Find ffmpeg for audio encoding."""
    return shutil.which('ffmpeg')




def add_activity_log(event_type: str, frequency: float, details: str = ''):
    """Add entry to activity log."""
    with activity_log_lock:
        entry = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'type': event_type,
            'frequency': frequency,
            'details': details,
        }
        activity_log.insert(0, entry)
        # Trim log
        while len(activity_log) > MAX_LOG_ENTRIES:
            activity_log.pop()

        # Also push to SSE queue
        try:
            scanner_queue.put_nowait({
                'type': 'log',
                'entry': entry
            })
        except queue.Full:
            pass


# ============================================
# SCANNER IMPLEMENTATION
# ============================================

def scanner_loop():
    """Main scanner loop - scans frequencies looking for signals."""
    global scanner_running, scanner_paused, scanner_current_freq, scanner_skip_signal
    global audio_process, audio_rtl_process, audio_running, audio_frequency

    logger.info("Scanner thread started")
    add_activity_log('scanner_start', scanner_config['start_freq'],
                     f"Scanning {scanner_config['start_freq']}-{scanner_config['end_freq']} MHz")

    rtl_fm_path = find_rtl_fm()

    if not rtl_fm_path:
        logger.error("rtl_fm not found")
        add_activity_log('error', 0, 'rtl_fm not found')
        scanner_running = False
        return

    current_freq = scanner_config['start_freq']
    last_signal_time = 0
    signal_detected = False

    try:
        while scanner_running:
            # Check if paused
            if scanner_paused:
                time.sleep(0.1)
                continue

            # Read config values on each iteration (allows live updates)
            step_mhz = scanner_config['step'] / 1000.0
            squelch = scanner_config['squelch']
            mod = scanner_config['modulation']
            gain = scanner_config['gain']
            device = scanner_config['device']

            scanner_current_freq = current_freq

            # Notify clients of frequency change
            try:
                scanner_queue.put_nowait({
                    'type': 'freq_change',
                    'frequency': current_freq,
                    'scanning': not signal_detected
                })
            except queue.Full:
                pass

            # Start rtl_fm at this frequency
            freq_hz = int(current_freq * 1e6)

            # Sample rates
            if mod == 'wfm':
                sample_rate = 170000
                resample_rate = 32000
            elif mod in ['usb', 'lsb']:
                sample_rate = 12000
                resample_rate = 12000
            else:
                sample_rate = 24000
                resample_rate = 24000

            # Don't use squelch in rtl_fm - we want to analyze raw audio
            rtl_cmd = [
                rtl_fm_path,
                '-M', mod,
                '-f', str(freq_hz),
                '-s', str(sample_rate),
                '-r', str(resample_rate),
                '-g', str(gain),
                '-d', str(device),
            ]
            # Add bias-t flag if enabled (for external LNA power)
            if scanner_config.get('bias_t', False):
                rtl_cmd.append('-T')

            try:
                # Start rtl_fm
                rtl_proc = subprocess.Popen(
                    rtl_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL
                )

                # Read audio data for analysis
                audio_data = b''

                # Read audio samples for a short period
                sample_duration = 0.25  # 250ms - balance between speed and detection
                bytes_needed = int(resample_rate * 2 * sample_duration)  # 16-bit mono

                while len(audio_data) < bytes_needed and scanner_running:
                    chunk = rtl_proc.stdout.read(4096)
                    if not chunk:
                        break
                    audio_data += chunk

                # Clean up rtl_fm
                rtl_proc.terminate()
                try:
                    rtl_proc.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    rtl_proc.kill()

                # Analyze audio level
                audio_detected = False
                rms = 0
                threshold = 500
                if len(audio_data) > 100:
                    import struct
                    samples = struct.unpack(f'{len(audio_data)//2}h', audio_data)
                    # Calculate RMS level (root mean square)
                    rms = (sum(s*s for s in samples) / len(samples)) ** 0.5

                    # Threshold based on squelch setting
                    # Lower squelch = more sensitive (lower threshold)
                    # squelch 0 = very sensitive, squelch 100 = only strong signals
                    if mod == 'wfm':
                        # WFM: threshold 500-10000 based on squelch
                        threshold = 500 + (squelch * 95)
                    else:
                        # AM/NFM: threshold 300-6500 based on squelch
                        threshold = 300 + (squelch * 62)

                    audio_detected = rms > threshold

                # Send level info to clients
                try:
                    scanner_queue.put_nowait({
                        'type': 'scan_update',
                        'frequency': current_freq,
                        'level': int(rms),
                        'threshold': int(threshold) if 'threshold' in dir() else 0,
                        'detected': audio_detected
                    })
                except queue.Full:
                    pass

                if audio_detected and scanner_running:
                    if not signal_detected:
                        # New signal found!
                        signal_detected = True
                        last_signal_time = time.time()
                        add_activity_log('signal_found', current_freq,
                                         f'Signal detected on {current_freq:.3f} MHz ({mod.upper()})')
                        logger.info(f"Signal found at {current_freq} MHz")

                        # Start audio streaming for user
                        _start_audio_stream(current_freq, mod)

                        try:
                            scanner_queue.put_nowait({
                                'type': 'signal_found',
                                'frequency': current_freq,
                                'modulation': mod,
                                'audio_streaming': True
                            })
                        except queue.Full:
                            pass

                    # Check for skip signal
                    if scanner_skip_signal:
                        scanner_skip_signal = False
                        signal_detected = False
                        _stop_audio_stream()
                        try:
                            scanner_queue.put_nowait({
                                'type': 'signal_skipped',
                                'frequency': current_freq
                            })
                        except queue.Full:
                            pass
                        # Move to next frequency (step is in kHz, convert to MHz)
                        current_freq += step_mhz
                        if current_freq > scanner_config['end_freq']:
                            current_freq = scanner_config['start_freq']
                        continue

                    # Stay on this frequency (dwell) but check periodically
                    dwell_start = time.time()
                    while (time.time() - dwell_start) < scanner_config['dwell_time'] and scanner_running:
                        if scanner_skip_signal:
                            break
                        time.sleep(0.2)

                    last_signal_time = time.time()

                else:
                    # No signal at this frequency
                    if signal_detected:
                        # Signal lost
                        duration = time.time() - last_signal_time + scanner_config['dwell_time']
                        add_activity_log('signal_lost', current_freq,
                                         f'Signal lost after {duration:.1f}s')
                        signal_detected = False

                        # Stop audio
                        _stop_audio_stream()

                        try:
                            scanner_queue.put_nowait({
                                'type': 'signal_lost',
                                'frequency': current_freq
                            })
                        except queue.Full:
                            pass

                    # Move to next frequency (step is in kHz, convert to MHz)
                    current_freq += step_mhz
                    if current_freq > scanner_config['end_freq']:
                        current_freq = scanner_config['start_freq']
                        add_activity_log('scan_cycle', current_freq, 'Scan cycle complete')

                    time.sleep(scanner_config['scan_delay'])

            except Exception as e:
                logger.error(f"Scanner error at {current_freq} MHz: {e}")
                time.sleep(0.5)

    except Exception as e:
        logger.error(f"Scanner loop error: {e}")
    finally:
        scanner_running = False
        _stop_audio_stream()
        add_activity_log('scanner_stop', scanner_current_freq, 'Scanner stopped')
        logger.info("Scanner thread stopped")


def _start_audio_stream(frequency: float, modulation: str):
    """Start audio streaming at given frequency."""
    global audio_process, audio_rtl_process, audio_running, audio_frequency, audio_modulation

    with audio_lock:
        # Stop any existing stream
        _stop_audio_stream_internal()

        ffmpeg_path = find_ffmpeg()
        if not ffmpeg_path:
            logger.error("ffmpeg not found")
            return

        # Determine SDR type and build appropriate command
        sdr_type_str = scanner_config.get('sdr_type', 'rtlsdr')
        try:
            sdr_type = SDRType(sdr_type_str)
        except ValueError:
            sdr_type = SDRType.RTL_SDR

        # Set sample rates based on modulation
        if modulation == 'wfm':
            sample_rate = 170000
            resample_rate = 32000
        elif modulation in ['usb', 'lsb']:
            sample_rate = 12000
            resample_rate = 12000
        else:
            sample_rate = 24000
            resample_rate = 24000

        # Build the SDR command based on device type
        if sdr_type == SDRType.RTL_SDR:
            # Use rtl_fm for RTL-SDR devices
            rtl_fm_path = find_rtl_fm()
            if not rtl_fm_path:
                logger.error("rtl_fm not found")
                return

            freq_hz = int(frequency * 1e6)
            sdr_cmd = [
                rtl_fm_path,
                '-M', modulation,
                '-f', str(freq_hz),
                '-s', str(sample_rate),
                '-r', str(resample_rate),
                '-g', str(scanner_config['gain']),
                '-d', str(scanner_config['device']),
                '-l', str(scanner_config['squelch']),
            ]
            if scanner_config.get('bias_t', False):
                sdr_cmd.append('-T')
        else:
            # Use SDR abstraction layer for HackRF, Airspy, LimeSDR, SDRPlay
            rx_fm_path = find_rx_fm()
            if not rx_fm_path:
                logger.error(f"rx_fm not found - required for {sdr_type.value}. Install SoapySDR utilities.")
                return

            # Create device and get command builder
            device = SDRFactory.create_default_device(sdr_type, index=scanner_config['device'])
            builder = SDRFactory.get_builder(sdr_type)

            # Build FM demod command
            sdr_cmd = builder.build_fm_demod_command(
                device=device,
                frequency_mhz=frequency,
                sample_rate=resample_rate,
                gain=float(scanner_config['gain']),
                modulation=modulation,
                squelch=scanner_config['squelch'],
                bias_t=scanner_config.get('bias_t', False)
            )
            # Ensure we use the found rx_fm path
            sdr_cmd[0] = rx_fm_path

        encoder_cmd = [
            ffmpeg_path,
            '-hide_banner',
            '-loglevel', 'error',
            '-f', 's16le',
            '-ar', str(resample_rate),
            '-ac', '1',
            '-i', 'pipe:0',
            '-acodec', 'libmp3lame',
            '-b:a', '128k',
            '-ar', '44100',
            '-f', 'mp3',
            'pipe:1'
        ]

        try:
            # Use shell pipe for reliable streaming (Python subprocess piping can be unreliable)
            shell_cmd = f"{' '.join(sdr_cmd)} 2>/dev/null | {' '.join(encoder_cmd)}"
            logger.info(f"Starting audio pipeline: {shell_cmd}")

            audio_rtl_process = None  # Not used in shell mode
            audio_process = subprocess.Popen(
                shell_cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,
                start_new_session=True  # Create new process group for clean shutdown
            )

            # Brief delay to check if process started successfully
            time.sleep(0.3)

            if audio_process.poll() is not None:
                stderr = audio_process.stderr.read().decode() if audio_process.stderr else ''
                logger.error(f"Audio pipeline exited immediately: {stderr}")
                return

            audio_running = True
            audio_frequency = frequency
            audio_modulation = modulation
            logger.info(f"Audio stream started: {frequency} MHz ({modulation}) via {sdr_type.value}")

        except Exception as e:
            logger.error(f"Failed to start audio stream: {e}")


def _stop_audio_stream():
    """Stop audio streaming."""
    with audio_lock:
        _stop_audio_stream_internal()


def _stop_audio_stream_internal():
    """Internal stop (must hold lock)."""
    global audio_process, audio_rtl_process, audio_running, audio_frequency

    # Set flag first to stop any streaming
    audio_running = False
    audio_frequency = 0.0

    # Kill the shell process and its children
    if audio_process:
        try:
            # Kill entire process group (rtl_fm, ffmpeg, shell)
            try:
                os.killpg(os.getpgid(audio_process.pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                audio_process.kill()
            audio_process.wait(timeout=0.5)
        except:
            pass

    audio_process = None
    audio_rtl_process = None

    # Kill any orphaned rtl_fm and ffmpeg processes
    try:
        subprocess.run(['pkill', '-9', 'rtl_fm'], capture_output=True, timeout=0.5)
    except:
        pass
    try:
        subprocess.run(['pkill', '-9', '-f', 'ffmpeg.*pipe:0'], capture_output=True, timeout=0.5)
    except:
        pass

    # Pause for SDR device to be released (important for frequency/modulation changes)
    time.sleep(0.7)


# ============================================
# API ENDPOINTS
# ============================================

@listening_post_bp.route('/tools')
def check_tools() -> Response:
    """Check for required tools."""
    rtl_fm = find_rtl_fm()
    rx_fm = find_rx_fm()
    ffmpeg = find_ffmpeg()

    # Determine which SDR types are supported
    supported_sdr_types = []
    if rtl_fm:
        supported_sdr_types.append('rtlsdr')
    if rx_fm:
        # rx_fm from SoapySDR supports these types
        supported_sdr_types.extend(['hackrf', 'airspy', 'limesdr', 'sdrplay'])

    return jsonify({
        'rtl_fm': rtl_fm is not None,
        'rx_fm': rx_fm is not None,
        'ffmpeg': ffmpeg is not None,
        'available': (rtl_fm is not None or rx_fm is not None) and ffmpeg is not None,
        'supported_sdr_types': supported_sdr_types
    })


@listening_post_bp.route('/scanner/start', methods=['POST'])
def start_scanner() -> Response:
    """Start the frequency scanner."""
    global scanner_thread, scanner_running, scanner_config

    with scanner_lock:
        if scanner_running:
            return jsonify({
                'status': 'error',
                'message': 'Scanner already running'
            }), 409

    data = request.json or {}

    # Update scanner config
    try:
        scanner_config['start_freq'] = float(data.get('start_freq', 88.0))
        scanner_config['end_freq'] = float(data.get('end_freq', 108.0))
        scanner_config['step'] = float(data.get('step', 0.1))
        scanner_config['modulation'] = str(data.get('modulation', 'wfm')).lower()
        scanner_config['squelch'] = int(data.get('squelch', 20))
        scanner_config['dwell_time'] = float(data.get('dwell_time', 3.0))
        scanner_config['scan_delay'] = float(data.get('scan_delay', 0.5))
        scanner_config['device'] = int(data.get('device', 0))
        scanner_config['gain'] = int(data.get('gain', 40))
        scanner_config['bias_t'] = bool(data.get('bias_t', False))
        scanner_config['sdr_type'] = str(data.get('sdr_type', 'rtlsdr')).lower()
    except (ValueError, TypeError) as e:
        return jsonify({
            'status': 'error',
            'message': f'Invalid parameter: {e}'
        }), 400

    # Validate
    if scanner_config['start_freq'] >= scanner_config['end_freq']:
        return jsonify({
            'status': 'error',
            'message': 'start_freq must be less than end_freq'
        }), 400

    # Check tools based on SDR type
    sdr_type = scanner_config['sdr_type']
    if sdr_type == 'rtlsdr':
        if not find_rtl_fm():
            return jsonify({
                'status': 'error',
                'message': 'rtl_fm not found. Install rtl-sdr tools.'
            }), 503
    else:
        if not find_rx_fm():
            return jsonify({
                'status': 'error',
                'message': f'rx_fm not found. Install SoapySDR utilities for {sdr_type}.'
            }), 503

    # Start scanner thread
    scanner_running = True
    scanner_thread = threading.Thread(target=scanner_loop, daemon=True)
    scanner_thread.start()

    return jsonify({
        'status': 'started',
        'config': scanner_config
    })


@listening_post_bp.route('/scanner/stop', methods=['POST'])
def stop_scanner() -> Response:
    """Stop the frequency scanner."""
    global scanner_running

    scanner_running = False
    _stop_audio_stream()

    return jsonify({'status': 'stopped'})


@listening_post_bp.route('/scanner/pause', methods=['POST'])
def pause_scanner() -> Response:
    """Pause/resume the scanner."""
    global scanner_paused

    scanner_paused = not scanner_paused

    if scanner_paused:
        add_activity_log('scanner_pause', scanner_current_freq, 'Scanner paused')
    else:
        add_activity_log('scanner_resume', scanner_current_freq, 'Scanner resumed')

    return jsonify({
        'status': 'paused' if scanner_paused else 'resumed',
        'paused': scanner_paused
    })


# Flag to trigger skip from API
scanner_skip_signal = False


@listening_post_bp.route('/scanner/skip', methods=['POST'])
def skip_signal() -> Response:
    """Skip current signal and continue scanning."""
    global scanner_skip_signal

    if not scanner_running:
        return jsonify({
            'status': 'error',
            'message': 'Scanner not running'
        }), 400

    scanner_skip_signal = True
    add_activity_log('signal_skip', scanner_current_freq, f'Skipped signal at {scanner_current_freq:.3f} MHz')

    return jsonify({
        'status': 'skipped',
        'frequency': scanner_current_freq
    })


@listening_post_bp.route('/scanner/config', methods=['POST'])
def update_scanner_config() -> Response:
    """Update scanner config while running (step, squelch, gain, dwell)."""
    data = request.json or {}

    updated = []

    if 'step' in data:
        scanner_config['step'] = float(data['step'])
        updated.append(f"step={data['step']}kHz")

    if 'squelch' in data:
        scanner_config['squelch'] = int(data['squelch'])
        updated.append(f"squelch={data['squelch']}")

    if 'gain' in data:
        scanner_config['gain'] = int(data['gain'])
        updated.append(f"gain={data['gain']}")

    if 'dwell_time' in data:
        scanner_config['dwell_time'] = int(data['dwell_time'])
        updated.append(f"dwell={data['dwell_time']}s")

    if 'modulation' in data:
        scanner_config['modulation'] = str(data['modulation']).lower()
        updated.append(f"mod={data['modulation']}")

    if updated:
        logger.info(f"Scanner config updated: {', '.join(updated)}")

    return jsonify({
        'status': 'updated',
        'config': scanner_config
    })


@listening_post_bp.route('/scanner/status')
def scanner_status() -> Response:
    """Get scanner status."""
    return jsonify({
        'running': scanner_running,
        'paused': scanner_paused,
        'current_freq': scanner_current_freq,
        'config': scanner_config,
        'audio_streaming': audio_running,
        'audio_frequency': audio_frequency
    })


@listening_post_bp.route('/scanner/stream')
def stream_scanner_events() -> Response:
    """SSE stream for scanner events."""
    def generate() -> Generator[str, None, None]:
        last_keepalive = time.time()

        while True:
            try:
                msg = scanner_queue.get(timeout=SSE_QUEUE_TIMEOUT)
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


@listening_post_bp.route('/scanner/log')
def get_activity_log() -> Response:
    """Get activity log."""
    limit = request.args.get('limit', 100, type=int)
    with activity_log_lock:
        return jsonify({
            'log': activity_log[:limit],
            'total': len(activity_log)
        })


@listening_post_bp.route('/scanner/log/clear', methods=['POST'])
def clear_activity_log() -> Response:
    """Clear activity log."""
    with activity_log_lock:
        activity_log.clear()
    return jsonify({'status': 'cleared'})


@listening_post_bp.route('/presets')
def get_presets() -> Response:
    """Get scanner presets."""
    presets = [
        {'name': 'FM Broadcast', 'start': 88.0, 'end': 108.0, 'step': 0.2, 'mod': 'wfm'},
        {'name': 'Air Band', 'start': 118.0, 'end': 137.0, 'step': 0.025, 'mod': 'am'},
        {'name': 'Marine VHF', 'start': 156.0, 'end': 163.0, 'step': 0.025, 'mod': 'fm'},
        {'name': 'Amateur 2m', 'start': 144.0, 'end': 148.0, 'step': 0.0125, 'mod': 'fm'},
        {'name': 'Amateur 70cm', 'start': 430.0, 'end': 440.0, 'step': 0.025, 'mod': 'fm'},
        {'name': 'PMR446', 'start': 446.0, 'end': 446.2, 'step': 0.0125, 'mod': 'fm'},
        {'name': 'FRS/GMRS', 'start': 462.5, 'end': 467.7, 'step': 0.025, 'mod': 'fm'},
        {'name': 'Weather Radio', 'start': 162.4, 'end': 162.55, 'step': 0.025, 'mod': 'fm'},
    ]
    return jsonify({'presets': presets})


# ============================================
# MANUAL AUDIO ENDPOINTS (for direct listening)
# ============================================

@listening_post_bp.route('/audio/start', methods=['POST'])
def start_audio() -> Response:
    """Start audio at specific frequency (manual mode)."""
    global scanner_running

    logger.info("Audio start request received")

    # Stop scanner if running
    if scanner_running:
        scanner_running = False
        time.sleep(0.5)

    data = request.json or {}

    try:
        frequency = float(data.get('frequency', 0))
        modulation = str(data.get('modulation', 'wfm')).lower()
        squelch = int(data.get('squelch', 0))
        gain = int(data.get('gain', 40))
        device = int(data.get('device', 0))
        sdr_type = str(data.get('sdr_type', 'rtlsdr')).lower()
    except (ValueError, TypeError) as e:
        return jsonify({
            'status': 'error',
            'message': f'Invalid parameter: {e}'
        }), 400

    if frequency <= 0:
        return jsonify({
            'status': 'error',
            'message': 'frequency is required'
        }), 400

    valid_mods = ['fm', 'wfm', 'am', 'usb', 'lsb']
    if modulation not in valid_mods:
        return jsonify({
            'status': 'error',
            'message': f'Invalid modulation. Use: {", ".join(valid_mods)}'
        }), 400

    valid_sdr_types = ['rtlsdr', 'hackrf', 'airspy', 'limesdr', 'sdrplay']
    if sdr_type not in valid_sdr_types:
        return jsonify({
            'status': 'error',
            'message': f'Invalid sdr_type. Use: {", ".join(valid_sdr_types)}'
        }), 400

    # Update config for audio
    scanner_config['squelch'] = squelch
    scanner_config['gain'] = gain
    scanner_config['device'] = device
    scanner_config['sdr_type'] = sdr_type

    _start_audio_stream(frequency, modulation)

    if audio_running:
        return jsonify({
            'status': 'started',
            'frequency': frequency,
            'modulation': modulation
        })
    else:
        return jsonify({
            'status': 'error',
            'message': 'Failed to start audio. Check SDR device.'
        }), 500


@listening_post_bp.route('/audio/stop', methods=['POST'])
def stop_audio() -> Response:
    """Stop audio."""
    _stop_audio_stream()
    return jsonify({'status': 'stopped'})


@listening_post_bp.route('/audio/status')
def audio_status() -> Response:
    """Get audio status."""
    return jsonify({
        'running': audio_running,
        'frequency': audio_frequency,
        'modulation': audio_modulation
    })


@listening_post_bp.route('/audio/stream')
def stream_audio() -> Response:
    """Stream MP3 audio."""
    # Wait for audio to be ready (up to 2 seconds for modulation/squelch changes)
    for _ in range(40):
        if audio_running and audio_process:
            break
        time.sleep(0.05)

    if not audio_running or not audio_process:
        return Response(b'', mimetype='audio/mpeg', status=204)

    def generate():
        try:
            while audio_running and audio_process and audio_process.poll() is None:
                # Use select to avoid blocking forever
                ready, _, _ = select.select([audio_process.stdout], [], [], 2.0)
                if ready:
                    chunk = audio_process.stdout.read(4096)
                    if chunk:
                        yield chunk
                    else:
                        break
        except GeneratorExit:
            pass
        except:
            pass

    return Response(
        generate(),
        mimetype='audio/mpeg',
        headers={
            'Content-Type': 'audio/mpeg',
            'Cache-Control': 'no-cache, no-store',
            'X-Accel-Buffering': 'no',
            'Transfer-Encoding': 'chunked',
        }
    )
