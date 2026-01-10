"""WebSocket-based audio streaming for SDR."""

import subprocess
import threading
import time
import shutil
import json
from flask import Flask

# Try to import flask-sock
try:
    from flask_sock import Sock
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False
    Sock = None

from utils.logging import get_logger

logger = get_logger('intercept.audio_ws')

# Global state
audio_process = None
rtl_process = None
process_lock = threading.Lock()
current_config = {
    'frequency': 118.0,
    'modulation': 'am',
    'squelch': 0,
    'gain': 40,
    'device': 0
}


def find_rtl_fm():
    return shutil.which('rtl_fm')


def find_ffmpeg():
    return shutil.which('ffmpeg')


def kill_audio_processes():
    """Kill any running audio processes."""
    global audio_process, rtl_process

    if audio_process:
        try:
            audio_process.terminate()
            audio_process.wait(timeout=0.5)
        except:
            try:
                audio_process.kill()
            except:
                pass
        audio_process = None

    if rtl_process:
        try:
            rtl_process.terminate()
            rtl_process.wait(timeout=0.5)
        except:
            try:
                rtl_process.kill()
            except:
                pass
        rtl_process = None

    # Kill any orphaned processes
    try:
        subprocess.run(['pkill', '-9', '-f', 'rtl_fm'], capture_output=True, timeout=1)
    except:
        pass

    time.sleep(0.3)


def start_audio_stream(config):
    """Start rtl_fm + ffmpeg pipeline, return the ffmpeg process."""
    global audio_process, rtl_process, current_config

    kill_audio_processes()

    rtl_fm = find_rtl_fm()
    ffmpeg = find_ffmpeg()

    if not rtl_fm or not ffmpeg:
        logger.error("rtl_fm or ffmpeg not found")
        return None

    current_config.update(config)

    freq = config.get('frequency', 118.0)
    mod = config.get('modulation', 'am')
    squelch = config.get('squelch', 0)
    gain = config.get('gain', 40)
    device = config.get('device', 0)

    # Sample rates based on modulation
    if mod == 'wfm':
        sample_rate = 170000
        resample_rate = 32000
    elif mod in ['usb', 'lsb']:
        sample_rate = 12000
        resample_rate = 12000
    else:
        sample_rate = 24000
        resample_rate = 24000

    freq_hz = int(freq * 1e6)

    rtl_cmd = [
        rtl_fm,
        '-M', mod,
        '-f', str(freq_hz),
        '-s', str(sample_rate),
        '-r', str(resample_rate),
        '-g', str(gain),
        '-d', str(device),
        '-l', str(squelch),
    ]

    # Encode to MP3 for browser compatibility
    ffmpeg_cmd = [
        ffmpeg,
        '-hide_banner',
        '-loglevel', 'error',
        '-f', 's16le',
        '-ar', str(resample_rate),
        '-ac', '1',
        '-i', 'pipe:0',
        '-acodec', 'libmp3lame',
        '-b:a', '128k',
        '-f', 'mp3',
        '-flush_packets', '1',
        'pipe:1'
    ]

    try:
        logger.info(f"Starting rtl_fm: {freq} MHz, {mod}")
        rtl_process = subprocess.Popen(
            rtl_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL
        )

        audio_process = subprocess.Popen(
            ffmpeg_cmd,
            stdin=rtl_process.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            bufsize=0
        )

        rtl_process.stdout.close()

        # Check processes started
        time.sleep(0.2)
        if rtl_process.poll() is not None or audio_process.poll() is not None:
            logger.error("Audio process failed to start")
            kill_audio_processes()
            return None

        return audio_process

    except Exception as e:
        logger.error(f"Failed to start audio: {e}")
        kill_audio_processes()
        return None


def init_audio_websocket(app: Flask):
    """Initialize WebSocket audio streaming."""
    if not WEBSOCKET_AVAILABLE:
        logger.warning("flask-sock not installed, WebSocket audio disabled")
        return

    sock = Sock(app)

    @sock.route('/ws/audio')
    def audio_stream(ws):
        """WebSocket endpoint for audio streaming."""
        logger.info("WebSocket audio client connected")

        proc = None
        streaming = False

        try:
            while True:
                # Check for messages from client (non-blocking with timeout)
                try:
                    msg = ws.receive(timeout=0.01)
                    if msg:
                        data = json.loads(msg)
                        cmd = data.get('cmd')

                        if cmd == 'start':
                            config = data.get('config', {})
                            logger.info(f"Starting audio: {config}")
                            with process_lock:
                                proc = start_audio_stream(config)
                            if proc:
                                streaming = True
                                ws.send(json.dumps({'status': 'started'}))
                            else:
                                ws.send(json.dumps({'status': 'error', 'message': 'Failed to start'}))

                        elif cmd == 'stop':
                            logger.info("Stopping audio")
                            streaming = False
                            with process_lock:
                                kill_audio_processes()
                            proc = None
                            ws.send(json.dumps({'status': 'stopped'}))

                        elif cmd == 'tune':
                            # Change frequency/modulation - restart stream
                            config = data.get('config', {})
                            logger.info(f"Retuning: {config}")
                            with process_lock:
                                proc = start_audio_stream(config)
                            if proc:
                                streaming = True
                                ws.send(json.dumps({'status': 'tuned'}))
                            else:
                                streaming = False
                                ws.send(json.dumps({'status': 'error', 'message': 'Failed to tune'}))

                except TimeoutError:
                    pass
                except Exception as e:
                    if "timed out" not in str(e).lower():
                        logger.error(f"WebSocket receive error: {e}")

                # Stream audio data if active
                if streaming and proc and proc.poll() is None:
                    try:
                        chunk = proc.stdout.read(4096)
                        if chunk:
                            ws.send(chunk)
                    except Exception as e:
                        logger.error(f"Audio read error: {e}")
                        streaming = False
                elif streaming:
                    # Process died
                    streaming = False
                    ws.send(json.dumps({'status': 'error', 'message': 'Audio process died'}))
                else:
                    time.sleep(0.01)

        except Exception as e:
            logger.info(f"WebSocket closed: {e}")
        finally:
            with process_lock:
                kill_audio_processes()
            logger.info("WebSocket audio client disconnected")
