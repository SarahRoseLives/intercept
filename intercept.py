#!/usr/bin/env python3
"""
INTERCEPT - Signal Intelligence Platform

A comprehensive signal intelligence tool featuring:
- Pager decoding (POCSAG/FLEX)
- 433MHz sensor monitoring
- ADS-B aircraft tracking with WarGames-style display
- Satellite pass prediction and Iridium burst detection
- WiFi reconnaissance and drone detection
- Bluetooth scanning

Requires RTL-SDR hardware for RF modes.
"""

import sys
import site

# Ensure user site-packages is available (may be disabled when running as root/sudo)
if not site.ENABLE_USER_SITE:
    user_site = site.getusersitepackages()
    if user_site and user_site not in sys.path:
        sys.path.insert(0, user_site)

import subprocess
import shutil
import re
import threading
import queue
import pty
import os
import select
import json
import time
from flask import Flask, render_template_string, jsonify, request, Response, send_file

app = Flask(__name__)


def load_oui_database():
    """Load OUI database from external JSON file, with fallback to built-in."""
    oui_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'oui_database.json')
    try:
        if os.path.exists(oui_file):
            with open(oui_file, 'r') as f:
                data = json.load(f)
                # Remove comment fields
                return {k: v for k, v in data.items() if not k.startswith('_')}
    except Exception as e:
        print(f"[OUI] Error loading oui_database.json: {e}, using built-in database")
    return None  # Will fall back to built-in

# Global process management
current_process = None
sensor_process = None
wifi_process = None
bt_process = None
output_queue = queue.Queue()
sensor_queue = queue.Queue()
wifi_queue = queue.Queue()
bt_queue = queue.Queue()
process_lock = threading.Lock()
sensor_lock = threading.Lock()
wifi_lock = threading.Lock()
bt_lock = threading.Lock()

# Logging settings
logging_enabled = False
log_file_path = 'pager_messages.log'

# WiFi state
wifi_monitor_interface = None
wifi_networks = {}  # BSSID -> network info
wifi_clients = {}   # Client MAC -> client info
wifi_handshakes = []  # Captured handshakes

# Bluetooth state
bt_interface = None
bt_devices = {}      # MAC -> device info
bt_beacons = {}      # MAC -> beacon info (AirTags, Tiles, iBeacons)
bt_services = {}     # MAC -> list of services

# Aircraft (ADS-B) state
adsb_process = None
adsb_queue = queue.Queue()
adsb_lock = threading.Lock()
adsb_aircraft = {}   # ICAO hex -> aircraft info

# Satellite state
satellite_process = None
satellite_queue = queue.Queue()
satellite_lock = threading.Lock()
iridium_bursts = []  # List of detected Iridium bursts
satellite_passes = []  # Predicted satellite passes

# TLE data for satellite tracking (updated periodically)
TLE_SATELLITES = {
    'ISS': ('ISS (ZARYA)',
            '1 25544U 98067A   24001.00000000  .00000000  00000-0  00000-0 0  0000',
            '2 25544  51.6400   0.0000 0000000   0.0000   0.0000 15.50000000000000'),
    'NOAA-15': ('NOAA 15',
                '1 25338U 98030A   24001.00000000  .00000-0  00000-0  00000-0 0  0000',
                '2 25338  98.7300   0.0000 0010000   0.0000   0.0000 14.26000000000000'),
    'NOAA-18': ('NOAA 18',
                '1 28654U 05018A   24001.00000000  .00000-0  00000-0  00000-0 0  0000',
                '2 28654  98.8800   0.0000 0014000   0.0000   0.0000 14.12000000000000'),
    'NOAA-19': ('NOAA 19',
                '1 33591U 09005A   24001.00000000  .00000-0  00000-0  00000-0 0  0000',
                '2 33591  99.1900   0.0000 0014000   0.0000   0.0000 14.12000000000000'),
    'NOAA-20': ('NOAA 20 (JPSS-1)',
                '1 43013U 17073A   24001.00000000  .00000-0  00000-0  00000-0 0  0000',
                '2 43013  98.7400   0.0000 0001000   0.0000   0.0000 14.19000000000000'),
    'METEOR-M2': ('METEOR-M 2',
                  '1 40069U 14037A   24001.00000000  .00000-0  00000-0  00000-0 0  0000',
                  '2 40069  98.5400   0.0000 0005000   0.0000   0.0000 14.21000000000000'),
    'METEOR-M2-3': ('METEOR-M2 3',
                    '1 57166U 23091A   24001.00000000  .00000-0  00000-0  00000-0 0  0000',
                    '2 57166  98.7700   0.0000 0002000   0.0000   0.0000 14.23000000000000'),
}

# Known beacon prefixes for detection
AIRTAG_PREFIXES = ['4C:00']  # Apple continuity
TILE_PREFIXES = ['C4:E7', 'DC:54', 'E4:B0', 'F8:8A']
SAMSUNG_TRACKER = ['58:4D', 'A0:75']

# Drone detection patterns
DRONE_SSID_PATTERNS = [
    # DJI
    'DJI-', 'DJI_', 'Mavic', 'Phantom', 'Spark-', 'Mini-', 'Air-', 'Inspire',
    'Matrice', 'Avata', 'FPV-', 'Osmo', 'RoboMaster', 'Tello',
    # Parrot
    'Parrot', 'Bebop', 'Anafi', 'Disco-', 'Mambo', 'Swing',
    # Autel
    'Autel', 'EVO-', 'Dragonfish', 'Lite+', 'Nano',
    # Skydio
    'Skydio',
    # Other brands
    'Holy Stone', 'Potensic', 'SYMA', 'Hubsan', 'Eachine', 'FIMI',
    'Xiaomi_FIMI', 'Yuneec', 'Typhoon', 'PowerVision', 'PowerEgg',
    # Generic drone patterns
    'Drone', 'UAV-', 'Quadcopter', 'FPV_', 'RC-Drone'
]

# Drone OUI prefixes (MAC address prefixes for drone manufacturers)
DRONE_OUI_PREFIXES = {
    # DJI
    '60:60:1F': 'DJI', '48:1C:B9': 'DJI', '34:D2:62': 'DJI', 'E0:DB:55': 'DJI',
    'C8:6C:87': 'DJI', 'A0:14:3D': 'DJI', '70:D7:11': 'DJI', '98:3A:56': 'DJI',
    # Parrot
    '90:03:B7': 'Parrot', 'A0:14:3D': 'Parrot', '00:12:1C': 'Parrot', '00:26:7E': 'Parrot',
    # Autel
    '8C:F5:A3': 'Autel', 'D8:E0:E1': 'Autel',
    # Yuneec
    '60:60:1F': 'Yuneec',
    # Skydio
    'F8:0F:6F': 'Skydio',
}

# OUI Database for manufacturer lookup (expanded)
OUI_DATABASE = {
    # Apple (extensive list)
    '00:25:DB': 'Apple', '04:52:F3': 'Apple', '0C:3E:9F': 'Apple', '10:94:BB': 'Apple',
    '14:99:E2': 'Apple', '20:78:F0': 'Apple', '28:6A:BA': 'Apple', '3C:22:FB': 'Apple',
    '40:98:AD': 'Apple', '48:D7:05': 'Apple', '4C:57:CA': 'Apple', '54:4E:90': 'Apple',
    '5C:97:F3': 'Apple', '60:F8:1D': 'Apple', '68:DB:CA': 'Apple', '70:56:81': 'Apple',
    '78:7B:8A': 'Apple', '7C:D1:C3': 'Apple', '84:FC:FE': 'Apple', '8C:2D:AA': 'Apple',
    '90:B0:ED': 'Apple', '98:01:A7': 'Apple', '98:D6:BB': 'Apple', 'A4:D1:D2': 'Apple',
    'AC:BC:32': 'Apple', 'B0:34:95': 'Apple', 'B8:C1:11': 'Apple', 'C8:69:CD': 'Apple',
    'D0:03:4B': 'Apple', 'DC:A9:04': 'Apple', 'E0:C7:67': 'Apple', 'F0:18:98': 'Apple',
    'F4:5C:89': 'Apple', '78:4F:43': 'Apple', '00:CD:FE': 'Apple', '04:4B:ED': 'Apple',
    '04:D3:CF': 'Apple', '08:66:98': 'Apple', '0C:74:C2': 'Apple', '10:DD:B1': 'Apple',
    '14:10:9F': 'Apple', '18:EE:69': 'Apple', '1C:36:BB': 'Apple', '24:A0:74': 'Apple',
    '28:37:37': 'Apple', '2C:BE:08': 'Apple', '34:08:BC': 'Apple', '38:C9:86': 'Apple',
    '3C:06:30': 'Apple', '44:D8:84': 'Apple', '48:A9:1C': 'Apple', '4C:32:75': 'Apple',
    '50:32:37': 'Apple', '54:26:96': 'Apple', '58:B0:35': 'Apple', '5C:F7:E6': 'Apple',
    '64:A3:CB': 'Apple', '68:FE:F7': 'Apple', '6C:4D:73': 'Apple', '70:DE:E2': 'Apple',
    '74:E2:F5': 'Apple', '78:67:D7': 'Apple', '7C:04:D0': 'Apple', '80:E6:50': 'Apple',
    '84:78:8B': 'Apple', '88:66:A5': 'Apple', '8C:85:90': 'Apple', '94:E9:6A': 'Apple',
    '9C:F4:8E': 'Apple', 'A0:99:9B': 'Apple', 'A4:83:E7': 'Apple', 'A8:5C:2C': 'Apple',
    'AC:1F:74': 'Apple', 'B0:19:C6': 'Apple', 'B4:F1:DA': 'Apple', 'BC:52:B7': 'Apple',
    'C0:A5:3E': 'Apple', 'C4:B3:01': 'Apple', 'CC:20:E8': 'Apple', 'D0:C5:F3': 'Apple',
    'D4:61:9D': 'Apple', 'D8:1C:79': 'Apple', 'E0:5F:45': 'Apple', 'E4:C6:3D': 'Apple',
    'F0:B4:79': 'Apple', 'F4:0F:24': 'Apple', 'F8:4D:89': 'Apple', 'FC:D8:48': 'Apple',
    # Samsung
    '00:1B:66': 'Samsung', '00:21:19': 'Samsung', '00:26:37': 'Samsung', '5C:0A:5B': 'Samsung',
    '8C:71:F8': 'Samsung', 'C4:73:1E': 'Samsung', '38:2C:4A': 'Samsung', '00:1E:4C': 'Samsung',
    '00:12:47': 'Samsung', '00:15:99': 'Samsung', '00:17:D5': 'Samsung', '00:1D:F6': 'Samsung',
    '00:21:D1': 'Samsung', '00:24:54': 'Samsung', '00:26:5D': 'Samsung', '08:D4:2B': 'Samsung',
    '10:D5:42': 'Samsung', '14:49:E0': 'Samsung', '18:3A:2D': 'Samsung', '1C:66:AA': 'Samsung',
    '24:4B:81': 'Samsung', '28:98:7B': 'Samsung', '2C:AE:2B': 'Samsung', '30:96:FB': 'Samsung',
    '34:C3:AC': 'Samsung', '38:01:95': 'Samsung', '3C:5A:37': 'Samsung', '40:0E:85': 'Samsung',
    '44:4E:1A': 'Samsung', '4C:BC:A5': 'Samsung', '50:01:BB': 'Samsung', '50:A4:D0': 'Samsung',
    '54:88:0E': 'Samsung', '58:C3:8B': 'Samsung', '5C:2E:59': 'Samsung', '60:D0:A9': 'Samsung',
    '64:B3:10': 'Samsung', '68:48:98': 'Samsung', '6C:2F:2C': 'Samsung', '70:F9:27': 'Samsung',
    '74:45:8A': 'Samsung', '78:47:1D': 'Samsung', '7C:0B:C6': 'Samsung', '84:11:9E': 'Samsung',
    '88:32:9B': 'Samsung', '8C:77:12': 'Samsung', '90:18:7C': 'Samsung', '94:35:0A': 'Samsung',
    '98:52:B1': 'Samsung', '9C:02:98': 'Samsung', 'A0:0B:BA': 'Samsung', 'A4:7B:85': 'Samsung',
    'A8:06:00': 'Samsung', 'AC:5F:3E': 'Samsung', 'B0:72:BF': 'Samsung', 'B4:79:A7': 'Samsung',
    'BC:44:86': 'Samsung', 'C0:97:27': 'Samsung', 'C4:42:02': 'Samsung', 'CC:07:AB': 'Samsung',
    'D0:22:BE': 'Samsung', 'D4:87:D8': 'Samsung', 'D8:90:E8': 'Samsung', 'E4:7C:F9': 'Samsung',
    'E8:50:8B': 'Samsung', 'F0:25:B7': 'Samsung', 'F4:7B:5E': 'Samsung', 'FC:A1:3E': 'Samsung',
    # Google
    '54:60:09': 'Google', '00:1A:11': 'Google', 'F4:F5:D8': 'Google', '94:EB:2C': 'Google',
    '64:B5:C6': 'Google', '3C:5A:B4': 'Google', 'F8:8F:CA': 'Google', '20:DF:B9': 'Google',
    '54:27:1E': 'Google', '58:CB:52': 'Google', 'A4:77:33': 'Google', 'F4:0E:22': 'Google',
    # Sony
    '00:13:A9': 'Sony', '00:1D:28': 'Sony', '00:24:BE': 'Sony', '04:5D:4B': 'Sony',
    '08:A9:5A': 'Sony', '10:4F:A8': 'Sony', '24:21:AB': 'Sony', '30:52:CB': 'Sony',
    '40:B8:37': 'Sony', '58:48:22': 'Sony', '70:9E:29': 'Sony', '84:00:D2': 'Sony',
    'AC:9B:0A': 'Sony', 'B4:52:7D': 'Sony', 'BC:60:A7': 'Sony', 'FC:0F:E6': 'Sony',
    # Bose
    '00:0C:8A': 'Bose', '04:52:C7': 'Bose', '08:DF:1F': 'Bose', '2C:41:A1': 'Bose',
    '4C:87:5D': 'Bose', '60:AB:D2': 'Bose', '88:C9:E8': 'Bose', 'D8:9C:67': 'Bose',
    # JBL/Harman
    '00:1D:DF': 'JBL', '08:AE:D6': 'JBL', '20:3C:AE': 'JBL', '44:5E:F3': 'JBL',
    '50:C9:71': 'JBL', '74:5E:1C': 'JBL', '88:C6:26': 'JBL', 'AC:12:2F': 'JBL',
    # Beats (Apple subsidiary)
    '00:61:71': 'Beats', '48:D6:D5': 'Beats', '9C:64:8B': 'Beats', 'A4:E9:75': 'Beats',
    # Jabra/GN Audio
    '00:13:17': 'Jabra', '1C:48:F9': 'Jabra', '50:C2:ED': 'Jabra', '70:BF:92': 'Jabra',
    '74:5C:4B': 'Jabra', '94:16:25': 'Jabra', 'D0:81:7A': 'Jabra', 'E8:EE:CC': 'Jabra',
    # Sennheiser
    '00:1B:66': 'Sennheiser', '00:22:27': 'Sennheiser', 'B8:AD:3E': 'Sennheiser',
    # Xiaomi
    '04:CF:8C': 'Xiaomi', '0C:1D:AF': 'Xiaomi', '10:2A:B3': 'Xiaomi', '18:59:36': 'Xiaomi',
    '20:47:DA': 'Xiaomi', '28:6C:07': 'Xiaomi', '34:CE:00': 'Xiaomi', '38:A4:ED': 'Xiaomi',
    '44:23:7C': 'Xiaomi', '50:64:2B': 'Xiaomi', '58:44:98': 'Xiaomi', '64:09:80': 'Xiaomi',
    '74:23:44': 'Xiaomi', '78:02:F8': 'Xiaomi', '7C:1C:4E': 'Xiaomi', '84:F3:EB': 'Xiaomi',
    '8C:BE:BE': 'Xiaomi', '98:FA:E3': 'Xiaomi', 'A4:77:58': 'Xiaomi', 'AC:C1:EE': 'Xiaomi',
    'B0:E2:35': 'Xiaomi', 'C4:0B:CB': 'Xiaomi', 'C8:47:8C': 'Xiaomi', 'D4:97:0B': 'Xiaomi',
    'E4:46:DA': 'Xiaomi', 'F0:B4:29': 'Xiaomi', 'FC:64:BA': 'Xiaomi',
    # Huawei
    '00:18:82': 'Huawei', '00:1E:10': 'Huawei', '00:25:68': 'Huawei', '04:B0:E7': 'Huawei',
    '08:63:61': 'Huawei', '10:1B:54': 'Huawei', '18:DE:D7': 'Huawei', '20:A6:80': 'Huawei',
    '28:31:52': 'Huawei', '34:12:98': 'Huawei', '3C:47:11': 'Huawei', '48:00:31': 'Huawei',
    '4C:50:77': 'Huawei', '5C:7D:5E': 'Huawei', '60:DE:44': 'Huawei', '70:72:3C': 'Huawei',
    '78:F5:57': 'Huawei', '80:B6:86': 'Huawei', '88:53:D4': 'Huawei', '94:04:9C': 'Huawei',
    'A4:99:47': 'Huawei', 'B4:15:13': 'Huawei', 'BC:76:70': 'Huawei', 'C8:D1:5E': 'Huawei',
    'DC:D2:FC': 'Huawei', 'E4:68:A3': 'Huawei', 'F4:63:1F': 'Huawei',
    # OnePlus/BBK
    '64:A2:F9': 'OnePlus', 'C0:EE:FB': 'OnePlus', '94:65:2D': 'OnePlus',
    # Fitbit
    '2C:09:4D': 'Fitbit', 'C4:D9:87': 'Fitbit', 'E4:88:6D': 'Fitbit',
    # Garmin
    '00:1C:D1': 'Garmin', 'C4:AC:59': 'Garmin', 'E8:0F:C8': 'Garmin',
    # Microsoft
    '00:50:F2': 'Microsoft', '28:18:78': 'Microsoft', '60:45:BD': 'Microsoft',
    '7C:1E:52': 'Microsoft', '98:5F:D3': 'Microsoft', 'B4:0E:DE': 'Microsoft',
    # Intel
    '00:1B:21': 'Intel', '00:1C:C0': 'Intel', '00:1E:64': 'Intel', '00:21:5C': 'Intel',
    '08:D4:0C': 'Intel', '18:1D:EA': 'Intel', '34:02:86': 'Intel', '40:74:E0': 'Intel',
    '48:51:B7': 'Intel', '58:A0:23': 'Intel', '64:D4:DA': 'Intel', '80:19:34': 'Intel',
    '8C:8D:28': 'Intel', 'A4:4E:31': 'Intel', 'B4:6B:FC': 'Intel', 'C8:D0:83': 'Intel',
    # Qualcomm/Atheros
    '00:03:7F': 'Qualcomm', '00:24:E4': 'Qualcomm', '04:F0:21': 'Qualcomm',
    '1C:4B:D6': 'Qualcomm', '88:71:B1': 'Qualcomm', 'A0:65:18': 'Qualcomm',
    # Broadcom
    '00:10:18': 'Broadcom', '00:1A:2B': 'Broadcom', '20:10:7A': 'Broadcom',
    # Realtek
    '00:0A:EB': 'Realtek', '00:E0:4C': 'Realtek', '48:02:2A': 'Realtek',
    '52:54:00': 'Realtek', '80:EA:96': 'Realtek',
    # Logitech
    '00:1F:20': 'Logitech', '34:88:5D': 'Logitech', '6C:B7:49': 'Logitech',
    # Lenovo
    '00:09:2D': 'Lenovo', '28:D2:44': 'Lenovo', '54:EE:75': 'Lenovo', '98:FA:9B': 'Lenovo',
    # Dell
    '00:14:22': 'Dell', '00:1A:A0': 'Dell', '18:DB:F2': 'Dell', '34:17:EB': 'Dell',
    '78:2B:CB': 'Dell', 'A4:BA:DB': 'Dell', 'E4:B9:7A': 'Dell',
    # HP
    '00:0F:61': 'HP', '00:14:C2': 'HP', '10:1F:74': 'HP', '28:80:23': 'HP',
    '38:63:BB': 'HP', '5C:B9:01': 'HP', '80:CE:62': 'HP', 'A0:D3:C1': 'HP',
    # Tile
    'F8:E4:E3': 'Tile', 'C4:E7:BE': 'Tile', 'DC:54:D7': 'Tile', 'E4:B0:21': 'Tile',
    # Raspberry Pi
    'B8:27:EB': 'Raspberry Pi', 'DC:A6:32': 'Raspberry Pi', 'E4:5F:01': 'Raspberry Pi',
    # Amazon
    '00:FC:8B': 'Amazon', '10:CE:A9': 'Amazon', '34:D2:70': 'Amazon', '40:B4:CD': 'Amazon',
    '44:65:0D': 'Amazon', '68:54:FD': 'Amazon', '74:C2:46': 'Amazon', '84:D6:D0': 'Amazon',
    'A0:02:DC': 'Amazon', 'AC:63:BE': 'Amazon', 'B4:7C:9C': 'Amazon', 'FC:65:DE': 'Amazon',
    # Skullcandy
    '00:01:00': 'Skullcandy', '88:E6:03': 'Skullcandy',
    # Bang & Olufsen
    '00:21:3E': 'Bang & Olufsen', '78:C5:E5': 'Bang & Olufsen',
    # Audio-Technica
    'A0:E9:DB': 'Audio-Technica', 'EC:81:93': 'Audio-Technica',
    # Plantronics/Poly
    '00:1D:DF': 'Plantronics', 'B0:B4:48': 'Plantronics', 'E8:FC:AF': 'Plantronics',
    # Anker
    'AC:89:95': 'Anker', 'E8:AB:FA': 'Anker',
    # Misc/Generic
    '00:00:0A': 'Omron', '00:1A:7D': 'Cyber-Blue', '00:1E:3D': 'Alps Electric',
    '00:0B:57': 'Silicon Wave', '00:02:72': 'CC&C',
}

# Try to load from external file (easier to update)
_external_oui = load_oui_database()
if _external_oui:
    OUI_DATABASE = _external_oui
    print(f"[OUI] Loaded {len(OUI_DATABASE)} entries from oui_database.json")
else:
    print(f"[OUI] Using built-in database with {len(OUI_DATABASE)} entries")


HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>INTERCEPT // Signal Intelligence</title>
    <link rel="icon" type="image/svg+xml" href="/favicon.svg">
    <!-- Leaflet.js for aircraft map -->
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" crossorigin=""/>
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" crossorigin=""></script>
    <!-- Leaflet MarkerCluster for aircraft clustering -->
    <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.4.1/dist/MarkerCluster.css"/>
    <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.4.1/dist/MarkerCluster.Default.css"/>
    <script src="https://unpkg.com/leaflet.markercluster@1.4.1/dist/leaflet.markercluster.js"></script>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&family=Rajdhani:wght@400;500;600;700&display=swap');

        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }

        :root {
            --bg-primary: #000000;
            --bg-secondary: #0a0a0a;
            --bg-tertiary: #111111;
            --bg-card: #0d0d0d;
            --accent-cyan: #00d4ff;
            --accent-cyan-dim: #00d4ff40;
            --accent-green: #00ff88;
            --accent-red: #ff3366;
            --accent-orange: #ff8800;
            --text-primary: #ffffff;
            --text-secondary: #888888;
            --text-dim: #444444;
            --border-color: #1a1a1a;
            --border-glow: #00d4ff33;
        }

        [data-theme="light"] {
            --bg-primary: #f5f5f5;
            --bg-secondary: #e8e8e8;
            --bg-tertiary: #dddddd;
            --bg-card: #ffffff;
            --accent-cyan: #0088aa;
            --accent-cyan-dim: #0088aa40;
            --accent-green: #00aa55;
            --accent-red: #cc2244;
            --accent-orange: #cc6600;
            --text-primary: #111111;
            --text-secondary: #555555;
            --text-dim: #999999;
            --border-color: #cccccc;
            --border-glow: #0088aa33;
        }

        [data-theme="light"] body {
            background-image:
                radial-gradient(ellipse at top, #d0e8f0 0%, transparent 50%),
                radial-gradient(ellipse at bottom, #f0f0f0 0%, var(--bg-primary) 100%);
        }

        [data-theme="light"] .leaflet-tile-pane {
            filter: none;
        }

        body {
            font-family: 'Rajdhani', 'Segoe UI', sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
            background-image:
                radial-gradient(ellipse at top, #001a2c 0%, transparent 50%),
                radial-gradient(ellipse at bottom, #0a0a0a 0%, var(--bg-primary) 100%);
        }

        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
        }

        header {
            background: linear-gradient(180deg, var(--bg-secondary) 0%, transparent 100%);
            padding: 30px 20px;
            text-align: center;
            border-bottom: 1px solid var(--border-color);
            margin-bottom: 25px;
            position: relative;
        }

        header::after {
            content: '';
            position: absolute;
            bottom: -1px;
            left: 50%;
            transform: translateX(-50%);
            width: 200px;
            height: 1px;
            background: linear-gradient(90deg, transparent, var(--accent-cyan), transparent);
        }

        header h1 {
            color: var(--text-primary);
            font-size: 2.5em;
            font-weight: 700;
            letter-spacing: 8px;
            text-transform: uppercase;
            margin-bottom: 8px;
            text-shadow: 0 0 30px var(--accent-cyan-dim);
        }

        header p {
            color: var(--text-secondary);
            font-size: 14px;
            letter-spacing: 3px;
            text-transform: uppercase;
        }

        .help-btn {
            position: absolute;
            top: 20px;
            right: 20px;
            width: 32px;
            height: 32px;
            border-radius: 50%;
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            color: var(--text-secondary);
            font-size: 16px;
            font-weight: bold;
            cursor: pointer;
            transition: all 0.2s ease;
            display: flex;
            align-items: center;
            justify-content: center;
            z-index: 100;
        }

        .help-btn:hover {
            border-color: var(--accent-cyan);
            color: var(--accent-cyan);
            box-shadow: 0 0 15px var(--accent-cyan-dim);
        }

        .theme-toggle {
            position: absolute;
            top: 20px;
            right: 60px;
            width: 32px;
            height: 32px;
            border-radius: 50%;
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            color: var(--text-secondary);
            font-size: 16px;
            cursor: pointer;
            transition: all 0.2s ease;
            display: flex;
            align-items: center;
            justify-content: center;
            z-index: 100;
        }

        .theme-toggle:hover {
            border-color: var(--accent-cyan);
            color: var(--accent-cyan);
            box-shadow: 0 0 15px var(--accent-cyan-dim);
        }

        .theme-toggle .icon-sun,
        .theme-toggle .icon-moon {
            position: absolute;
            transition: opacity 0.2s, transform 0.2s;
        }

        .theme-toggle .icon-sun {
            opacity: 0;
            transform: rotate(-90deg);
        }

        .theme-toggle .icon-moon {
            opacity: 1;
            transform: rotate(0deg);
        }

        [data-theme="light"] .theme-toggle .icon-sun {
            opacity: 1;
            transform: rotate(0deg);
        }

        [data-theme="light"] .theme-toggle .icon-moon {
            opacity: 0;
            transform: rotate(90deg);
        }

        .help-modal {
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: rgba(0, 0, 0, 0.85);
            z-index: 10000;
            overflow-y: auto;
            padding: 40px 20px;
        }

        .help-modal.active {
            display: block;
        }

        .help-content {
            max-width: 800px;
            margin: 0 auto;
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 30px;
            position: relative;
        }

        .help-content h2 {
            color: var(--accent-cyan);
            margin-bottom: 20px;
            font-size: 24px;
            letter-spacing: 2px;
        }

        .help-content h3 {
            color: var(--text-primary);
            margin: 25px 0 15px 0;
            font-size: 14px;
            text-transform: uppercase;
            letter-spacing: 1px;
            border-bottom: 1px solid var(--border-color);
            padding-bottom: 8px;
        }

        .help-close {
            position: absolute;
            top: 15px;
            right: 15px;
            background: none;
            border: none;
            color: var(--text-dim);
            font-size: 24px;
            cursor: pointer;
            transition: color 0.2s;
        }

        .help-close:hover {
            color: var(--accent-red);
        }

        .icon-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
            gap: 12px;
            margin: 15px 0;
        }

        .icon-item {
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 10px;
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            border-radius: 4px;
            font-size: 12px;
        }

        .icon-item .icon {
            font-size: 18px;
            width: 30px;
            text-align: center;
        }

        .icon-item .desc {
            color: var(--text-secondary);
        }

        .tip-list {
            list-style: none;
            padding: 0;
            margin: 15px 0;
        }

        .tip-list li {
            padding: 8px 0;
            padding-left: 20px;
            position: relative;
            color: var(--text-secondary);
            font-size: 13px;
            border-bottom: 1px solid var(--border-color);
        }

        .tip-list li:last-child {
            border-bottom: none;
        }

        .tip-list li::before {
            content: '›';
            position: absolute;
            left: 0;
            color: var(--accent-cyan);
            font-weight: bold;
        }

        .help-tabs {
            display: flex;
            gap: 0;
            margin-bottom: 20px;
            border: 1px solid var(--border-color);
            border-radius: 4px;
            overflow: hidden;
        }

        .help-tab {
            flex: 1;
            padding: 10px;
            background: var(--bg-primary);
            border: none;
            color: var(--text-secondary);
            cursor: pointer;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 1px;
            transition: all 0.2s;
        }

        .help-tab:not(:last-child) {
            border-right: 1px solid var(--border-color);
        }

        .help-tab:hover {
            background: var(--bg-secondary);
        }

        .help-tab.active {
            background: var(--accent-cyan);
            color: var(--bg-primary);
        }

        .help-section {
            display: none;
        }

        .help-section.active {
            display: block;
        }

        .logo {
            margin-bottom: 15px;
            animation: logo-pulse 3s ease-in-out infinite;
        }

        .logo svg {
            filter: drop-shadow(0 0 10px var(--accent-cyan-dim));
        }

        @keyframes logo-pulse {
            0%, 100% {
                filter: drop-shadow(0 0 5px var(--accent-cyan-dim));
            }
            50% {
                filter: drop-shadow(0 0 20px var(--accent-cyan));
            }
        }

        .main-content {
            display: grid;
            grid-template-columns: 340px 1fr;
            gap: 25px;
        }

        @media (max-width: 900px) {
            .main-content {
                grid-template-columns: 1fr;
            }
        }

        .sidebar {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            padding: 20px;
            position: relative;
        }

        .sidebar::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 2px;
            background: linear-gradient(90deg, var(--accent-cyan), transparent);
        }

        .section {
            margin-bottom: 20px;
        }

        .section h3 {
            color: var(--accent-cyan);
            margin-bottom: 12px;
            padding-bottom: 8px;
            border-bottom: 1px solid var(--border-color);
            font-size: 11px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 2px;
            display: flex;
            align-items: center;
            gap: 6px;
            cursor: pointer;
            user-select: none;
        }

        .section h3::before {
            content: '▼';
            font-size: 8px;
            color: var(--text-dim);
            transition: transform 0.2s ease;
        }

        .section.collapsed h3::before {
            transform: rotate(-90deg);
        }

        .section.collapsed > *:not(h3) {
            display: none !important;
        }

        .section h3:hover {
            color: var(--text-primary);
        }

        .section h3:hover::before {
            color: var(--accent-cyan);
        }

        .form-group {
            margin-bottom: 12px;
        }

        .form-group label {
            display: block;
            margin-bottom: 4px;
            color: var(--text-secondary);
            font-size: 10px;
            text-transform: uppercase;
            letter-spacing: 1px;
        }

        .form-group input,
        .form-group select {
            width: 100%;
            padding: 10px 12px;
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            color: var(--text-primary);
            font-family: 'JetBrains Mono', monospace;
            font-size: 13px;
            transition: all 0.2s ease;
        }

        .form-group input:focus,
        .form-group select:focus {
            outline: none;
            border-color: var(--accent-cyan);
            box-shadow: 0 0 15px var(--accent-cyan-dim), inset 0 0 15px var(--accent-cyan-dim);
        }

        .checkbox-group {
            display: flex;
            flex-wrap: wrap;
            gap: 12px;
        }

        .checkbox-group label {
            display: flex;
            align-items: center;
            gap: 8px;
            color: var(--text-secondary);
            font-size: 12px;
            cursor: pointer;
            padding: 8px 12px;
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            transition: all 0.2s ease;
        }

        .checkbox-group label:hover {
            border-color: var(--accent-cyan);
        }

        .checkbox-group input[type="checkbox"] {
            width: auto;
            accent-color: var(--accent-cyan);
        }

        .preset-buttons {
            display: flex;
            flex-wrap: wrap;
            gap: 6px;
        }

        .preset-btn {
            padding: 8px 14px;
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            color: var(--text-secondary);
            cursor: pointer;
            font-family: 'JetBrains Mono', monospace;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 1px;
            transition: all 0.2s ease;
            border-radius: 3px;
        }

        .preset-btn:hover {
            background: var(--accent-cyan);
            color: var(--bg-primary);
            border-color: var(--accent-cyan);
            box-shadow: 0 0 20px var(--accent-cyan-dim);
        }

        .run-btn {
            width: 100%;
            padding: 14px;
            background: transparent;
            border: 2px solid var(--accent-green);
            color: var(--accent-green);
            font-family: 'Rajdhani', sans-serif;
            font-size: 13px;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 3px;
            cursor: pointer;
            transition: all 0.3s ease;
            margin-top: 12px;
            position: relative;
            overflow: hidden;
            border-radius: 4px;
        }

        .run-btn::before {
            content: '';
            position: absolute;
            top: 0;
            left: -100%;
            width: 100%;
            height: 100%;
            background: linear-gradient(90deg, transparent, var(--accent-green), transparent);
            opacity: 0.3;
            transition: left 0.5s ease;
        }

        .run-btn:hover {
            background: var(--accent-green);
            color: var(--bg-primary);
            box-shadow: 0 0 30px rgba(0, 255, 136, 0.4);
        }

        .run-btn:hover::before {
            left: 100%;
        }

        .stop-btn {
            width: 100%;
            padding: 16px;
            background: transparent;
            border: 2px solid var(--accent-red);
            color: var(--accent-red);
            font-family: 'Rajdhani', sans-serif;
            font-size: 14px;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 4px;
            cursor: pointer;
            transition: all 0.3s ease;
            margin-top: 15px;
        }

        .stop-btn:hover {
            background: var(--accent-red);
            color: var(--bg-primary);
            box-shadow: 0 0 30px rgba(255, 51, 102, 0.4);
        }

        .output-panel {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            display: flex;
            flex-direction: column;
            position: relative;
        }

        .output-panel::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 2px;
            background: linear-gradient(90deg, transparent, var(--accent-cyan), transparent);
        }

        .output-header {
            padding: 18px 25px;
            background: var(--bg-secondary);
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid var(--border-color);
        }

        .output-header h3 {
            color: var(--text-primary);
            font-size: 12px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 3px;
        }

        .stats {
            display: flex;
            gap: 12px;
            font-size: 10px;
            color: var(--text-secondary);
            font-family: 'JetBrains Mono', monospace;
        }

        .stats > div {
            display: flex;
            align-items: center;
            gap: 4px;
            padding: 4px 8px;
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            border-radius: 3px;
        }

        .stats > div:hover {
            border-color: var(--accent-cyan);
        }

        .stats span {
            color: var(--accent-cyan);
            font-weight: 600;
        }

        .output-content {
            flex: 1;
            padding: 15px;
            overflow-y: auto;
            font-family: 'JetBrains Mono', monospace;
            font-size: 12px;
            background: var(--bg-primary);
            margin: 15px;
            border: 1px solid var(--border-color);
            min-height: 500px;
            max-height: 600px;
        }

        .output-content::-webkit-scrollbar {
            width: 6px;
        }

        .output-content::-webkit-scrollbar-track {
            background: var(--bg-primary);
        }

        .output-content::-webkit-scrollbar-thumb {
            background: var(--border-color);
        }

        .output-content::-webkit-scrollbar-thumb:hover {
            background: var(--accent-cyan);
        }

        .message {
            padding: 15px;
            margin-bottom: 10px;
            border: 1px solid var(--border-color);
            border-left: 3px solid var(--accent-cyan);
            background: var(--bg-secondary);
            position: relative;
            transition: all 0.2s ease;
        }

        .message:hover {
            border-left-color: var(--accent-cyan);
            box-shadow: 0 0 20px var(--accent-cyan-dim);
        }

        .message.pocsag {
            border-left-color: var(--accent-cyan);
        }

        .message.flex {
            border-left-color: var(--accent-orange);
        }

        .message .header {
            display: flex;
            justify-content: space-between;
            margin-bottom: 8px;
            font-size: 10px;
            color: var(--text-dim);
            text-transform: uppercase;
            letter-spacing: 1px;
        }

        .message .protocol {
            color: var(--accent-cyan);
            font-weight: 600;
        }

        .message.pocsag .protocol {
            color: var(--accent-cyan);
        }

        .message.flex .protocol {
            color: var(--accent-orange);
        }

        .message .address {
            color: var(--accent-green);
            font-family: 'JetBrains Mono', monospace;
            font-size: 11px;
            margin-bottom: 8px;
        }

        .message .content {
            color: var(--text-primary);
            word-wrap: break-word;
            font-size: 13px;
            line-height: 1.5;
        }

        .message .content.numeric {
            font-family: 'JetBrains Mono', monospace;
            font-size: 15px;
            letter-spacing: 2px;
            color: var(--accent-cyan);
        }

        .status-bar {
            padding: 15px 25px;
            background: var(--bg-secondary);
            border-top: 1px solid var(--border-color);
            display: flex;
            justify-content: space-between;
            align-items: center;
            font-size: 11px;
        }

        .status-indicator {
            display: flex;
            align-items: center;
            gap: 10px;
            text-transform: uppercase;
            letter-spacing: 2px;
        }

        .status-dot {
            width: 8px;
            height: 8px;
            background: var(--text-dim);
            position: relative;
        }

        .status-dot.running {
            background: var(--accent-green);
            box-shadow: 0 0 10px var(--accent-green);
            animation: pulse-glow 2s infinite;
        }

        @keyframes pulse-glow {
            0%, 100% {
                opacity: 1;
                box-shadow: 0 0 10px var(--accent-green);
            }
            50% {
                opacity: 0.7;
                box-shadow: 0 0 20px var(--accent-green), 0 0 30px var(--accent-green);
            }
        }

        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.3; }
        }

        .clear-btn {
            padding: 8px 16px;
            background: transparent;
            border: 1px solid var(--border-color);
            color: var(--text-secondary);
            cursor: pointer;
            font-size: 10px;
            text-transform: uppercase;
            letter-spacing: 2px;
            transition: all 0.2s ease;
        }

        .clear-btn:hover {
            border-color: var(--accent-cyan);
            color: var(--accent-cyan);
        }

        .tool-status {
            font-size: 10px;
            padding: 4px 10px;
            margin-left: 8px;
            text-transform: uppercase;
            letter-spacing: 1px;
            font-weight: 600;
        }

        .tool-status.ok {
            background: transparent;
            border: 1px solid var(--accent-green);
            color: var(--accent-green);
        }

        .tool-status.missing {
            background: transparent;
            border: 1px solid var(--accent-red);
            color: var(--accent-red);
        }

        .info-text {
            font-size: 10px;
            color: var(--text-dim);
            margin-top: 8px;
            text-transform: uppercase;
            letter-spacing: 1px;
        }

        .header-controls {
            display: flex;
            align-items: center;
            gap: 20px;
        }

        .signal-meter {
            display: flex;
            align-items: flex-end;
            gap: 2px;
            height: 20px;
            padding: 0 10px;
        }

        .signal-bar {
            width: 4px;
            background: var(--border-color);
            transition: all 0.1s ease;
        }

        .signal-bar:nth-child(1) { height: 4px; }
        .signal-bar:nth-child(2) { height: 8px; }
        .signal-bar:nth-child(3) { height: 12px; }
        .signal-bar:nth-child(4) { height: 16px; }
        .signal-bar:nth-child(5) { height: 20px; }

        .signal-bar.active {
            background: var(--accent-cyan);
            box-shadow: 0 0 8px var(--accent-cyan);
        }

        .waterfall-container {
            padding: 0 15px;
            margin-bottom: 10px;
        }

        #waterfallCanvas {
            width: 100%;
            height: 60px;
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            transition: box-shadow 0.3s ease;
        }

        #waterfallCanvas.active {
            box-shadow: 0 0 15px var(--accent-cyan-dim);
            border-color: var(--accent-cyan);
        }

        .status-controls {
            display: flex;
            gap: 8px;
            align-items: center;
        }

        .control-btn {
            padding: 6px 12px;
            background: transparent;
            border: 1px solid var(--border-color);
            color: var(--text-secondary);
            cursor: pointer;
            font-size: 10px;
            text-transform: uppercase;
            letter-spacing: 1px;
            transition: all 0.2s ease;
            font-family: 'Rajdhani', sans-serif;
        }

        .control-btn:hover {
            border-color: var(--accent-cyan);
            color: var(--accent-cyan);
        }

        .control-btn.active {
            border-color: var(--accent-green);
            color: var(--accent-green);
        }

        .control-btn.muted {
            border-color: var(--accent-red);
            color: var(--accent-red);
        }

        /* Signal Strength Graph */
        .signal-graph-panel {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 4px;
            padding: 12px;
            margin-top: 10px;
        }

        .signal-graph-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 10px;
        }

        .signal-graph-header h4 {
            color: var(--accent-cyan);
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin: 0;
        }

        .signal-graph-device {
            font-size: 10px;
            color: var(--text-secondary);
            font-family: 'JetBrains Mono', monospace;
        }

        #signalGraph {
            width: 100%;
            height: 80px;
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            border-radius: 3px;
        }

        /* Network Relationship Graph */
        .network-graph-container {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 4px;
            padding: 12px;
            margin-top: 10px;
        }

        .network-graph-container h4 {
            color: var(--accent-cyan);
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin: 0 0 10px 0;
        }

        #networkGraph {
            width: 100%;
            height: 200px;
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            border-radius: 3px;
        }

        .network-graph-legend {
            display: flex;
            gap: 15px;
            margin-top: 8px;
            font-size: 10px;
        }

        .legend-item {
            display: flex;
            align-items: center;
            gap: 5px;
            color: var(--text-secondary);
        }

        .legend-dot {
            width: 10px;
            height: 10px;
            border-radius: 50%;
        }

        .legend-dot.ap { background: var(--accent-cyan); }
        .legend-dot.client { background: var(--accent-green); }
        .legend-dot.drone { background: var(--accent-orange); }

        /* Channel Recommendation */
        .channel-recommendation {
            background: var(--bg-card);
            border: 1px solid var(--accent-green);
            border-radius: 4px;
            padding: 10px;
            margin-top: 10px;
        }

        .channel-recommendation h4 {
            color: var(--accent-green);
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin: 0 0 8px 0;
        }

        .channel-recommendation .rec-text {
            font-size: 12px;
            color: var(--text-secondary);
        }

        .channel-recommendation .rec-channel {
            font-size: 18px;
            font-weight: bold;
            color: var(--accent-green);
        }

        /* Device Correlation */
        .correlation-badge {
            display: inline-block;
            padding: 2px 6px;
            background: var(--accent-orange);
            color: var(--bg-primary);
            font-size: 9px;
            border-radius: 3px;
            margin-left: 5px;
            font-weight: bold;
        }

        /* Hidden SSID reveal */
        .hidden-ssid-revealed {
            color: var(--accent-orange);
            font-style: italic;
        }

        /* Mode tabs - grouped layout */
        .mode-tabs-container {
            margin-bottom: 15px;
        }

        .tab-group {
            margin-bottom: 8px;
        }

        .tab-group-label {
            font-size: 9px;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 2px;
            margin-bottom: 4px;
            padding-left: 4px;
            font-family: 'Rajdhani', sans-serif;
        }

        .mode-tabs {
            display: flex;
            gap: 0;
            border: 1px solid var(--border-color);
            border-radius: 4px;
            overflow: hidden;
        }

        .mode-tab {
            flex: 1;
            padding: 10px 8px;
            background: var(--bg-primary);
            border: none;
            color: var(--text-secondary);
            cursor: pointer;
            font-family: 'Rajdhani', sans-serif;
            font-size: 10px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 1px;
            transition: all 0.2s ease;
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 4px;
        }

        .mode-tab .tab-icon {
            font-size: 16px;
        }

        .mode-tab:not(:last-child) {
            border-right: 1px solid var(--border-color);
        }

        .mode-tab:hover {
            background: var(--bg-secondary);
            color: var(--text-primary);
        }

        .mode-tab.active {
            background: var(--accent-cyan);
            color: var(--bg-primary);
        }

        .mode-content {
            display: none;
        }

        .mode-content.active {
            display: block;
        }

        /* Aircraft (ADS-B) Styles */
        .aircraft-card {
            padding: 12px;
            margin-bottom: 8px;
            border: 1px solid var(--border-color);
            border-left: 3px solid var(--accent-cyan);
            background: var(--bg-secondary);
            display: grid;
            grid-template-columns: auto 1fr auto;
            gap: 12px;
            align-items: center;
        }

        .aircraft-icon {
            font-size: 28px;
            transform: rotate(var(--heading, 0deg));
            transition: transform 0.3s ease;
        }

        .aircraft-info {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(100px, 1fr));
            gap: 8px;
        }

        .aircraft-callsign {
            color: var(--accent-cyan);
            font-weight: 600;
            font-size: 14px;
        }

        .aircraft-data {
            font-size: 11px;
            color: var(--text-secondary);
        }

        .aircraft-data span {
            color: var(--text-primary);
        }

        /* Aircraft Map Display - Leaflet */
        .aircraft-map-container {
            position: relative;
            width: 100%;
            height: 400px;
            background: #0a0a0a;
            border: 1px solid var(--accent-cyan);
            border-radius: 4px;
            overflow: hidden;
            box-shadow: 0 0 20px rgba(0, 212, 255, 0.2);
        }

        #aircraftMap {
            width: 100%;
            height: 100%;
            background: #0a0a0a;
        }

        /* Dark theme for Leaflet */
        .leaflet-container {
            background: #0a0a0a;
            font-family: 'JetBrains Mono', monospace;
        }

        .leaflet-tile-pane {
            filter: invert(1) hue-rotate(180deg) brightness(0.8) contrast(1.2);
        }

        .leaflet-control-zoom {
            margin-top: 45px !important;
        }

        .leaflet-control-zoom a {
            background: var(--bg-card) !important;
            color: var(--accent-cyan) !important;
            border-color: var(--border-color) !important;
        }

        .leaflet-control-zoom a:hover {
            background: var(--bg-tertiary) !important;
        }

        .leaflet-control-attribution {
            background: rgba(0, 0, 0, 0.7) !important;
            color: #666 !important;
            font-size: 9px !important;
        }

        .leaflet-control-attribution a {
            color: #888 !important;
        }

        .map-header {
            position: absolute;
            top: 8px;
            left: 10px;
            right: 10px;
            display: flex;
            justify-content: space-between;
            z-index: 1000;
            font-family: 'JetBrains Mono', monospace;
            font-size: 11px;
            color: var(--accent-cyan);
            text-shadow: 0 0 5px var(--accent-cyan);
            background: rgba(0, 0, 0, 0.6);
            padding: 4px 8px;
            border-radius: 3px;
        }

        .map-footer {
            position: absolute;
            bottom: 8px;
            left: 10px;
            right: 10px;
            display: flex;
            justify-content: space-between;
            z-index: 1000;
            font-family: 'JetBrains Mono', monospace;
            font-size: 10px;
            color: var(--accent-cyan);
            text-shadow: 0 0 5px var(--accent-cyan);
            background: rgba(0, 0, 0, 0.6);
            padding: 4px 8px;
            border-radius: 3px;
        }

        /* Aircraft marker styles */
        .aircraft-marker {
            display: flex;
            align-items: center;
            justify-content: center;
        }

        .aircraft-marker svg {
            filter: drop-shadow(0 0 4px currentColor);
        }

        .aircraft-popup {
            font-family: 'JetBrains Mono', monospace;
            font-size: 11px;
        }

        .aircraft-popup .callsign {
            color: var(--accent-cyan);
            font-weight: bold;
            font-size: 13px;
        }

        .aircraft-popup .data-row {
            display: flex;
            justify-content: space-between;
            margin: 3px 0;
        }

        .aircraft-popup .label {
            color: #888;
        }

        .aircraft-popup .value {
            color: #fff;
        }

        .leaflet-popup-content-wrapper {
            background: var(--bg-card) !important;
            border: 1px solid var(--border-color) !important;
            border-radius: 4px !important;
        }

        .leaflet-popup-tip {
            background: var(--bg-card) !important;
            border: 1px solid var(--border-color) !important;
        }

        .leaflet-popup-content {
            color: var(--text-primary) !important;
            margin: 10px !important;
        }

        .leaflet-tooltip.aircraft-tooltip {
            background: rgba(0, 0, 0, 0.8) !important;
            border: 1px solid var(--accent-cyan) !important;
            color: var(--accent-cyan) !important;
            font-family: 'JetBrains Mono', monospace !important;
            font-size: 10px !important;
            padding: 2px 6px !important;
            border-radius: 2px !important;
        }

        .leaflet-tooltip.aircraft-tooltip::before {
            border-right-color: var(--accent-cyan) !important;
        }

        /* Satellite Mode Styles */
        .satellite-section {
            margin-bottom: 20px;
        }

        .satellite-tabs {
            display: flex;
            gap: 4px;
            margin-bottom: 15px;
        }

        .satellite-tab {
            padding: 8px 16px;
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 4px;
            color: var(--text-secondary);
            cursor: pointer;
            font-family: 'Rajdhani', sans-serif;
            font-size: 11px;
            text-transform: uppercase;
            transition: all 0.2s ease;
        }

        .satellite-tab:hover {
            border-color: var(--accent-cyan);
            color: var(--text-primary);
        }

        .satellite-tab.active {
            background: var(--accent-cyan);
            border-color: var(--accent-cyan);
            color: var(--bg-primary);
        }

        .satellite-content {
            display: none;
        }

        .satellite-content.active {
            display: block;
        }

        /* Satellite Pass Predictor - Cool UI */
        .pass-predictor {
            display: grid;
            grid-template-columns: 1fr 1fr;
            grid-template-rows: auto auto;
            gap: 15px;
        }

        @media (max-width: 1100px) {
            .pass-predictor {
                grid-template-columns: 1fr;
            }
        }

        .polar-plot-container {
            position: relative;
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 15px;
            min-height: 320px;
        }

        .ground-track-cell {
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 15px;
            min-height: 320px;
        }

        .ground-track-cell #groundTrackMap {
            height: 240px;
        }

        .countdown-cell {
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 15px;
        }

        .countdown-cell .satellite-countdown {
            margin: 0;
            padding: 0;
            border: none;
            background: none;
        }

        .pass-list-cell {
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 15px;
            max-height: 350px;
            overflow-y: auto;
        }

        .polar-plot-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 10px;
        }

        .polar-plot-title {
            color: var(--accent-cyan);
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 1px;
        }

        .popout-btn {
            padding: 4px 10px;
            background: transparent;
            border: 1px solid var(--accent-cyan);
            border-radius: 3px;
            color: var(--accent-cyan);
            font-size: 10px;
            cursor: pointer;
            transition: all 0.2s ease;
        }

        .popout-btn:hover {
            background: var(--accent-cyan);
            color: var(--bg-primary);
        }

        .polar-plot {
            position: relative;
            width: 100%;
            padding-bottom: 100%;
        }

        .polar-plot canvas {
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
        }

        .pass-list-container {
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 15px;
            max-height: 450px;
            overflow-y: auto;
        }

        .pass-list-header {
            color: var(--accent-cyan);
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 10px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        /* Satellite Countdown Block */
        .satellite-countdown {
            background: linear-gradient(135deg, var(--bg-tertiary) 0%, var(--bg-secondary) 100%);
            border: 1px solid var(--accent-cyan);
            border-radius: 8px;
            padding: 15px;
            margin-bottom: 15px;
            box-shadow: 0 0 20px rgba(0, 212, 255, 0.1);
        }

        .countdown-satellite-name {
            color: var(--accent-cyan);
            font-size: 14px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 2px;
            text-align: center;
            margin-bottom: 12px;
            text-shadow: 0 0 10px var(--accent-cyan-dim);
        }

        .countdown-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 10px;
            margin-bottom: 10px;
        }

        .countdown-block {
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            border-radius: 6px;
            padding: 10px;
            text-align: center;
        }

        .countdown-label {
            color: var(--text-dim);
            font-size: 9px;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 5px;
        }

        .countdown-value {
            color: var(--accent-cyan);
            font-size: 22px;
            font-weight: 700;
            font-family: 'JetBrains Mono', monospace;
            text-shadow: 0 0 15px var(--accent-cyan-dim);
            line-height: 1.2;
        }

        .countdown-value.active {
            color: var(--accent-green);
            text-shadow: 0 0 15px rgba(0, 255, 136, 0.4);
            animation: countdown-pulse 1s ease-in-out infinite;
        }

        @keyframes countdown-pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.7; }
        }

        .countdown-sublabel {
            color: var(--text-secondary);
            font-size: 9px;
            margin-top: 4px;
        }

        .countdown-status {
            text-align: center;
            font-size: 10px;
            color: var(--text-dim);
            padding-top: 8px;
            border-top: 1px solid var(--border-color);
        }

        .countdown-status.visible {
            color: var(--accent-green);
        }

        .countdown-status.upcoming {
            color: var(--accent-orange);
        }

        .location-input {
            display: flex;
            gap: 8px;
            margin-bottom: 15px;
        }

        .location-input input {
            flex: 1;
            padding: 8px;
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            border-radius: 3px;
            color: var(--text-primary);
            font-size: 12px;
        }

        .pass-card {
            padding: 12px;
            margin-bottom: 8px;
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            border-radius: 4px;
            cursor: pointer;
            transition: all 0.2s ease;
        }

        .pass-card:hover {
            border-color: var(--accent-cyan);
        }

        .pass-card.active {
            border-color: var(--accent-cyan);
            box-shadow: 0 0 10px rgba(0, 255, 255, 0.2);
        }

        .pass-satellite {
            color: var(--accent-cyan);
            font-weight: 600;
            margin-bottom: 4px;
        }

        .pass-time {
            color: var(--text-primary);
            font-size: 13px;
            margin-bottom: 4px;
        }

        .pass-details {
            display: flex;
            gap: 15px;
            font-size: 11px;
            color: var(--text-secondary);
        }

        .pass-details span {
            color: var(--text-primary);
        }

        .pass-quality {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 10px;
            font-size: 10px;
            font-weight: 600;
        }

        .pass-quality.excellent {
            background: rgba(0, 255, 0, 0.2);
            color: var(--accent-green);
        }

        .pass-quality.good {
            background: rgba(0, 255, 255, 0.2);
            color: var(--accent-cyan);
        }

        .pass-quality.fair {
            background: rgba(255, 102, 0, 0.2);
            color: var(--accent-orange);
        }

        /* Satellite List Styles */
        .satellite-list {
            max-height: 200px;
            overflow-y: auto;
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            border-radius: 4px;
            padding: 8px;
        }

        .sat-item {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 6px 8px;
            margin-bottom: 4px;
            background: var(--bg-secondary);
            border-radius: 3px;
            font-size: 12px;
        }

        .sat-item:last-child {
            margin-bottom: 0;
        }

        .sat-item label {
            display: flex;
            align-items: center;
            gap: 8px;
            cursor: pointer;
            flex: 1;
        }

        .sat-item input[type="checkbox"] {
            margin: 0;
        }

        .sat-item .sat-name {
            color: var(--text-primary);
        }

        .sat-item .sat-norad {
            color: var(--text-secondary);
            font-size: 10px;
        }

        .sat-item .sat-remove {
            background: none;
            border: none;
            color: var(--accent-red);
            cursor: pointer;
            font-size: 14px;
            padding: 2px 6px;
            opacity: 0.6;
        }

        .sat-item .sat-remove:hover {
            opacity: 1;
        }

        .sat-item.builtin .sat-remove {
            display: none;
        }

        /* Satellite Add Modal */
        .sat-modal {
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0, 0, 0, 0.8);
            display: none;
            justify-content: center;
            align-items: center;
            z-index: 10000;
        }

        .sat-modal.active {
            display: flex;
        }

        .sat-modal-content {
            background: var(--bg-secondary);
            border: 1px solid var(--accent-cyan);
            border-radius: 8px;
            padding: 20px;
            width: 90%;
            max-width: 500px;
            max-height: 80vh;
            overflow-y: auto;
        }

        .sat-modal-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 1px solid var(--border-color);
        }

        .sat-modal-header h3 {
            color: var(--accent-cyan);
            margin: 0;
        }

        .sat-modal-close {
            background: none;
            border: none;
            color: var(--text-secondary);
            font-size: 24px;
            cursor: pointer;
        }

        .sat-modal-close:hover {
            color: var(--text-primary);
        }

        .sat-modal-tabs {
            display: flex;
            gap: 10px;
            margin-bottom: 15px;
        }

        .sat-modal-tab {
            flex: 1;
            padding: 8px;
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            border-radius: 4px;
            color: var(--text-secondary);
            cursor: pointer;
            font-size: 12px;
        }

        .sat-modal-tab.active {
            border-color: var(--accent-cyan);
            color: var(--accent-cyan);
        }

        .sat-modal-section {
            display: none;
        }

        .sat-modal-section.active {
            display: block;
        }

        .tle-textarea {
            width: 100%;
            height: 120px;
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            border-radius: 4px;
            color: var(--text-primary);
            font-family: monospace;
            font-size: 11px;
            padding: 10px;
            resize: vertical;
        }

        .celestrak-categories {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 8px;
            max-height: 200px;
            overflow-y: auto;
        }

        .celestrak-cat {
            padding: 8px 12px;
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            border-radius: 4px;
            color: var(--text-secondary);
            cursor: pointer;
            font-size: 11px;
            text-align: center;
        }

        .celestrak-cat:hover {
            border-color: var(--accent-cyan);
            color: var(--accent-cyan);
        }

        /* Iridium Burst Styles */
        .iridium-warning {
            padding: 12px;
            margin-bottom: 15px;
            background: rgba(255, 102, 0, 0.1);
            border: 1px solid var(--accent-orange);
            border-radius: 4px;
            color: var(--accent-orange);
            font-size: 12px;
        }

        .iridium-warning strong {
            display: block;
            margin-bottom: 4px;
        }

        .burst-card {
            padding: 10px;
            margin-bottom: 6px;
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-left: 3px solid #9370DB;
            font-family: monospace;
            font-size: 11px;
        }

        .burst-time {
            color: var(--text-secondary);
        }

        .burst-freq {
            color: #9370DB;
            font-weight: 600;
        }

        .burst-data {
            color: var(--text-primary);
            word-break: break-all;
        }

        /* Popout window styles */
        .popout-container {
            position: fixed;
            top: 0;
            left: 0;
            width: 100vw;
            height: 100vh;
            background: var(--bg-primary);
            z-index: 10000;
            display: none;
        }

        .popout-container.active {
            display: block;
        }

        .popout-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 15px 20px;
            background: var(--bg-secondary);
            border-bottom: 1px solid var(--border-color);
        }

        .popout-title {
            color: var(--accent-cyan);
            font-size: 18px;
            font-weight: 600;
        }

        .popout-close {
            padding: 8px 16px;
            background: transparent;
            border: 1px solid var(--accent-red);
            border-radius: 3px;
            color: var(--accent-red);
            cursor: pointer;
        }

        .popout-body {
            padding: 20px;
            height: calc(100vh - 70px);
            overflow: auto;
        }

        /* Sensor card styling */
        .sensor-card {
            padding: 15px;
            margin-bottom: 10px;
            border: 1px solid var(--border-color);
            border-left: 3px solid var(--accent-green);
            background: var(--bg-secondary);
        }

        .sensor-card .device-name {
            color: var(--accent-green);
            font-weight: 600;
            font-size: 13px;
            margin-bottom: 8px;
        }

        .sensor-card .sensor-data {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
            gap: 8px;
        }

        .sensor-card .data-item {
            background: var(--bg-primary);
            padding: 8px 10px;
            border: 1px solid var(--border-color);
        }

        .sensor-card .data-label {
            font-size: 9px;
            color: var(--text-dim);
            text-transform: uppercase;
            letter-spacing: 1px;
        }

        .sensor-card .data-value {
            font-family: 'JetBrains Mono', monospace;
            font-size: 14px;
            color: var(--accent-cyan);
        }

        /* Recon Dashboard - Prominent Device Intelligence */
        .recon-panel {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            margin: 15px;
            margin-bottom: 10px;
            position: relative;
        }

        .recon-panel::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 2px;
            background: linear-gradient(90deg, var(--accent-orange), var(--accent-cyan), transparent);
        }

        .recon-panel.collapsed .recon-content {
            display: none;
        }

        .recon-header {
            padding: 12px 15px;
            background: var(--bg-secondary);
            border-bottom: 1px solid var(--border-color);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .recon-header h4 {
            color: var(--accent-orange);
            font-size: 11px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 2px;
            margin: 0;
        }

        .recon-stats {
            display: flex;
            gap: 15px;
            font-size: 10px;
            font-family: 'JetBrains Mono', monospace;
        }

        .recon-stats span {
            color: var(--accent-cyan);
        }

        .recon-content {
            max-height: 300px;
            overflow-y: auto;
        }

        .device-row {
            display: grid;
            grid-template-columns: 1fr auto auto auto;
            gap: 10px;
            padding: 10px 15px;
            border-bottom: 1px solid var(--border-color);
            font-size: 11px;
            align-items: center;
            transition: background 0.2s ease;
        }

        .device-row:hover {
            background: var(--bg-secondary);
        }

        .device-row.anomaly {
            border-left: 3px solid var(--accent-red);
            background: rgba(255, 51, 102, 0.05);
        }

        .device-row.new-device {
            border-left: 3px solid var(--accent-green);
            background: rgba(0, 255, 136, 0.05);
        }

        .device-info {
            display: flex;
            flex-direction: column;
            gap: 2px;
        }

        .device-name-row {
            color: var(--text-primary);
            font-weight: 500;
        }

        .device-id {
            color: var(--text-dim);
            font-family: 'JetBrains Mono', monospace;
            font-size: 10px;
        }

        .device-meta {
            text-align: right;
            color: var(--text-secondary);
            font-family: 'JetBrains Mono', monospace;
        }

        .device-meta.encrypted {
            color: var(--accent-green);
        }

        .device-meta.plaintext {
            color: var(--accent-red);
        }

        .transmission-bar {
            width: 60px;
            height: 4px;
            background: var(--border-color);
            position: relative;
        }

        .transmission-bar-fill {
            height: 100%;
            background: var(--accent-cyan);
            transition: width 0.3s ease;
        }

        .badge {
            display: inline-block;
            padding: 2px 6px;
            font-size: 9px;
            text-transform: uppercase;
            letter-spacing: 1px;
            border: 1px solid;
        }

        .badge.proto-pocsag { border-color: var(--accent-cyan); color: var(--accent-cyan); }
        .badge.proto-flex { border-color: var(--accent-orange); color: var(--accent-orange); }
        .badge.proto-433 { border-color: var(--accent-green); color: var(--accent-green); }
        .badge.proto-unknown { border-color: var(--text-dim); color: var(--text-dim); }

        .recon-toggle {
            padding: 4px 8px;
            background: transparent;
            border: 1px solid var(--border-color);
            color: var(--text-secondary);
            cursor: pointer;
            font-size: 9px;
            text-transform: uppercase;
            letter-spacing: 1px;
        }

        .recon-toggle:hover {
            border-color: var(--accent-orange);
            color: var(--accent-orange);
        }

        .recon-toggle.active {
            border-color: var(--accent-orange);
            color: var(--accent-orange);
            background: rgba(255, 136, 0, 0.1);
        }

        .hex-dump {
            font-family: 'JetBrains Mono', monospace;
            font-size: 10px;
            color: var(--text-dim);
            background: var(--bg-primary);
            padding: 8px;
            margin-top: 8px;
            border: 1px solid var(--border-color);
            word-break: break-all;
        }

        .timeline-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: var(--accent-cyan);
            display: inline-block;
            margin-right: 5px;
        }

        .timeline-dot.recent { background: var(--accent-green); }
        .timeline-dot.stale { background: var(--accent-orange); }
        .timeline-dot.old { background: var(--text-dim); }

        /* WiFi Visualizations */
        .wifi-visuals {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 10px;
            padding: 15px;
            background: var(--bg-secondary);
            margin: 0 15px 10px 15px;
            border: 1px solid var(--border-color);
        }

        @media (max-width: 1200px) {
            .wifi-visuals { grid-template-columns: 1fr; }
        }

        .wifi-visual-panel {
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            padding: 10px;
            position: relative;
        }

        .wifi-visual-panel h5 {
            color: var(--accent-cyan);
            font-size: 10px;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 10px;
            padding-bottom: 5px;
            border-bottom: 1px solid var(--border-color);
        }

        /* Radar Display */
        .radar-container {
            position: relative;
            width: 150px;
            height: 150px;
            margin: 0 auto;
        }

        #radarCanvas, #btRadarCanvas {
            width: 100%;
            height: 100%;
            border-radius: 50%;
            background: radial-gradient(circle, #001515 0%, #000a0a 100%);
            border: 1px solid var(--accent-cyan-dim);
        }

        #btRadarCanvas {
            background: radial-gradient(circle, #150015 0%, #0a000a 100%);
            border: 1px solid rgba(138, 43, 226, 0.3);
        }

        /* Channel Graph */
        .channel-graph {
            display: flex;
            align-items: flex-end;
            justify-content: space-around;
            height: 60px;
            padding: 5px 0;
            border-bottom: 1px solid var(--border-color);
        }

        .channel-bar-wrapper {
            display: flex;
            flex-direction: column;
            align-items: center;
            flex: 1;
        }

        .channel-bar {
            width: 80%;
            background: var(--border-color);
            min-height: 2px;
            transition: height 0.3s ease, background 0.3s ease;
        }

        .channel-bar.active {
            background: var(--accent-cyan);
            box-shadow: 0 0 5px var(--accent-cyan);
        }

        .channel-bar.congested {
            background: var(--accent-orange);
        }

        .channel-bar.very-congested {
            background: var(--accent-red);
        }

        .channel-label {
            font-size: 8px;
            color: #fff;
            margin-top: 3px;
        }

        /* Security Donut */
        .security-container {
            display: flex;
            align-items: center;
            gap: 15px;
        }

        .security-donut {
            width: 80px;
            height: 80px;
            flex-shrink: 0;
        }

        #securityCanvas {
            width: 100%;
            height: 100%;
        }

        .security-legend {
            display: flex;
            flex-direction: column;
            gap: 4px;
            font-size: 10px;
            font-family: 'JetBrains Mono', monospace;
        }

        .security-legend-item {
            display: flex;
            align-items: center;
            gap: 6px;
        }

        .security-legend-dot {
            width: 10px;
            height: 10px;
            border-radius: 2px;
        }

        .security-legend-dot.wpa3 { background: var(--accent-green); }
        .security-legend-dot.wpa2 { background: var(--accent-orange); }
        .security-legend-dot.wep { background: var(--accent-red); }
        .security-legend-dot.open { background: var(--accent-cyan); }

        /* Signal Strength Meter */
        .signal-strength-display {
            text-align: center;
            padding: 5px;
        }

        .target-ssid {
            font-size: 11px;
            color: var(--text-secondary);
            margin-bottom: 5px;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .signal-value {
            font-family: 'JetBrains Mono', monospace;
            font-size: 28px;
            color: var(--accent-cyan);
            text-shadow: 0 0 10px var(--accent-cyan-dim);
        }

        .signal-value.weak { color: var(--accent-red); text-shadow: 0 0 10px rgba(255,51,102,0.4); }
        .signal-value.medium { color: var(--accent-orange); text-shadow: 0 0 10px rgba(255,136,0,0.4); }
        .signal-value.strong { color: var(--accent-green); text-shadow: 0 0 10px rgba(0,255,136,0.4); }

        .signal-bars-large {
            display: flex;
            justify-content: center;
            align-items: flex-end;
            gap: 3px;
            height: 30px;
            margin-top: 8px;
        }

        .signal-bar-large {
            width: 8px;
            background: var(--border-color);
            transition: all 0.2s ease;
        }

        .signal-bar-large.active {
            box-shadow: 0 0 5px currentColor;
        }

        .signal-bar-large.weak { background: var(--accent-red); }
        .signal-bar-large.medium { background: var(--accent-orange); }
        .signal-bar-large.strong { background: var(--accent-green); }

        .signal-bar-large:nth-child(1) { height: 20%; }
        .signal-bar-large:nth-child(2) { height: 40%; }
        .signal-bar-large:nth-child(3) { height: 60%; }
        .signal-bar-large:nth-child(4) { height: 80%; }
        .signal-bar-large:nth-child(5) { height: 100%; }

        /* Scanline effect overlay */
        body::before {
            content: '';
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            pointer-events: none;
            background: repeating-linear-gradient(
                0deg,
                rgba(0, 0, 0, 0.03),
                rgba(0, 0, 0, 0.03) 1px,
                transparent 1px,
                transparent 2px
            );
            z-index: 1000;
        }

        /* Disclaimer Modal */
        .disclaimer-overlay {
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0, 0, 0, 0.95);
            z-index: 9999;
            display: flex;
            align-items: center;
            justify-content: center;
        }

        .disclaimer-modal {
            background: var(--bg-card);
            border: 1px solid var(--accent-cyan);
            max-width: 550px;
            padding: 30px;
            text-align: center;
            box-shadow: 0 0 50px rgba(0, 212, 255, 0.3);
        }

        .disclaimer-modal h2 {
            color: var(--accent-red);
            font-size: 1.5em;
            margin-bottom: 20px;
            letter-spacing: 3px;
        }

        .disclaimer-modal .warning-icon {
            font-size: 48px;
            margin-bottom: 15px;
        }

        .disclaimer-modal p {
            color: var(--text-secondary);
            font-size: 13px;
            line-height: 1.8;
            margin-bottom: 15px;
            text-align: left;
        }

        .disclaimer-modal ul {
            text-align: left;
            color: var(--text-secondary);
            font-size: 12px;
            margin: 15px 0;
            padding-left: 20px;
        }

        .disclaimer-modal ul li {
            margin-bottom: 8px;
        }

        .disclaimer-modal .accept-btn {
            background: var(--accent-cyan);
            color: #000;
            border: none;
            padding: 12px 40px;
            font-family: 'Rajdhani', sans-serif;
            font-size: 14px;
            font-weight: 600;
            letter-spacing: 2px;
            cursor: pointer;
            margin-top: 20px;
            transition: all 0.3s ease;
        }

        .disclaimer-modal .accept-btn:hover {
            background: #fff;
            box-shadow: 0 0 20px rgba(0, 212, 255, 0.5);
        }

        .disclaimer-hidden {
            display: none !important;
        }

        /* Ground Track Map */
        #groundTrackMap {
            height: 240px;
            border-radius: 6px;
            margin-top: 10px;
        }

        .ground-track-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 5px;
        }

        .ground-track-title {
            font-size: 14px;
            font-weight: 600;
            color: var(--accent-cyan);
        }

        .sat-position-indicator {
            position: absolute;
            width: 12px;
            height: 12px;
            background: #ff0;
            border: 2px solid #000;
            border-radius: 50%;
            transform: translate(-50%, -50%);
            z-index: 100;
            animation: pulse-sat 1s infinite;
        }

        @keyframes pulse-sat {
            0%, 100% { box-shadow: 0 0 0 0 rgba(255, 255, 0, 0.7); }
            50% { box-shadow: 0 0 0 8px rgba(255, 255, 0, 0); }
        }

        /* Beacon Flood Alert */
        .beacon-flood-alert {
            background: linear-gradient(135deg, rgba(255, 0, 0, 0.2), rgba(255, 100, 0, 0.2));
            border: 1px solid #ff4444;
            border-radius: 6px;
            padding: 10px;
            margin: 10px 0;
            animation: beacon-flash 0.5s infinite alternate;
        }

        @keyframes beacon-flash {
            from { opacity: 0.8; }
            to { opacity: 1; }
        }

        /* WPS Indicator */
        .wps-enabled {
            background: #ff6600;
            color: #000;
            padding: 2px 6px;
            border-radius: 3px;
            font-size: 9px;
            font-weight: bold;
            margin-left: 5px;
        }

        /* PMKID Capture */
        .pmkid-btn {
            background: linear-gradient(135deg, #9933ff, #6600cc);
            color: #fff;
        }

        .pmkid-btn:hover {
            background: linear-gradient(135deg, #aa44ff, #7700dd);
        }

        /* Find My Detection */
        .findmy-device {
            border-left: 3px solid #007aff;
            background: rgba(0, 122, 255, 0.1);
        }

        .findmy-badge {
            background: linear-gradient(135deg, #007aff, #5856d6);
            color: #fff;
            padding: 2px 8px;
            border-radius: 10px;
            font-size: 10px;
            font-weight: bold;
        }

        /* Tracker Following Alert */
        .tracker-following-alert {
            background: linear-gradient(135deg, rgba(255, 0, 0, 0.3), rgba(255, 50, 50, 0.2));
            border: 2px solid #ff0000;
            border-radius: 8px;
            padding: 15px;
            margin: 10px 0;
            animation: tracker-pulse 1s infinite;
        }

        @keyframes tracker-pulse {
            0%, 100% { border-color: #ff0000; }
            50% { border-color: #ff6666; }
        }

        .tracker-following-alert h4 {
            color: #ff4444;
            margin: 0 0 10px 0;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        /* Flight Path Trails */
        .flight-trail {
            stroke-dasharray: 5, 5;
            fill: none;
        }

        /* Squawk Alerts */
        .squawk-emergency {
            background: #ff0000 !important;
            animation: squawk-flash 0.3s infinite alternate;
        }

        .squawk-hijack {
            background: #ff0000 !important;
        }

        .squawk-radio-fail {
            background: #ff6600 !important;
        }

        .squawk-mayday {
            background: #ff0000 !important;
        }

        @keyframes squawk-flash {
            from { opacity: 0.7; }
            to { opacity: 1; }
        }

        .squawk-alert-banner {
            position: fixed;
            top: 60px;
            left: 50%;
            transform: translateX(-50%);
            background: #ff0000;
            color: #fff;
            padding: 15px 30px;
            border-radius: 8px;
            font-weight: bold;
            z-index: 9999;
            animation: squawk-banner-flash 0.5s infinite alternate;
        }

        @keyframes squawk-banner-flash {
            from { background: #ff0000; }
            to { background: #cc0000; }
        }

        /* Military Aircraft */
        .military-aircraft {
            border-left: 3px solid #556b2f;
            background: rgba(85, 107, 47, 0.2);
        }

        .military-badge {
            background: #556b2f;
            color: #fff;
            padding: 2px 6px;
            border-radius: 3px;
            font-size: 9px;
            font-weight: bold;
        }

        /* Map Clustering */
        .marker-cluster {
            background: rgba(0, 212, 255, 0.6);
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: bold;
            color: #000;
            border: 2px solid var(--accent-cyan);
        }

        .marker-cluster-small {
            width: 30px;
            height: 30px;
            font-size: 12px;
        }

        .marker-cluster-medium {
            width: 40px;
            height: 40px;
            font-size: 14px;
        }

        .marker-cluster-large {
            width: 50px;
            height: 50px;
            font-size: 16px;
        }
    </style>
</head>
<body>
    <!-- Disclaimer Modal -->
    <div class="disclaimer-overlay" id="disclaimerModal">
        <div class="disclaimer-modal">
            <div class="warning-icon">⚠️</div>
            <h2>DISCLAIMER</h2>
            <p>
                <strong>INTERCEPT</strong> is a signal intelligence tool designed for <strong>educational purposes only</strong>.
            </p>
            <p>By using this software, you acknowledge and agree that:</p>
            <ul>
                <li>This tool is intended for use by <strong>cyber security professionals</strong> and researchers only</li>
                <li>You will only use this software in a <strong>controlled environment</strong> with proper authorization</li>
                <li>Intercepting communications without consent may be <strong>illegal</strong> in your jurisdiction</li>
                <li>You are solely responsible for ensuring compliance with all applicable laws and regulations</li>
                <li>The developers assume no liability for misuse of this software</li>
            </ul>
            <p style="color: var(--accent-red); font-weight: bold;">
                Only proceed if you understand and accept these terms.
            </p>
            <div style="display: flex; gap: 15px; justify-content: center; margin-top: 20px;">
                <button class="accept-btn" onclick="acceptDisclaimer()">I UNDERSTAND & ACCEPT</button>
                <button class="accept-btn" onclick="declineDisclaimer()" style="background: transparent; border: 1px solid var(--accent-red); color: var(--accent-red);">DECLINE</button>
            </div>
        </div>
    </div>

    <!-- Rejection Page -->
    <div class="disclaimer-overlay disclaimer-hidden" id="rejectionPage">
        <div class="disclaimer-modal" style="max-width: 600px;">
            <pre style="color: var(--accent-red); font-size: 9px; line-height: 1.1; margin-bottom: 20px; text-align: center;">
 █████╗  ██████╗ ██████╗███████╗███████╗███████╗
██╔══██╗██╔════╝██╔════╝██╔════╝██╔════╝██╔════╝
███████║██║     ██║     █████╗  ███████╗███████╗
██╔══██║██║     ██║     ██╔══╝  ╚════██║╚════██║
██║  ██║╚██████╗╚██████╗███████╗███████║███████║
╚═╝  ╚═╝ ╚═════╝ ╚═════╝╚══════╝╚══════╝╚══════╝
██████╗ ███████╗███╗   ██╗██╗███████╗██████╗
██╔══██╗██╔════╝████╗  ██║██║██╔════╝██╔══██╗
██║  ██║█████╗  ██╔██╗ ██║██║█████╗  ██║  ██║
██║  ██║██╔══╝  ██║╚██╗██║██║██╔══╝  ██║  ██║
██████╔╝███████╗██║ ╚████║██║███████╗██████╔╝
╚═════╝ ╚══════╝╚═╝  ╚═══╝╚═╝╚══════╝╚═════╝</pre>
            <div style="margin: 25px 0; padding: 15px; background: #0a0a0a; border-left: 3px solid var(--accent-red);">
                <p style="font-family: 'JetBrains Mono', monospace; font-size: 11px; color: #888; text-align: left; margin: 0;">
                    <span style="color: var(--accent-red);">root@intercepted:</span><span style="color: var(--accent-cyan);">~#</span> sudo access --grant-permission<br>
                    <span style="color: #666;">[sudo] password for user: ********</span><br>
                    <span style="color: var(--accent-red);">Error:</span> User is not in the sudoers file.<br>
                    <span style="color: var(--accent-orange);">This incident will be reported.</span>
                </p>
            </div>
            <p style="color: #666; font-size: 11px; text-align: center;">
                "In a world of locked doors, the man with the key is king.<br>
                And you, my friend, just threw away the key."
            </p>
            <button class="accept-btn" onclick="location.reload()" style="margin-top: 20px; background: transparent; border: 1px solid var(--accent-cyan); color: var(--accent-cyan);">
                TRY AGAIN
            </button>
        </div>
    </div>
    <header>
        <button class="theme-toggle" onclick="toggleTheme()" title="Toggle Light/Dark Theme">
            <span class="icon-moon">🌙</span>
            <span class="icon-sun">☀️</span>
        </button>
        <button class="help-btn" onclick="showDependencies()" title="Check Tool Dependencies" id="depsBtn" style="margin-right: 5px;">🔧</button>
        <button class="help-btn" onclick="showHelp()" title="Help & Documentation">?</button>
        <div class="logo">
            <svg width="50" height="50" viewBox="0 0 100 100" fill="none" xmlns="http://www.w3.org/2000/svg">
                <!-- Outer hexagon -->
                <path d="M50 5 L90 27.5 L90 72.5 L50 95 L10 72.5 L10 27.5 Z" stroke="#00d4ff" stroke-width="2" fill="none"/>
                <!-- Inner signal waves -->
                <path d="M30 50 Q40 35, 50 50 Q60 65, 70 50" stroke="#00d4ff" stroke-width="2.5" fill="none" stroke-linecap="round"/>
                <path d="M35 50 Q42 40, 50 50 Q58 60, 65 50" stroke="#00ff88" stroke-width="2" fill="none" stroke-linecap="round"/>
                <path d="M40 50 Q45 45, 50 50 Q55 55, 60 50" stroke="#ffffff" stroke-width="1.5" fill="none" stroke-linecap="round"/>
                <!-- Center dot -->
                <circle cx="50" cy="50" r="3" fill="#00d4ff"/>
                <!-- Corner accents -->
                <path d="M50 12 L55 17 L50 17 Z" fill="#00d4ff"/>
                <path d="M50 88 L45 83 L50 83 Z" fill="#00d4ff"/>
            </svg>
        </div>
        <h1>INTERCEPT</h1>
        <p>Signal Intelligence // by smittix</p>
    </header>

    <div class="container">
        <div class="main-content">
            <div class="sidebar">
                <!-- Mode Tabs - Grouped -->
                <div class="mode-tabs-container">
                    <div class="tab-group">
                        <div class="tab-group-label">SDR / RF</div>
                        <div class="mode-tabs">
                            <button class="mode-tab active" onclick="switchMode('pager')"><span class="tab-icon">📟</span>Pager</button>
                            <button class="mode-tab" onclick="switchMode('sensor')"><span class="tab-icon">📡</span>433MHz</button>
                            <button class="mode-tab" onclick="switchMode('aircraft')"><span class="tab-icon">✈️</span>Aircraft</button>
                            <button class="mode-tab" onclick="switchMode('satellite')"><span class="tab-icon">🛰️</span>Satellite</button>
                        </div>
                    </div>
                    <div class="tab-group">
                        <div class="tab-group-label">Wireless</div>
                        <div class="mode-tabs">
                            <button class="mode-tab" onclick="switchMode('wifi')"><span class="tab-icon">📶</span>WiFi</button>
                            <button class="mode-tab" onclick="switchMode('bluetooth')"><span class="tab-icon">🔵</span>BT</button>
                        </div>
                    </div>
                </div>

                <div class="section" id="rtlDeviceSection">
                    <h3>RTL-SDR Device</h3>
                    <div class="form-group">
                        <select id="deviceSelect">
                            {% if devices %}
                                {% for device in devices %}
                                <option value="{{ device.index }}">{{ device.index }}: {{ device.name }}</option>
                                {% endfor %}
                            {% else %}
                                <option value="0">No devices found</option>
                            {% endif %}
                        </select>
                    </div>
                    <button class="preset-btn" onclick="refreshDevices()" style="width: 100%;">
                        Refresh Devices
                    </button>
                    <div id="toolStatusPager" class="info-text tool-status-section" style="display: grid; grid-template-columns: auto auto; gap: 4px 8px; align-items: center;">
                        <span>rtl_fm:</span><span class="tool-status {{ 'ok' if tools.rtl_fm else 'missing' }}">{{ 'OK' if tools.rtl_fm else 'Missing' }}</span>
                        <span>multimon-ng:</span><span class="tool-status {{ 'ok' if tools.multimon else 'missing' }}">{{ 'OK' if tools.multimon else 'Missing' }}</span>
                    </div>
                    <div id="toolStatusSensor" class="info-text tool-status-section" style="display: none; grid-template-columns: auto auto; gap: 4px 8px; align-items: center;">
                        <span>rtl_433:</span><span class="tool-status {{ 'ok' if tools.rtl_433 else 'missing' }}">{{ 'OK' if tools.rtl_433 else 'Missing' }}</span>
                    </div>
                    <div id="toolStatusAircraft" class="info-text tool-status-section" style="display: none; grid-template-columns: auto auto; gap: 4px 8px; align-items: center;">
                        <span>dump1090:</span><span id="dump1090StatusSidebar" class="tool-status">Checking...</span>
                        <span>rtl_adsb:</span><span id="rtlAdsbStatusSidebar" class="tool-status">Checking...</span>
                    </div>
                </div>

                <!-- PAGER MODE -->
                <div id="pagerMode" class="mode-content active">
                    <div class="section">
                        <h3>Frequency</h3>
                        <div class="form-group">
                            <label>Frequency (MHz)</label>
                            <input type="text" id="frequency" value="153.350" placeholder="e.g., 153.350">
                        </div>
                        <div class="preset-buttons" id="presetButtons">
                            <!-- Populated by JavaScript -->
                        </div>
                        <div style="margin-top: 8px; display: flex; gap: 5px;">
                            <input type="text" id="newPresetFreq" placeholder="New freq (MHz)" style="flex: 1; padding: 6px; background: #0f3460; border: 1px solid #1a1a2e; color: #fff; border-radius: 4px; font-size: 12px;">
                            <button class="preset-btn" onclick="addPreset()" style="background: #2ecc71;">Add</button>
                        </div>
                        <div style="margin-top: 5px;">
                            <button class="preset-btn" onclick="resetPresets()" style="font-size: 11px;">Reset to Defaults</button>
                        </div>
                    </div>

                    <div class="section">
                        <h3>Protocols</h3>
                        <div class="checkbox-group">
                            <label><input type="checkbox" id="proto_pocsag512" checked> POCSAG-512</label>
                            <label><input type="checkbox" id="proto_pocsag1200" checked> POCSAG-1200</label>
                            <label><input type="checkbox" id="proto_pocsag2400" checked> POCSAG-2400</label>
                            <label><input type="checkbox" id="proto_flex" checked> FLEX</label>
                        </div>
                    </div>

                    <div class="section">
                        <h3>Settings</h3>
                        <div class="form-group">
                            <label>Gain (dB, 0 = auto)</label>
                            <input type="text" id="gain" value="0" placeholder="0-49 or 0 for auto">
                        </div>
                        <div class="form-group">
                            <label>Squelch Level</label>
                            <input type="text" id="squelch" value="0" placeholder="0 = off">
                        </div>
                        <div class="form-group">
                            <label>PPM Correction</label>
                            <input type="text" id="ppm" value="0" placeholder="Frequency correction">
                        </div>
                    </div>

                    <div class="section">
                        <h3>Logging</h3>
                        <div class="checkbox-group" style="margin-bottom: 15px;">
                            <label>
                                <input type="checkbox" id="loggingEnabled" onchange="toggleLogging()">
                                Enable Logging
                            </label>
                        </div>
                        <div class="form-group">
                            <label>Log file path</label>
                            <input type="text" id="logFilePath" value="pager_messages.log" placeholder="pager_messages.log">
                        </div>
                    </div>

                    <button class="run-btn" id="startBtn" onclick="startDecoding()">
                        Start Decoding
                    </button>
                    <button class="stop-btn" id="stopBtn" onclick="stopDecoding()" style="display: none;">
                        Stop Decoding
                    </button>
                </div>

                <!-- 433MHz SENSOR MODE -->
                <div id="sensorMode" class="mode-content">
                    <div class="section">
                        <h3>Frequency</h3>
                        <div class="form-group">
                            <label>Frequency (MHz)</label>
                            <input type="text" id="sensorFrequency" value="433.92" placeholder="e.g., 433.92">
                        </div>
                        <div class="preset-buttons">
                            <button class="preset-btn" onclick="setSensorFreq('433.92')">433.92</button>
                            <button class="preset-btn" onclick="setSensorFreq('315.00')">315.00</button>
                            <button class="preset-btn" onclick="setSensorFreq('868.00')">868.00</button>
                            <button class="preset-btn" onclick="setSensorFreq('915.00')">915.00</button>
                        </div>
                    </div>

                    <div class="section">
                        <h3>Settings</h3>
                        <div class="form-group">
                            <label>Gain (dB, 0 = auto)</label>
                            <input type="text" id="sensorGain" value="0" placeholder="0-49 or 0 for auto">
                        </div>
                        <div class="form-group">
                            <label>PPM Correction</label>
                            <input type="text" id="sensorPpm" value="0" placeholder="Frequency correction">
                        </div>
                    </div>

                    <div class="section">
                        <h3>Protocols</h3>
                        <div class="info-text" style="margin-bottom: 10px;">
                            rtl_433 auto-detects 200+ device protocols including weather stations, TPMS, doorbells, and more.
                        </div>
                        <div class="checkbox-group">
                            <label>
                                <input type="checkbox" id="sensorLogging" onchange="toggleSensorLogging()">
                                Enable Logging
                            </label>
                        </div>
                    </div>

                    <button class="run-btn" id="startSensorBtn" onclick="startSensorDecoding()">
                        Start Listening
                    </button>
                    <button class="stop-btn" id="stopSensorBtn" onclick="stopSensorDecoding()" style="display: none;">
                        Stop Listening
                    </button>
                </div>

                <!-- WiFi MODE -->
                <div id="wifiMode" class="mode-content">
                    <div class="section">
                        <h3>WiFi Interface</h3>
                        <div class="form-group">
                            <select id="wifiInterfaceSelect">
                                <option value="">Detecting interfaces...</option>
                            </select>
                        </div>
                        <button class="preset-btn" onclick="refreshWifiInterfaces()" style="width: 100%;">
                            Refresh Interfaces
                        </button>
                        <div class="info-text" style="margin-top: 8px; display: grid; grid-template-columns: auto auto; gap: 4px 8px; align-items: center;" id="wifiToolStatus">
                            <span>airmon-ng:</span><span class="tool-status missing">Checking...</span>
                            <span>airodump-ng:</span><span class="tool-status missing">Checking...</span>
                        </div>
                    </div>

                    <div class="section">
                        <h3>Monitor Mode</h3>
                        <div style="display: flex; gap: 8px;">
                            <button class="preset-btn" id="monitorStartBtn" onclick="enableMonitorMode()" style="flex: 1; background: var(--accent-green); color: #000;">
                                Enable Monitor
                            </button>
                            <button class="preset-btn" id="monitorStopBtn" onclick="disableMonitorMode()" style="flex: 1; display: none;">
                                Disable Monitor
                            </button>
                        </div>
                        <div class="checkbox-group" style="margin-top: 8px;">
                            <label style="font-size: 10px;">
                                <input type="checkbox" id="killProcesses">
                                Kill interfering processes (may drop other connections)
                            </label>
                        </div>
                        <div id="monitorStatus" class="info-text" style="margin-top: 8px;">
                            Monitor mode: <span style="color: var(--accent-red);">Inactive</span>
                        </div>
                    </div>

                    <div class="section">
                        <h3>Scan Settings</h3>
                        <div class="form-group">
                            <label>Band</label>
                            <select id="wifiBand">
                                <option value="abg">All (2.4 + 5 GHz)</option>
                                <option value="bg">2.4 GHz only</option>
                                <option value="a">5 GHz only</option>
                            </select>
                        </div>
                        <div class="form-group">
                            <label>Channel (empty = hop)</label>
                            <input type="text" id="wifiChannel" placeholder="e.g., 6 or 36">
                        </div>
                    </div>

                    <div class="section">
                        <h3>Proximity Alerts</h3>
                        <div class="info-text" style="margin-bottom: 8px;">
                            Alert when specific MAC addresses appear
                        </div>
                        <div class="form-group">
                            <input type="text" id="watchMacInput" placeholder="AA:BB:CC:DD:EE:FF">
                        </div>
                        <button class="preset-btn" onclick="addWatchMac()" style="width: 100%; margin-bottom: 8px;">
                            Add to Watch List
                        </button>
                        <div id="watchList" style="max-height: 80px; overflow-y: auto; font-size: 10px; color: var(--text-dim);"></div>
                    </div>

                    <div class="section">
                        <h3>Attack Options</h3>
                        <div class="info-text" style="color: var(--accent-red); margin-bottom: 10px;">
                            ⚠ Only use on authorized networks
                        </div>
                        <div class="form-group">
                            <label>Target BSSID</label>
                            <input type="text" id="targetBssid" placeholder="AA:BB:CC:DD:EE:FF">
                        </div>
                        <div class="form-group">
                            <label>Target Client (optional)</label>
                            <input type="text" id="targetClient" placeholder="FF:FF:FF:FF:FF:FF (broadcast)">
                        </div>
                        <div class="form-group">
                            <label>Deauth Count</label>
                            <input type="text" id="deauthCount" value="5" placeholder="5">
                        </div>
                        <button class="preset-btn" onclick="sendDeauth()" style="width: 100%; border-color: var(--accent-red); color: var(--accent-red);">
                            Send Deauth
                        </button>
                    </div>

                    <!-- Handshake Capture Status Panel -->
                    <div class="section" id="captureStatusPanel" style="display: none; border: 1px solid var(--accent-orange); border-radius: 4px; padding: 10px; background: rgba(255, 165, 0, 0.1);">
                        <h3 style="color: var(--accent-orange); margin: 0 0 8px 0;">🎯 Handshake Capture</h3>
                        <div style="font-size: 11px;">
                            <div style="margin-bottom: 4px;">
                                <span style="color: var(--text-dim);">Target:</span>
                                <span id="captureTargetBssid" style="font-family: monospace;">--</span>
                            </div>
                            <div style="margin-bottom: 4px;">
                                <span style="color: var(--text-dim);">Channel:</span>
                                <span id="captureTargetChannel">--</span>
                            </div>
                            <div style="margin-bottom: 4px;">
                                <span style="color: var(--text-dim);">File:</span>
                                <span id="captureFilePath" style="font-size: 9px; word-break: break-all;">--</span>
                            </div>
                            <div style="margin-bottom: 8px;">
                                <span style="color: var(--text-dim);">Status:</span>
                                <span id="captureStatus" style="font-weight: bold;">--</span>
                            </div>
                            <div style="display: flex; gap: 8px;">
                                <button class="preset-btn" onclick="checkCaptureStatus()" style="flex: 1; font-size: 10px; padding: 4px;">
                                    Check Status
                                </button>
                                <button class="preset-btn" onclick="stopHandshakeCapture()" style="flex: 1; font-size: 10px; padding: 4px; border-color: var(--accent-red); color: var(--accent-red);">
                                    Stop Capture
                                </button>
                            </div>
                        </div>
                    </div>

                    <!-- PMKID Capture Panel -->
                    <div class="section" id="pmkidPanel" style="display: none; border: 1px solid #9933ff; border-radius: 4px; padding: 10px; background: rgba(153, 51, 255, 0.1);">
                        <h3 style="color: #9933ff; margin: 0 0 8px 0;">🔐 PMKID Capture</h3>
                        <div style="font-size: 11px;">
                            <div style="margin-bottom: 4px;">
                                <span style="color: var(--text-dim);">Target:</span>
                                <span id="pmkidTargetBssid" style="font-family: monospace;">--</span>
                            </div>
                            <div style="margin-bottom: 4px;">
                                <span style="color: var(--text-dim);">Status:</span>
                                <span id="pmkidStatus" style="font-weight: bold;">--</span>
                            </div>
                            <div style="display: flex; gap: 8px;">
                                <button class="preset-btn" onclick="stopPmkidCapture()" style="flex: 1; font-size: 10px; padding: 4px; border-color: var(--accent-red); color: var(--accent-red);">
                                    Stop
                                </button>
                            </div>
                        </div>
                    </div>

                    <!-- Beacon Flood Alert Panel -->
                    <div id="beaconFloodAlert" class="beacon-flood-alert" style="display: none;">
                        <h4 style="color: #ff4444; margin: 0 0 8px 0;">⚠️ BEACON FLOOD DETECTED</h4>
                        <div style="font-size: 11px;">
                            <div id="beaconFloodDetails">Multiple beacon frames detected from same channel</div>
                            <div style="margin-top: 8px;">
                                <span style="color: var(--text-dim);">Networks/sec:</span>
                                <span id="beaconFloodRate" style="font-weight: bold; color: #ff4444;">--</span>
                            </div>
                        </div>
                    </div>

                    <button class="run-btn" id="startWifiBtn" onclick="startWifiScan()">
                        Start Scanning
                    </button>
                    <button class="stop-btn" id="stopWifiBtn" onclick="stopWifiScan()" style="display: none;">
                        Stop Scanning
                    </button>
                </div>

                <!-- BLUETOOTH MODE -->
                <div id="bluetoothMode" class="mode-content">
                    <div class="section">
                        <h3>Bluetooth Interface</h3>
                        <div class="form-group">
                            <select id="btInterfaceSelect">
                                <option value="">Detecting interfaces...</option>
                            </select>
                        </div>
                        <button class="preset-btn" onclick="refreshBtInterfaces()" style="width: 100%;">
                            Refresh Interfaces
                        </button>
                        <div class="info-text" style="margin-top: 8px; display: grid; grid-template-columns: auto auto; gap: 4px 8px; align-items: center;" id="btToolStatus">
                            <span>hcitool:</span><span class="tool-status missing">Checking...</span>
                            <span>bluetoothctl:</span><span class="tool-status missing">Checking...</span>
                        </div>
                    </div>

                    <div class="section">
                        <h3>Scan Mode</h3>
                        <div class="checkbox-group" style="margin-bottom: 10px;">
                            <label><input type="radio" name="btScanMode" value="bluetoothctl" checked> bluetoothctl (Recommended)</label>
                            <label><input type="radio" name="btScanMode" value="hcitool"> hcitool (Legacy)</label>
                        </div>
                        <div class="form-group">
                            <label>Scan Duration (sec)</label>
                            <input type="text" id="btScanDuration" value="30" placeholder="30">
                        </div>
                        <div class="checkbox-group">
                            <label>
                                <input type="checkbox" id="btScanBLE" checked>
                                Scan BLE Devices
                            </label>
                            <label>
                                <input type="checkbox" id="btScanClassic" checked>
                                Scan Classic BT
                            </label>
                            <label>
                                <input type="checkbox" id="btDetectBeacons" checked>
                                Detect Trackers (AirTag/Tile)
                            </label>
                        </div>
                    </div>

                    <div class="section">
                        <h3>Device Actions</h3>
                        <div class="form-group">
                            <label>Target MAC</label>
                            <input type="text" id="btTargetMac" placeholder="AA:BB:CC:DD:EE:FF">
                        </div>
                        <button class="preset-btn" onclick="btEnumServices()" style="width: 100%;">
                            Enumerate Services
                        </button>
                    </div>

                    <!-- Tracker Following Alert -->
                    <div id="trackerFollowingAlert" class="tracker-following-alert" style="display: none;">
                        <!-- Populated by JavaScript -->
                    </div>

                    <button class="run-btn" id="startBtBtn" onclick="startBtScan()">
                        Start Scanning
                    </button>
                    <button class="stop-btn" id="stopBtBtn" onclick="stopBtScan()" style="display: none;">
                        Stop Scanning
                    </button>
                    <button class="preset-btn" onclick="resetBtAdapter()" style="margin-top: 5px; width: 100%;">
                        Reset Adapter
                    </button>
                </div>

                <!-- AIRCRAFT MODE (ADS-B) -->
                <div id="aircraftMode" class="mode-content">
                    <div class="section">
                        <h3>ADS-B Receiver</h3>
                        <div class="form-group">
                            <label>Frequency</label>
                            <input type="text" id="adsbFrequency" value="1090" readonly style="opacity: 0.7;">
                            <div class="info-text">Fixed at 1090 MHz</div>
                        </div>
                        <div class="form-group">
                            <label>Gain (dB)</label>
                            <input type="text" id="adsbGain" value="40" placeholder="40">
                        </div>
                        <div class="checkbox-group">
                            <label>
                                <input type="checkbox" id="adsbEnableMap" checked>
                                Show Radar Display
                            </label>
                        </div>
                    </div>

                    <div class="section">
                        <h3>Display Settings</h3>
                        <div class="form-group">
                            <label>Range (nm)</label>
                            <select id="adsbRange">
                                <option value="50">50 nm</option>
                                <option value="100" selected>100 nm</option>
                                <option value="200">200 nm</option>
                                <option value="500">500 nm</option>
                            </select>
                        </div>
                        <div class="checkbox-group">
                            <label>
                                <input type="checkbox" id="adsbShowLabels" checked>
                                Show Callsigns
                            </label>
                            <label>
                                <input type="checkbox" id="adsbShowAltitude" checked>
                                Show Altitude
                            </label>
                            <label>
                                <input type="checkbox" id="adsbShowTrails">
                                Show Flight Trails
                            </label>
                            <label>
                                <input type="checkbox" id="adsbEnableClustering" onchange="toggleAircraftClustering()">
                                Cluster Markers
                            </label>
                        </div>
                    </div>

                    <div class="info-text" style="margin-top: 8px; display: grid; grid-template-columns: auto auto; gap: 4px 8px; align-items: center;" id="adsbToolStatus">
                        <span>dump1090:</span><span class="tool-status" id="dump1090Status">Checking...</span>
                        <span>rtl_adsb:</span><span class="tool-status" id="rtlAdsbStatus">Checking...</span>
                    </div>

                    <button class="run-btn" id="startAdsbBtn" onclick="startAdsbScan()">
                        Start Tracking
                    </button>
                    <button class="stop-btn" id="stopAdsbBtn" onclick="stopAdsbScan()" style="display: none;">
                        Stop Tracking
                    </button>
                </div>

                <!-- SATELLITE MODE -->
                <div id="satelliteMode" class="mode-content">
                    <div class="satellite-tabs">
                        <button class="satellite-tab active" onclick="switchSatelliteTab('predictor')">🛰️ Pass Predictor</button>
                        <button class="satellite-tab" onclick="switchSatelliteTab('iridium')">📡 Iridium</button>
                    </div>

                    <!-- Pass Predictor Sub-tab -->
                    <div id="predictorTab" class="satellite-content active">
                        <div class="section">
                            <h3>Observer Location</h3>
                            <div class="form-group">
                                <label>Latitude</label>
                                <input type="text" id="obsLat" value="51.5074" placeholder="51.5074">
                            </div>
                            <div class="form-group">
                                <label>Longitude</label>
                                <input type="text" id="obsLon" value="-0.1278" placeholder="-0.1278">
                            </div>
                            <button class="preset-btn" onclick="getLocation()" style="width: 100%;">
                                📍 Use My Location
                            </button>
                        </div>

                        <div class="section">
                            <h3>Satellites to Track</h3>
                            <div id="satelliteList" class="satellite-list">
                                <!-- Dynamically populated -->
                            </div>
                            <div style="margin-top: 10px; display: flex; gap: 5px;">
                                <button class="preset-btn" onclick="showAddSatelliteModal()" style="flex: 1;">
                                    ➕ Add Satellite
                                </button>
                                <button class="preset-btn" onclick="fetchCelestrak()" style="flex: 1;">
                                    🌐 Celestrak
                                </button>
                            </div>
                        </div>

                        <div class="section">
                            <h3>Prediction Settings</h3>
                            <div class="form-group">
                                <label>Time Range</label>
                                <select id="predictionHours">
                                    <option value="12">12 hours</option>
                                    <option value="24" selected>24 hours</option>
                                    <option value="48">48 hours</option>
                                    <option value="72">72 hours</option>
                                </select>
                            </div>
                            <div class="form-group">
                                <label>Min Elevation</label>
                                <select id="minElevation">
                                    <option value="0">0° (All passes)</option>
                                    <option value="10" selected>10° (Good)</option>
                                    <option value="30">30° (Best)</option>
                                    <option value="45">45° (Overhead)</option>
                                </select>
                            </div>
                        </div>

                        <button class="run-btn" onclick="calculatePasses()">
                            Calculate Passes
                        </button>
                        <button class="preset-btn" onclick="updateTLE()" style="width: 100%; margin-top: 5px;">
                            🔄 Update TLE Data
                        </button>
                    </div>

                    <!-- Iridium Sub-tab -->
                    <div id="iridiumTab" class="satellite-content">
                        <div class="iridium-warning">
                            <strong>⚠️ Hardware Required</strong>
                            Iridium burst detection requires:<br>
                            • RTL-SDR dongle<br>
                            • L-band patch antenna (1616-1626 MHz)<br>
                            • Low Noise Amplifier (LNA)<br>
                            • Clear view of sky
                        </div>

                        <div class="section">
                            <h3>Iridium Settings</h3>
                            <div class="form-group">
                                <label>Center Frequency (MHz)</label>
                                <input type="text" id="iridiumFreq" value="1626.0" placeholder="1626.0">
                            </div>
                            <div class="form-group">
                                <label>Gain (dB)</label>
                                <input type="text" id="iridiumGain" value="40" placeholder="40">
                            </div>
                            <div class="form-group">
                                <label>Sample Rate</label>
                                <select id="iridiumSampleRate">
                                    <option value="2.4e6">2.4 MSPS</option>
                                    <option value="2.048e6" selected>2.048 MSPS</option>
                                </select>
                            </div>
                        </div>

                        <div class="info-text" style="margin-bottom: 10px;" id="iridiumToolStatus">
                            <span>iridium-extractor:</span> <span class="tool-status" id="iridiumExtractorStatus">Checking...</span>
                        </div>

                        <button class="run-btn" id="startIridiumBtn" onclick="startIridiumCapture()">
                            Start Capture
                        </button>
                        <button class="stop-btn" id="stopIridiumBtn" onclick="stopIridiumCapture()" style="display: none;">
                            Stop Capture
                        </button>
                    </div>
                </div>

                <button class="preset-btn" onclick="killAll()" style="width: 100%; margin-top: 10px; border-color: #ff3366; color: #ff3366;">
                    Kill All Processes
                </button>
            </div>

            <div class="output-panel">
                <div class="output-header">
                    <h3 id="outputTitle">Pager Decoder</h3>
                    <div class="header-controls">
                        <div id="signalMeter" class="signal-meter" title="Signal Activity">
                            <div class="signal-bar"></div>
                            <div class="signal-bar"></div>
                            <div class="signal-bar"></div>
                            <div class="signal-bar"></div>
                            <div class="signal-bar"></div>
                        </div>
                        <div class="stats" id="pagerStats">
                            <div title="Total Messages">📨 <span id="msgCount">0</span></div>
                            <div title="POCSAG Messages">📟 <span id="pocsagCount">0</span></div>
                            <div title="FLEX Messages">📠 <span id="flexCount">0</span></div>
                        </div>
                        <div class="stats" id="sensorStats" style="display: none;">
                            <div title="Unique Sensors">🌡️ <span id="sensorCount">0</span></div>
                            <div title="Device Types">📊 <span id="deviceCount">0</span></div>
                        </div>
                        <div class="stats" id="wifiStats" style="display: none;">
                            <div title="Access Points">📡 <span id="apCount">0</span></div>
                            <div title="Connected Clients">👤 <span id="clientCount">0</span></div>
                            <div title="Captured Handshakes" style="color: var(--accent-green);">🤝 <span id="handshakeCount">0</span></div>
                            <div style="color: var(--accent-orange); cursor: pointer;" onclick="showDroneDetails()" title="Click: Drone details">🚁 <span id="droneCount">0</span></div>
                            <div style="color: var(--accent-red); cursor: pointer;" onclick="showRogueApDetails()" title="Click: Rogue AP details">⚠️ <span id="rogueApCount">0</span></div>
                        </div>
                        <div class="stats" id="btStats" style="display: none;">
                            <div title="Bluetooth Devices">🔵 <span id="btDeviceCount">0</span></div>
                            <div title="BLE Beacons">📍 <span id="btBeaconCount">0</span></div>
                        </div>
                        <div class="stats" id="aircraftStats" style="display: none;">
                            <div title="Aircraft Tracked">✈️ <span id="aircraftCount">0</span></div>
                            <div title="Messages Received">📨 <span id="adsbMsgCount">0</span></div>
                            <div title="Unique ICAO Codes">🔢 <span id="icaoCount">0</span></div>
                        </div>
                        <div class="stats" id="satelliteStats" style="display: none;">
                            <div title="Upcoming Passes">🛰️ <span id="passCount">0</span></div>
                            <div title="Iridium Bursts">📡 <span id="burstCount">0</span></div>
                        </div>
                    </div>
                </div>

                <!-- WiFi Visualizations (shown only in WiFi mode) -->
                <div class="wifi-visuals" id="wifiVisuals" style="display: none;">
                    <div class="wifi-visual-panel">
                        <h5>Network Radar</h5>
                        <div class="radar-container">
                            <canvas id="radarCanvas" width="150" height="150"></canvas>
                        </div>
                    </div>
                    <div class="wifi-visual-panel">
                        <h5>Channel Utilization (2.4 GHz)</h5>
                        <div class="channel-graph" id="channelGraph">
                            <div class="channel-bar-wrapper"><div class="channel-bar" style="height: 2px;"></div><span class="channel-label">1</span></div>
                            <div class="channel-bar-wrapper"><div class="channel-bar" style="height: 2px;"></div><span class="channel-label">2</span></div>
                            <div class="channel-bar-wrapper"><div class="channel-bar" style="height: 2px;"></div><span class="channel-label">3</span></div>
                            <div class="channel-bar-wrapper"><div class="channel-bar" style="height: 2px;"></div><span class="channel-label">4</span></div>
                            <div class="channel-bar-wrapper"><div class="channel-bar" style="height: 2px;"></div><span class="channel-label">5</span></div>
                            <div class="channel-bar-wrapper"><div class="channel-bar" style="height: 2px;"></div><span class="channel-label">6</span></div>
                            <div class="channel-bar-wrapper"><div class="channel-bar" style="height: 2px;"></div><span class="channel-label">7</span></div>
                            <div class="channel-bar-wrapper"><div class="channel-bar" style="height: 2px;"></div><span class="channel-label">8</span></div>
                            <div class="channel-bar-wrapper"><div class="channel-bar" style="height: 2px;"></div><span class="channel-label">9</span></div>
                            <div class="channel-bar-wrapper"><div class="channel-bar" style="height: 2px;"></div><span class="channel-label">10</span></div>
                            <div class="channel-bar-wrapper"><div class="channel-bar" style="height: 2px;"></div><span class="channel-label">11</span></div>
                            <div class="channel-bar-wrapper"><div class="channel-bar" style="height: 2px;"></div><span class="channel-label">12</span></div>
                            <div class="channel-bar-wrapper"><div class="channel-bar" style="height: 2px;"></div><span class="channel-label">13</span></div>
                        </div>
                    </div>
                    <div class="wifi-visual-panel">
                        <h5>Channel Utilization (5 GHz)</h5>
                        <div class="channel-graph" id="channelGraph5g" style="font-size: 7px;">
                            <div class="channel-bar-wrapper"><div class="channel-bar" style="height: 2px;"></div><span class="channel-label">36</span></div>
                            <div class="channel-bar-wrapper"><div class="channel-bar" style="height: 2px;"></div><span class="channel-label">40</span></div>
                            <div class="channel-bar-wrapper"><div class="channel-bar" style="height: 2px;"></div><span class="channel-label">44</span></div>
                            <div class="channel-bar-wrapper"><div class="channel-bar" style="height: 2px;"></div><span class="channel-label">48</span></div>
                            <div class="channel-bar-wrapper"><div class="channel-bar" style="height: 2px;"></div><span class="channel-label">52</span></div>
                            <div class="channel-bar-wrapper"><div class="channel-bar" style="height: 2px;"></div><span class="channel-label">56</span></div>
                            <div class="channel-bar-wrapper"><div class="channel-bar" style="height: 2px;"></div><span class="channel-label">60</span></div>
                            <div class="channel-bar-wrapper"><div class="channel-bar" style="height: 2px;"></div><span class="channel-label">64</span></div>
                            <div class="channel-bar-wrapper"><div class="channel-bar" style="height: 2px;"></div><span class="channel-label">100</span></div>
                            <div class="channel-bar-wrapper"><div class="channel-bar" style="height: 2px;"></div><span class="channel-label">149</span></div>
                            <div class="channel-bar-wrapper"><div class="channel-bar" style="height: 2px;"></div><span class="channel-label">153</span></div>
                            <div class="channel-bar-wrapper"><div class="channel-bar" style="height: 2px;"></div><span class="channel-label">157</span></div>
                            <div class="channel-bar-wrapper"><div class="channel-bar" style="height: 2px;"></div><span class="channel-label">161</span></div>
                            <div class="channel-bar-wrapper"><div class="channel-bar" style="height: 2px;"></div><span class="channel-label">165</span></div>
                        </div>
                    </div>
                    <div class="wifi-visual-panel">
                        <h5>Security Overview</h5>
                        <div class="security-container">
                            <div class="security-donut">
                                <canvas id="securityCanvas" width="80" height="80"></canvas>
                            </div>
                            <div class="security-legend">
                                <div class="security-legend-item"><div class="security-legend-dot wpa3"></div>WPA3: <span id="wpa3Count">0</span></div>
                                <div class="security-legend-item"><div class="security-legend-dot wpa2"></div>WPA2: <span id="wpa2Count">0</span></div>
                                <div class="security-legend-item"><div class="security-legend-dot wep"></div>WEP: <span id="wepCount">0</span></div>
                                <div class="security-legend-item"><div class="security-legend-dot open"></div>Open: <span id="openCount">0</span></div>
                            </div>
                        </div>
                    </div>
                    <div class="wifi-visual-panel">
                        <h5>Target Signal</h5>
                        <div class="signal-strength-display">
                            <div class="target-ssid" id="targetSsid">No target selected</div>
                            <div class="signal-value" id="signalValue">-- dBm</div>
                            <div class="signal-bars-large">
                                <div class="signal-bar-large"></div>
                                <div class="signal-bar-large"></div>
                                <div class="signal-bar-large"></div>
                                <div class="signal-bar-large"></div>
                                <div class="signal-bar-large"></div>
                            </div>
                        </div>
                    </div>
                    <!-- Signal Strength History Graph -->
                    <div class="wifi-visual-panel signal-graph-panel" id="signalGraphPanel" style="grid-column: span 2;">
                        <div class="signal-graph-header">
                            <h4>📈 Signal History</h4>
                            <span class="signal-graph-device" id="signalGraphDevice">Click a device to track</span>
                        </div>
                        <canvas id="signalGraph"></canvas>
                    </div>
                    <!-- Network Relationship Graph -->
                    <div class="wifi-visual-panel network-graph-container" style="grid-column: span 2;">
                        <h4>🕸️ Network Topology</h4>
                        <canvas id="networkGraph"></canvas>
                        <div class="network-graph-legend">
                            <div class="legend-item"><div class="legend-dot ap"></div>Access Point</div>
                            <div class="legend-item"><div class="legend-dot client"></div>Client</div>
                            <div class="legend-item"><div class="legend-dot drone"></div>Drone</div>
                        </div>
                    </div>
                    <!-- Channel Recommendation -->
                    <div class="wifi-visual-panel channel-recommendation" id="channelRecommendation">
                        <h4>💡 Channel Recommendation</h4>
                        <div class="rec-text">
                            <strong>2.4 GHz:</strong> Use channel <span class="rec-channel" id="rec24Channel">--</span>
                            <span id="rec24Reason" style="font-size: 10px; color: var(--text-dim);"></span>
                        </div>
                        <div class="rec-text" style="margin-top: 5px;">
                            <strong>5 GHz:</strong> Use channel <span class="rec-channel" id="rec5Channel">--</span>
                            <span id="rec5Reason" style="font-size: 10px; color: var(--text-dim);"></span>
                        </div>
                    </div>
                    <!-- Device Correlation -->
                    <div class="wifi-visual-panel" id="correlationPanel">
                        <h5>🔗 Device Correlation</h5>
                        <div id="correlationList" style="font-size: 11px; max-height: 100px; overflow-y: auto;">
                            <div style="color: var(--text-dim);">Analyzing WiFi/BT device patterns...</div>
                        </div>
                    </div>
                    <!-- Hidden SSID Revealer -->
                    <div class="wifi-visual-panel" id="hiddenSsidPanel">
                        <h5>👁️ Hidden SSIDs Revealed</h5>
                        <div id="hiddenSsidList" style="font-size: 11px; max-height: 100px; overflow-y: auto;">
                            <div style="color: var(--text-dim);">Monitoring probe requests...</div>
                        </div>
                    </div>
                    <!-- Client Probe Analysis -->
                    <div class="wifi-visual-panel" id="probeAnalysisPanel" style="grid-column: span 2;">
                        <h5>📡 Client Probe Analysis</h5>
                        <div style="display: flex; gap: 10px; margin-bottom: 8px; font-size: 10px;">
                            <span>Clients: <strong id="probeClientCount">0</strong></span>
                            <span>Unique SSIDs: <strong id="probeSSIDCount">0</strong></span>
                            <span>Privacy Leaks: <strong id="probePrivacyCount" style="color: var(--accent-orange);">0</strong></span>
                        </div>
                        <div id="probeAnalysisList" style="font-size: 11px; max-height: 200px; overflow-y: auto;">
                            <div style="color: var(--text-dim);">Waiting for client probe requests...</div>
                        </div>
                    </div>
                </div>

                <!-- Bluetooth Visualizations -->
                <div class="wifi-visuals" id="btVisuals" style="display: none;">
                    <div class="wifi-visual-panel" style="grid-column: span 2;">
                        <h5>Bluetooth Proximity Radar</h5>
                        <div class="radar-container">
                            <canvas id="btRadarCanvas" width="150" height="150"></canvas>
                        </div>
                    </div>
                </div>

                <!-- Aircraft Visualizations - Leaflet Map -->
                <div class="wifi-visuals" id="aircraftVisuals" style="display: none;">
                    <div class="wifi-visual-panel" style="grid-column: span 2;">
                        <h5 style="color: var(--accent-cyan); text-shadow: 0 0 10px var(--accent-cyan);">ADS-B AIRCRAFT TRACKING</h5>
                        <div class="aircraft-map-container">
                            <div class="map-header">
                                <span id="radarTime">--:--:--</span>
                                <span id="radarStatus">TRACKING</span>
                            </div>
                            <div id="aircraftMap"></div>
                            <div class="map-footer">
                                <span>AIRCRAFT: <span id="aircraftCount">0</span></span>
                                <span>CENTER: <span id="mapCenter">--</span></span>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Satellite Visualizations -->
                <div id="satelliteVisuals" style="display: none;">
                    <div class="pass-predictor">
                        <!-- Cell 1: Polar Plot (Top Left) -->
                        <div class="polar-plot-container">
                            <div class="polar-plot-header">
                                <span class="polar-plot-title">Sky View</span>
                                <button class="popout-btn" onclick="popoutSatellite()">⛶ Pop Out</button>
                            </div>
                            <div class="polar-plot">
                                <canvas id="polarPlotCanvas"></canvas>
                            </div>
                            <div style="text-align: center; margin-top: 10px; font-size: 10px; color: var(--text-secondary);">
                                <span style="color: var(--accent-cyan);">N</span> = North |
                                Center = Overhead (90°) |
                                Edge = Horizon (0°)
                            </div>
                        </div>

                        <!-- Cell 2: Ground Track Map (Top Right) -->
                        <div class="ground-track-cell">
                            <div class="ground-track-header">
                                <span class="ground-track-title">🌍 Ground Track</span>
                                <label style="font-size: 11px; display: flex; align-items: center; gap: 5px;">
                                    <input type="checkbox" id="showGroundTrack" checked onchange="toggleGroundTrack()">
                                    Show Track
                                </label>
                            </div>
                            <div id="groundTrackMap"></div>
                            <div style="text-align: center; margin-top: 8px; font-size: 10px; color: var(--text-secondary);">
                                <span style="color: #666;">---</span> Past |
                                <span style="color: #ffff00;">●</span> Current |
                                <span style="color: #00ff00;">―</span> Future |
                                <span style="color: #ff6600;">◉</span> Observer
                            </div>
                        </div>

                        <!-- Cell 3: Countdown (Bottom Left) -->
                        <div class="countdown-cell">
                            <div class="pass-list-header">
                                <span>Next Pass Countdown</span>
                            </div>
                            <div id="satelliteCountdown" class="satellite-countdown">
                                <div class="countdown-satellite-name" id="countdownSatName">--</div>
                                <div class="countdown-grid">
                                    <div class="countdown-block">
                                        <div class="countdown-label">Next Pass In</div>
                                        <div class="countdown-value" id="countdownToPass">--:--:--</div>
                                        <div class="countdown-sublabel" id="countdownPassTime">--</div>
                                    </div>
                                    <div class="countdown-block">
                                        <div class="countdown-label">Visibility</div>
                                        <div class="countdown-value" id="countdownVisibility">--:--</div>
                                        <div class="countdown-sublabel" id="countdownVisLabel">Duration</div>
                                    </div>
                                    <div class="countdown-block">
                                        <div class="countdown-label">Max Elevation</div>
                                        <div class="countdown-value" id="countdownMaxEl">--°</div>
                                        <div class="countdown-sublabel" id="countdownDirection">--</div>
                                    </div>
                                </div>
                                <div class="countdown-status" id="countdownStatus">Calculate passes to see countdown</div>
                            </div>
                        </div>

                        <!-- Cell 4: Pass List (Bottom Right) -->
                        <div class="pass-list-cell">
                            <div class="pass-list-header">
                                <span>Upcoming Passes</span>
                                <span id="passListCount">0 passes</span>
                            </div>
                            <div id="passList">
                                <div style="color: #666; text-align: center; padding: 30px; font-size: 11px;">
                                    Click "Calculate Passes" to predict satellite passes for your location.
                                </div>
                            </div>
                        </div>
                    </div>

                    <!-- Iridium Burst Log -->
                    <div id="iridiumBurstLog" style="display: none; margin-top: 15px;">
                        <div class="pass-list-container">
                            <div class="pass-list-header">
                                <span>Iridium Burst Log</span>
                                <button class="preset-btn" onclick="clearIridiumLog()" style="padding: 2px 8px; font-size: 10px;">Clear</button>
                            </div>
                            <div id="burstList">
                                <div style="color: #666; text-align: center; padding: 30px; font-size: 11px;">
                                    Iridium bursts will appear here when detected.
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Satellite Popout Container -->
                <div id="satellitePopout" class="popout-container">
                    <div class="popout-header">
                        <span class="popout-title">🛰️ Satellite Pass Predictor</span>
                        <button class="popout-close" onclick="closeSatellitePopout()">✕ Close</button>
                    </div>
                    <div class="popout-body">
                        <div class="pass-predictor" style="height: 100%;">
                            <div class="polar-plot-container" style="height: 100%;">
                                <div class="polar-plot-header">
                                    <span class="polar-plot-title">Sky View - Full Screen</span>
                                </div>
                                <div style="height: calc(100% - 80px);">
                                    <canvas id="polarPlotCanvasPopout" style="width: 100%; height: 100%;"></canvas>
                                </div>
                                <div style="text-align: center; margin-top: 10px; font-size: 12px; color: var(--text-secondary);">
                                    <span style="color: var(--accent-cyan);">N</span> = North |
                                    Center = Overhead (90°) |
                                    Edge = Horizon (0°)
                                </div>
                            </div>
                            <div class="pass-list-container" style="height: 100%; max-height: none;">
                                <div class="pass-list-header">
                                    <span>Upcoming Passes</span>
                                </div>
                                <!-- Countdown Block for Popout -->
                                <div id="satelliteCountdownPopout" class="satellite-countdown" style="display: none;">
                                    <div class="countdown-satellite-name" id="countdownSatNamePopout">--</div>
                                    <div class="countdown-grid">
                                        <div class="countdown-block">
                                            <div class="countdown-label">Next Pass In</div>
                                            <div class="countdown-value" id="countdownToPassPopout">--:--:--</div>
                                            <div class="countdown-sublabel" id="countdownPassTimePopout">--</div>
                                        </div>
                                        <div class="countdown-block">
                                            <div class="countdown-label">Visibility</div>
                                            <div class="countdown-value" id="countdownVisibilityPopout">--:--</div>
                                            <div class="countdown-sublabel" id="countdownVisLabelPopout">Duration</div>
                                        </div>
                                        <div class="countdown-block">
                                            <div class="countdown-label">Max Elevation</div>
                                            <div class="countdown-value" id="countdownMaxElPopout">--°</div>
                                            <div class="countdown-sublabel" id="countdownDirectionPopout">--</div>
                                        </div>
                                    </div>
                                    <div class="countdown-status" id="countdownStatusPopout">Waiting for pass data...</div>
                                </div>
                                <div id="passListPopout" style="height: calc(100% - 40px); overflow-y: auto;">
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Satellite Add Modal -->
                <div id="satModal" class="sat-modal">
                    <div class="sat-modal-content">
                        <div class="sat-modal-header">
                            <h3>🛰️ Add Satellites</h3>
                            <button class="sat-modal-close" onclick="closeSatModal()">&times;</button>
                        </div>
                        <div class="sat-modal-tabs">
                            <button class="sat-modal-tab active" onclick="switchSatModalTab('tle')">Paste TLE</button>
                            <button class="sat-modal-tab" onclick="switchSatModalTab('celestrak')">Celestrak</button>
                        </div>
                        <div id="tleSection" class="sat-modal-section active">
                            <p style="font-size: 11px; color: var(--text-secondary); margin-bottom: 10px;">
                                Paste TLE data (3 lines per satellite: name, line 1, line 2)
                            </p>
                            <textarea id="tleInput" class="tle-textarea" placeholder="SATELLITE NAME&#10;1 NNNNN...&#10;2 NNNNN..."></textarea>
                            <button class="run-btn" onclick="addFromTLE()" style="margin-top: 10px;">
                                Add Satellites from TLE
                            </button>
                        </div>
                        <div id="celestrakSection" class="sat-modal-section">
                            <p style="font-size: 11px; color: var(--text-secondary); margin-bottom: 10px;">
                                Select a category to fetch satellites from Celestrak
                            </p>
                            <div class="celestrak-categories">
                                <button class="celestrak-cat" onclick="fetchCelestrakCategory('stations')">🚀 Space Stations</button>
                                <button class="celestrak-cat" onclick="fetchCelestrakCategory('visual')">👁️ Brightest</button>
                                <button class="celestrak-cat" onclick="fetchCelestrakCategory('weather')">🌤️ Weather</button>
                                <button class="celestrak-cat" onclick="fetchCelestrakCategory('noaa')">📡 NOAA</button>
                                <button class="celestrak-cat" onclick="fetchCelestrakCategory('amateur')">📻 Amateur Radio</button>
                                <button class="celestrak-cat" onclick="fetchCelestrakCategory('starlink')">⭐ Starlink</button>
                                <button class="celestrak-cat" onclick="fetchCelestrakCategory('gps-ops')">🛰️ GPS</button>
                                <button class="celestrak-cat" onclick="fetchCelestrakCategory('iridium')">📱 Iridium</button>
                            </div>
                            <div id="celestrakStatus" style="margin-top: 10px; font-size: 11px; color: var(--text-secondary);"></div>
                        </div>
                    </div>
                </div>

                <!-- Device Intelligence Dashboard (above waterfall for prominence) -->
                <div class="recon-panel" id="reconPanel">
                    <div class="recon-header" onclick="toggleReconCollapse()" style="cursor: pointer;">
                        <h4><span id="reconCollapseIcon">▼</span> Device Intelligence</h4>
                        <div class="recon-stats">
                            <div>TRACKED: <span id="trackedCount">0</span></div>
                            <div>NEW: <span id="newDeviceCount">0</span></div>
                            <div>ANOMALIES: <span id="anomalyCount">0</span></div>
                        </div>
                    </div>
                    <div class="recon-content" id="reconContent">
                        <div style="color: #444; text-align: center; padding: 20px; font-size: 11px;">
                            Device intelligence data will appear here as signals are intercepted.
                        </div>
                    </div>
                </div>

                <div class="waterfall-container">
                    <canvas id="waterfallCanvas" width="800" height="60"></canvas>
                </div>

                <div class="output-content" id="output">
                    <div class="placeholder" style="color: #888; text-align: center; padding: 50px;">
                        Configure settings and click "Start Decoding" to begin.
                    </div>
                </div>

                <div class="status-bar">
                    <div class="status-indicator">
                        <div class="status-dot" id="statusDot"></div>
                        <span id="statusText">Idle</span>
                    </div>
                    <div class="status-controls">
                        <button id="reconBtn" class="recon-toggle" onclick="toggleRecon()">RECON</button>
                        <button id="muteBtn" class="control-btn" onclick="toggleMute()">🔊 MUTE</button>
                        <button id="autoScrollBtn" class="control-btn" onclick="toggleAutoScroll()">⬇ AUTO-SCROLL ON</button>
                        <button class="control-btn" onclick="exportCSV()">📄 CSV</button>
                        <button class="control-btn" onclick="exportJSON()">📋 JSON</button>
                        <button class="control-btn" onclick="exportDeviceDB()" title="Export Device Intelligence">🔍 INTEL</button>
                        <button class="clear-btn" onclick="clearMessages()">Clear</button>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
        // Disclaimer handling
        function checkDisclaimer() {
            const accepted = localStorage.getItem('disclaimerAccepted');
            if (accepted === 'true') {
                document.getElementById('disclaimerModal').classList.add('disclaimer-hidden');
            }
        }

        function acceptDisclaimer() {
            localStorage.setItem('disclaimerAccepted', 'true');
            document.getElementById('disclaimerModal').classList.add('disclaimer-hidden');
        }

        function declineDisclaimer() {
            document.getElementById('disclaimerModal').classList.add('disclaimer-hidden');
            document.getElementById('rejectionPage').classList.remove('disclaimer-hidden');
        }

        // Check disclaimer on load
        checkDisclaimer();

        let eventSource = null;
        let isRunning = false;
        let isSensorRunning = false;
        let isAdsbRunning = false;
        let isIridiumRunning = false;
        let currentMode = 'pager';
        let msgCount = 0;
        let pocsagCount = 0;
        let flexCount = 0;
        let sensorCount = 0;
        let deviceList = {{ devices | tojson | safe }};

        // Aircraft (ADS-B) state
        let adsbAircraft = {};
        let adsbMsgCount = 0;
        let adsbEventSource = null;
        let aircraftTrails = {};  // ICAO -> array of positions
        let activeSquawkAlerts = {};  // Active emergency squawk alerts

        // Emergency squawk codes
        const SQUAWK_CODES = {
            '7500': { type: 'hijack', name: 'HIJACK', color: '#ff0000', description: 'Aircraft being hijacked' },
            '7600': { type: 'radio', name: 'RADIO FAILURE', color: '#ff6600', description: 'Radio communications failure' },
            '7700': { type: 'mayday', name: 'EMERGENCY', color: '#ff0000', description: 'General emergency' }
        };

        // Military ICAO hex ranges (partial list - many countries)
        const MILITARY_RANGES = [
            { start: 0xADF7C0, end: 0xADFFFFF, country: 'US' },  // US Military
            { start: 0xAE0000, end: 0xAEFFFF, country: 'US' },   // US Military
            { start: 0x3F0000, end: 0x3FFFFF, country: 'FR' },   // France Military
            { start: 0x400000, end: 0x43FFFF, country: 'UK' },   // UK Military
            { start: 0x43C000, end: 0x43CFFF, country: 'UK' },   // UK Military
            { start: 0x4B0000, end: 0x4B7FFF, country: 'DE' },   // Germany Military
            { start: 0x501C00, end: 0x501FFF, country: 'NATO' }, // NATO
        ];

        // Military callsign prefixes
        const MILITARY_PREFIXES = [
            'REACH', 'JAKE', 'DOOM', 'IRON', 'HAWK', 'VIPER', 'COBRA', 'THUNDER',
            'SHADOW', 'NIGHT', 'STEEL', 'GRIM', 'REAPER', 'BLADE', 'STRIKE',
            'RCH', 'CNV', 'MCH', 'EVAC', 'TOPCAT', 'ASCOT', 'RRR', 'HRK',
            'NAVY', 'ARMY', 'USAF', 'RAF', 'RCAF', 'RAAF', 'IAF', 'PAF'
        ];

        function isMilitaryAircraft(icao, callsign) {
            // Check ICAO hex range
            const icaoInt = parseInt(icao, 16);
            for (const range of MILITARY_RANGES) {
                if (icaoInt >= range.start && icaoInt <= range.end) {
                    return { military: true, country: range.country };
                }
            }

            // Check callsign prefix
            if (callsign) {
                const upper = callsign.toUpperCase();
                for (const prefix of MILITARY_PREFIXES) {
                    if (upper.startsWith(prefix)) {
                        return { military: true, type: 'callsign' };
                    }
                }
            }

            return { military: false };
        }

        function checkSquawkCode(aircraft) {
            if (!aircraft.squawk) return null;

            const squawkInfo = SQUAWK_CODES[aircraft.squawk];
            if (squawkInfo) {
                // Show alert if not already shown
                if (!activeSquawkAlerts[aircraft.icao]) {
                    activeSquawkAlerts[aircraft.icao] = true;
                    showSquawkAlert(aircraft, squawkInfo);
                }
                return squawkInfo;
            }
            return null;
        }

        function showSquawkAlert(aircraft, squawkInfo) {
            // Create banner alert
            const banner = document.createElement('div');
            banner.className = 'squawk-alert-banner';
            banner.id = 'squawkBanner_' + aircraft.icao;
            banner.innerHTML = `
                ⚠️ ${squawkInfo.name} - ${aircraft.callsign || aircraft.icao} (${aircraft.squawk})
                <br><small>${squawkInfo.description}</small>
                <button onclick="this.parentElement.remove()" style="margin-left: 20px; background: transparent; border: 1px solid white; color: white; padding: 2px 10px; cursor: pointer;">✕</button>
            `;
            document.body.appendChild(banner);

            // Auto-remove after 30 seconds
            setTimeout(() => {
                const el = document.getElementById('squawkBanner_' + aircraft.icao);
                if (el) el.remove();
            }, 30000);

            // Audio alert
            if (!muted) {
                for (let i = 0; i < 5; i++) {
                    setTimeout(() => playAlertSound(), i * 200);
                }
            }

            showNotification(`⚠️ ${squawkInfo.name}`, `${aircraft.callsign || aircraft.icao} - Squawk ${aircraft.squawk}`);
        }

        function updateAircraftTrail(icao, lat, lon) {
            if (!aircraftTrails[icao]) {
                aircraftTrails[icao] = [];
            }

            const trail = aircraftTrails[icao];
            const lastPos = trail[trail.length - 1];

            // Only add if position changed significantly
            if (!lastPos || Math.abs(lastPos.lat - lat) > 0.001 || Math.abs(lastPos.lon - lon) > 0.001) {
                trail.push({ lat, lon, time: Date.now() });

                // Keep only last 100 positions (about 10 minutes at 1 update/6 seconds)
                if (trail.length > 100) {
                    trail.shift();
                }
            }
        }

        // Satellite state
        let satellitePasses = [];
        let selectedPass = null;
        let selectedPassIndex = 0;
        let iridiumBursts = [];
        let iridiumEventSource = null;
        let countdownInterval = null;

        // Start satellite countdown timer
        function startCountdownTimer() {
            if (countdownInterval) clearInterval(countdownInterval);
            countdownInterval = setInterval(updateSatelliteCountdown, 1000);
        }

        // Update satellite countdown display
        function updateSatelliteCountdown() {
            // Update both main and popout countdowns
            updateCountdownDisplay('');
            updateCountdownDisplay('Popout');
        }

        // Helper to update countdown elements by suffix
        function updateCountdownDisplay(suffix) {
            const container = document.getElementById('satelliteCountdown' + suffix);
            if (!container) return;

            // Use the globally selected pass
            if (!selectedPass || satellitePasses.length === 0) {
                container.style.display = 'none';
                return;
            }

            const now = new Date();
            const startTime = parsePassTime(selectedPass.startTime);
            const endTime = new Date(startTime.getTime() + selectedPass.duration * 60000);

            container.style.display = 'block';
            document.getElementById('countdownSatName' + suffix).textContent = selectedPass.satellite;

            if (now >= startTime && now <= endTime) {
                // Currently visible
                const remaining = Math.max(0, Math.floor((endTime - now) / 1000));
                const mins = Math.floor(remaining / 60);
                const secs = remaining % 60;

                document.getElementById('countdownToPass' + suffix).textContent = 'VISIBLE';
                document.getElementById('countdownToPass' + suffix).classList.add('active');
                document.getElementById('countdownPassTime' + suffix).textContent = 'Now overhead';

                document.getElementById('countdownVisibility' + suffix).textContent = `${mins}:${secs.toString().padStart(2, '0')}`;
                document.getElementById('countdownVisLabel' + suffix).textContent = 'Remaining';

                document.getElementById('countdownMaxEl' + suffix).textContent = selectedPass.maxEl + '°';
                document.getElementById('countdownDirection' + suffix).textContent = selectedPass.direction || 'Pass';

                document.getElementById('countdownStatus' + suffix).textContent = '🟢 SATELLITE CURRENTLY VISIBLE';
                document.getElementById('countdownStatus' + suffix).className = 'countdown-status visible';

            } else if (startTime > now) {
                // Upcoming pass
                const secsToPass = Math.max(0, Math.floor((startTime - now) / 1000));
                const hours = Math.floor(secsToPass / 3600);
                const mins = Math.floor((secsToPass % 3600) / 60);
                const secs = secsToPass % 60;

                let countdownStr;
                if (hours > 0) {
                    countdownStr = `${hours}:${mins.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
                } else {
                    countdownStr = `${mins}:${secs.toString().padStart(2, '0')}`;
                }

                document.getElementById('countdownToPass' + suffix).textContent = countdownStr;
                document.getElementById('countdownToPass' + suffix).classList.remove('active');
                document.getElementById('countdownPassTime' + suffix).textContent = selectedPass.startTime;

                document.getElementById('countdownVisibility' + suffix).textContent = selectedPass.duration + 'm';
                document.getElementById('countdownVisLabel' + suffix).textContent = 'Duration';

                document.getElementById('countdownMaxEl' + suffix).textContent = selectedPass.maxEl + '°';
                document.getElementById('countdownDirection' + suffix).textContent = selectedPass.direction || 'Pass';

                if (secsToPass < 300) {
                    document.getElementById('countdownStatus' + suffix).textContent = '🟡 PASS STARTING SOON';
                    document.getElementById('countdownStatus' + suffix).className = 'countdown-status upcoming';
                } else {
                    document.getElementById('countdownStatus' + suffix).textContent = 'Selected pass';
                    document.getElementById('countdownStatus' + suffix).className = 'countdown-status';
                }

            } else {
                // Pass already happened
                document.getElementById('countdownToPass' + suffix).textContent = 'PASSED';
                document.getElementById('countdownToPass' + suffix).classList.remove('active');
                document.getElementById('countdownPassTime' + suffix).textContent = selectedPass.startTime;

                document.getElementById('countdownVisibility' + suffix).textContent = selectedPass.duration + 'm';
                document.getElementById('countdownVisLabel' + suffix).textContent = 'Duration';

                document.getElementById('countdownMaxEl' + suffix).textContent = selectedPass.maxEl + '°';
                document.getElementById('countdownDirection' + suffix).textContent = selectedPass.direction || 'Pass';

                document.getElementById('countdownStatus' + suffix).textContent = 'Pass has ended';
                document.getElementById('countdownStatus' + suffix).className = 'countdown-status';
            }
        }

        // Parse pass time string to Date object
        function parsePassTime(timeStr) {
            // Expected format: "2025-12-21 14:32 UTC"
            // Remove "UTC" suffix and parse as ISO-like format
            const cleanTime = timeStr.replace(' UTC', '').replace(' ', 'T') + ':00Z';
            const parsed = new Date(cleanTime);

            // Fallback if that doesn't work
            if (isNaN(parsed.getTime())) {
                // Try parsing as-is
                return new Date(timeStr.replace(' UTC', ''));
            }
            return parsed;
        }

        // Make sections collapsible
        document.addEventListener('DOMContentLoaded', function() {
            document.querySelectorAll('.section h3').forEach(h3 => {
                h3.addEventListener('click', function() {
                    this.parentElement.classList.toggle('collapsed');
                });
            });
        });

        // Toggle section collapse
        function toggleSection(el) {
            el.closest('.section').classList.toggle('collapsed');
        }

        // Mode switching
        function switchMode(mode) {
            // Stop any running scans when switching modes
            if (isRunning) stopDecoding();
            if (isSensorRunning) stopSensorDecoding();
            if (isWifiRunning) stopWifiScan();
            if (isBtRunning) stopBtScan();
            if (isAdsbRunning) stopAdsbScan();
            if (isIridiumRunning) stopIridiumCapture();

            currentMode = mode;
            document.querySelectorAll('.mode-tab').forEach(tab => {
                const tabText = tab.textContent.toLowerCase();
                const isActive = (mode === 'pager' && tabText.includes('pager')) ||
                                 (mode === 'sensor' && tabText.includes('433')) ||
                                 (mode === 'aircraft' && tabText.includes('aircraft')) ||
                                 (mode === 'satellite' && tabText.includes('satellite')) ||
                                 (mode === 'wifi' && tabText.includes('wifi')) ||
                                 (mode === 'bluetooth' && tabText === 'bt');
                tab.classList.toggle('active', isActive);
            });
            document.getElementById('pagerMode').classList.toggle('active', mode === 'pager');
            document.getElementById('sensorMode').classList.toggle('active', mode === 'sensor');
            document.getElementById('aircraftMode').classList.toggle('active', mode === 'aircraft');
            document.getElementById('satelliteMode').classList.toggle('active', mode === 'satellite');
            document.getElementById('wifiMode').classList.toggle('active', mode === 'wifi');
            document.getElementById('bluetoothMode').classList.toggle('active', mode === 'bluetooth');
            document.getElementById('pagerStats').style.display = mode === 'pager' ? 'flex' : 'none';
            document.getElementById('sensorStats').style.display = mode === 'sensor' ? 'flex' : 'none';
            document.getElementById('aircraftStats').style.display = mode === 'aircraft' ? 'flex' : 'none';
            document.getElementById('satelliteStats').style.display = mode === 'satellite' ? 'flex' : 'none';
            document.getElementById('wifiStats').style.display = mode === 'wifi' ? 'flex' : 'none';
            document.getElementById('btStats').style.display = mode === 'bluetooth' ? 'flex' : 'none';
            document.getElementById('wifiVisuals').style.display = mode === 'wifi' ? 'grid' : 'none';
            document.getElementById('btVisuals').style.display = mode === 'bluetooth' ? 'grid' : 'none';
            document.getElementById('aircraftVisuals').style.display = mode === 'aircraft' ? 'grid' : 'none';
            document.getElementById('satelliteVisuals').style.display = mode === 'satellite' ? 'block' : 'none';

            // Update output panel title based on mode
            const titles = {
                'pager': 'Pager Decoder',
                'sensor': '433MHz Sensor Monitor',
                'aircraft': 'ADS-B Aircraft Tracker',
                'satellite': 'Satellite Monitor',
                'wifi': 'WiFi Scanner',
                'bluetooth': 'Bluetooth Scanner'
            };
            document.getElementById('outputTitle').textContent = titles[mode] || 'Signal Monitor';

            // Show/hide Device Intelligence for modes that use it (not for satellite/aircraft)
            const reconBtn = document.getElementById('reconBtn');
            const intelBtn = document.querySelector('[onclick="exportDeviceDB()"]');
            if (mode === 'satellite' || mode === 'aircraft') {
                document.getElementById('reconPanel').style.display = 'none';
                if (reconBtn) reconBtn.style.display = 'none';
                if (intelBtn) intelBtn.style.display = 'none';
            } else {
                if (reconBtn) reconBtn.style.display = 'inline-block';
                if (intelBtn) intelBtn.style.display = 'inline-block';
            }

            // Show RTL-SDR device section for modes that use it
            document.getElementById('rtlDeviceSection').style.display = (mode === 'pager' || mode === 'sensor' || mode === 'aircraft') ? 'block' : 'none';

            // Toggle mode-specific tool status displays
            document.getElementById('toolStatusPager').style.display = (mode === 'pager') ? 'grid' : 'none';
            document.getElementById('toolStatusSensor').style.display = (mode === 'sensor') ? 'grid' : 'none';
            document.getElementById('toolStatusAircraft').style.display = (mode === 'aircraft') ? 'grid' : 'none';

            // Hide waterfall and output console for satellite mode (uses its own visualizations)
            document.querySelector('.waterfall-container').style.display = (mode === 'satellite') ? 'none' : 'block';
            document.getElementById('output').style.display = (mode === 'satellite') ? 'none' : 'block';
            document.querySelector('.status-bar').style.display = (mode === 'satellite') ? 'none' : 'flex';

            // Load interfaces and initialize visualizations when switching modes
            if (mode === 'wifi') {
                refreshWifiInterfaces();
                initRadar();
                initWatchList();
            } else if (mode === 'bluetooth') {
                refreshBtInterfaces();
                initBtRadar();
            } else if (mode === 'aircraft') {
                checkAdsbTools();
                initAircraftRadar();
            } else if (mode === 'satellite') {
                initPolarPlot();
                initSatelliteList();
                checkIridiumTools();
            }
        }

        // Track unique sensor devices
        let uniqueDevices = new Set();

        // Sensor frequency
        function setSensorFreq(freq) {
            document.getElementById('sensorFrequency').value = freq;
            if (isSensorRunning) {
                fetch('/stop_sensor', {method: 'POST'})
                    .then(() => setTimeout(() => startSensorDecoding(), 500));
            }
        }

        // Start sensor decoding
        function startSensorDecoding() {
            const freq = document.getElementById('sensorFrequency').value;
            const gain = document.getElementById('sensorGain').value;
            const ppm = document.getElementById('sensorPpm').value;
            const device = getSelectedDevice();

            const config = {
                frequency: freq,
                gain: gain,
                ppm: ppm,
                device: device
            };

            fetch('/start_sensor', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(config)
            }).then(r => r.json())
              .then(data => {
                  if (data.status === 'started') {
                      setSensorRunning(true);
                      startSensorStream();
                  } else {
                      alert('Error: ' + data.message);
                  }
              });
        }

        // Stop sensor decoding
        function stopSensorDecoding() {
            fetch('/stop_sensor', {method: 'POST'})
                .then(r => r.json())
                .then(data => {
                    setSensorRunning(false);
                    if (eventSource) {
                        eventSource.close();
                        eventSource = null;
                    }
                });
        }

        function setSensorRunning(running) {
            isSensorRunning = running;
            document.getElementById('statusDot').classList.toggle('running', running);
            document.getElementById('statusText').textContent = running ? 'Listening...' : 'Idle';
            document.getElementById('startSensorBtn').style.display = running ? 'none' : 'block';
            document.getElementById('stopSensorBtn').style.display = running ? 'block' : 'none';
        }

        function startSensorStream() {
            if (eventSource) {
                eventSource.close();
            }

            eventSource = new EventSource('/stream_sensor');

            eventSource.onopen = function() {
                showInfo('Sensor stream connected...');
            };

            eventSource.onmessage = function(e) {
                const data = JSON.parse(e.data);
                if (data.type === 'sensor') {
                    addSensorReading(data);
                } else if (data.type === 'status') {
                    if (data.text === 'stopped') {
                        setSensorRunning(false);
                    }
                } else if (data.type === 'info' || data.type === 'raw') {
                    showInfo(data.text);
                }
            };

            eventSource.onerror = function(e) {
                console.error('Sensor stream error');
            };
        }

        function addSensorReading(data) {
            const output = document.getElementById('output');
            const placeholder = output.querySelector('.placeholder');
            if (placeholder) placeholder.remove();

            // Store for export
            allMessages.push(data);
            playAlert();
            pulseSignal();
            addWaterfallPoint(Date.now(), 0.8);

            sensorCount++;
            document.getElementById('sensorCount').textContent = sensorCount;

            // Track unique devices by model + id
            const deviceKey = (data.model || 'Unknown') + '_' + (data.id || data.channel || '0');
            if (!uniqueDevices.has(deviceKey)) {
                uniqueDevices.add(deviceKey);
                document.getElementById('deviceCount').textContent = uniqueDevices.size;
            }

            const card = document.createElement('div');
            card.className = 'sensor-card';

            let dataItems = '';
            const skipKeys = ['type', 'time', 'model', 'raw'];
            for (const [key, value] of Object.entries(data)) {
                if (!skipKeys.includes(key) && value !== null && value !== undefined) {
                    const label = key.replace(/_/g, ' ');
                    let displayValue = value;
                    if (key === 'temperature_C') displayValue = value + ' °C';
                    else if (key === 'temperature_F') displayValue = value + ' °F';
                    else if (key === 'humidity') displayValue = value + ' %';
                    else if (key === 'pressure_hPa') displayValue = value + ' hPa';
                    else if (key === 'wind_avg_km_h') displayValue = value + ' km/h';
                    else if (key === 'rain_mm') displayValue = value + ' mm';
                    else if (key === 'battery_ok') displayValue = value ? 'OK' : 'Low';

                    dataItems += '<div class="data-item"><div class="data-label">' + label + '</div><div class="data-value">' + displayValue + '</div></div>';
                }
            }

            const relTime = data.time ? getRelativeTime(data.time.split(' ')[1] || data.time) : 'now';

            card.innerHTML =
                '<div class="header" style="display: flex; justify-content: space-between; margin-bottom: 8px;">' +
                    '<span class="device-name">' + (data.model || 'Unknown Device') + '</span>' +
                    '<span class="msg-time" data-timestamp="' + (data.time || '') + '" style="color: #444; font-size: 10px;">' + relTime + '</span>' +
                '</div>' +
                '<div class="sensor-data">' + dataItems + '</div>';

            output.insertBefore(card, output.firstChild);

            if (autoScroll) output.scrollTop = 0;
            while (output.children.length > 100) {
                output.removeChild(output.lastChild);
            }
        }

        function toggleSensorLogging() {
            const enabled = document.getElementById('sensorLogging').checked;
            fetch('/logging', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({enabled: enabled, log_file: 'sensor_data.log'})
            });
        }

        // Audio alert settings
        let audioMuted = localStorage.getItem('audioMuted') === 'true';
        let audioContext = null;

        function initAudio() {
            if (!audioContext) {
                audioContext = new (window.AudioContext || window.webkitAudioContext)();
            }
        }

        function playAlert() {
            if (audioMuted || !audioContext) return;
            const oscillator = audioContext.createOscillator();
            const gainNode = audioContext.createGain();
            oscillator.connect(gainNode);
            gainNode.connect(audioContext.destination);
            oscillator.frequency.value = 880;
            oscillator.type = 'sine';
            gainNode.gain.setValueAtTime(0.3, audioContext.currentTime);
            gainNode.gain.exponentialRampToValueAtTime(0.01, audioContext.currentTime + 0.2);
            oscillator.start(audioContext.currentTime);
            oscillator.stop(audioContext.currentTime + 0.2);
        }

        function toggleMute() {
            audioMuted = !audioMuted;
            localStorage.setItem('audioMuted', audioMuted);
            updateMuteButton();
        }

        function updateMuteButton() {
            const btn = document.getElementById('muteBtn');
            if (btn) {
                btn.innerHTML = audioMuted ? '🔇 UNMUTE' : '🔊 MUTE';
                btn.classList.toggle('muted', audioMuted);
            }
        }

        // Message storage for export
        let allMessages = [];

        function exportCSV() {
            if (allMessages.length === 0) {
                alert('No messages to export');
                return;
            }
            const headers = ['Timestamp', 'Protocol', 'Address', 'Function', 'Type', 'Message'];
            const csv = [headers.join(',')];
            allMessages.forEach(msg => {
                const row = [
                    msg.timestamp || '',
                    msg.protocol || '',
                    msg.address || '',
                    msg.function || '',
                    msg.msg_type || '',
                    '"' + (msg.message || '').replace(/"/g, '""') + '"'
                ];
                csv.push(row.join(','));
            });
            downloadFile(csv.join('\\n'), 'intercept_messages.csv', 'text/csv');
        }

        function exportJSON() {
            if (allMessages.length === 0) {
                alert('No messages to export');
                return;
            }
            downloadFile(JSON.stringify(allMessages, null, 2), 'intercept_messages.json', 'application/json');
        }

        function downloadFile(content, filename, type) {
            const blob = new Blob([content], { type });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = filename;
            a.click();
            URL.revokeObjectURL(url);
        }

        // Auto-scroll setting
        let autoScroll = localStorage.getItem('autoScroll') !== 'false';

        function toggleAutoScroll() {
            autoScroll = !autoScroll;
            localStorage.setItem('autoScroll', autoScroll);
            updateAutoScrollButton();
        }

        function updateAutoScrollButton() {
            const btn = document.getElementById('autoScrollBtn');
            if (btn) {
                btn.innerHTML = autoScroll ? '⬇ AUTO-SCROLL ON' : '⬇ AUTO-SCROLL OFF';
                btn.classList.toggle('active', autoScroll);
            }
        }

        // Signal activity meter
        let signalActivity = 0;
        let lastMessageTime = 0;

        function updateSignalMeter() {
            const now = Date.now();
            const timeSinceLastMsg = now - lastMessageTime;

            // Decay signal activity over time
            if (timeSinceLastMsg > 1000) {
                signalActivity = Math.max(0, signalActivity - 0.05);
            }

            const meter = document.getElementById('signalMeter');
            const bars = meter?.querySelectorAll('.signal-bar');
            if (bars) {
                const activeBars = Math.ceil(signalActivity * bars.length);
                bars.forEach((bar, i) => {
                    bar.classList.toggle('active', i < activeBars);
                });
            }
        }

        function pulseSignal() {
            signalActivity = Math.min(1, signalActivity + 0.4);
            lastMessageTime = Date.now();

            // Flash waterfall canvas
            const canvas = document.getElementById('waterfallCanvas');
            if (canvas) {
                canvas.classList.add('active');
                setTimeout(() => canvas.classList.remove('active'), 500);
            }
        }

        // Waterfall display
        const waterfallData = [];
        const maxWaterfallRows = 50;

        function addWaterfallPoint(timestamp, intensity) {
            waterfallData.push({ time: timestamp, intensity });
            if (waterfallData.length > maxWaterfallRows * 100) {
                waterfallData.shift();
            }
            renderWaterfall();
        }

        function renderWaterfall() {
            const canvas = document.getElementById('waterfallCanvas');
            if (!canvas) return;
            const ctx = canvas.getContext('2d', { willReadFrequently: true });
            const width = canvas.width;
            const height = canvas.height;

            // Shift existing image down
            const imageData = ctx.getImageData(0, 0, width, height - 2);
            ctx.putImageData(imageData, 0, 2);

            // Draw new row at top
            ctx.fillStyle = '#000';
            ctx.fillRect(0, 0, width, 2);

            // Add activity markers
            const now = Date.now();
            const recentData = waterfallData.filter(d => now - d.time < 100);
            recentData.forEach(d => {
                const x = Math.random() * width;
                const hue = 180 + (d.intensity * 60); // cyan to green
                ctx.fillStyle = `hsla(${hue}, 100%, 50%, ${d.intensity})`;
                ctx.fillRect(x - 2, 0, 4, 2);
            });
        }

        // Relative timestamps
        function getRelativeTime(timestamp) {
            if (!timestamp) return '';
            const now = new Date();
            const parts = timestamp.split(':');
            const msgTime = new Date();
            msgTime.setHours(parseInt(parts[0]), parseInt(parts[1]), parseInt(parts[2]));

            const diff = Math.floor((now - msgTime) / 1000);
            if (diff < 5) return 'just now';
            if (diff < 60) return diff + 's ago';
            if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
            return timestamp;
        }

        function updateRelativeTimes() {
            document.querySelectorAll('.msg-time').forEach(el => {
                const ts = el.dataset.timestamp;
                if (ts) el.textContent = getRelativeTime(ts);
            });
        }

        // Update timers
        setInterval(updateSignalMeter, 100);
        setInterval(updateRelativeTimes, 10000);

        // Default presets (UK frequencies)
        const defaultPresets = ['153.350', '153.025'];

        // Load presets from localStorage or use defaults
        function loadPresets() {
            const saved = localStorage.getItem('pagerPresets');
            return saved ? JSON.parse(saved) : [...defaultPresets];
        }

        function savePresets(presets) {
            localStorage.setItem('pagerPresets', JSON.stringify(presets));
        }

        function renderPresets() {
            const presets = loadPresets();
            const container = document.getElementById('presetButtons');
            container.innerHTML = presets.map(freq =>
                `<button class="preset-btn" onclick="setFreq('${freq}')" oncontextmenu="removePreset('${freq}'); return false;" title="Right-click to remove">${freq}</button>`
            ).join('');
        }

        function addPreset() {
            const input = document.getElementById('newPresetFreq');
            const freq = input.value.trim();
            if (!freq || isNaN(parseFloat(freq))) {
                alert('Please enter a valid frequency');
                return;
            }
            const presets = loadPresets();
            if (!presets.includes(freq)) {
                presets.push(freq);
                savePresets(presets);
                renderPresets();
            }
            input.value = '';
        }

        function removePreset(freq) {
            if (confirm('Remove preset ' + freq + ' MHz?')) {
                let presets = loadPresets();
                presets = presets.filter(p => p !== freq);
                savePresets(presets);
                renderPresets();
            }
        }

        function resetPresets() {
            if (confirm('Reset to default presets?')) {
                savePresets([...defaultPresets]);
                renderPresets();
            }
        }

        // Initialize presets on load
        renderPresets();

        // Initialize button states on load
        updateMuteButton();
        updateAutoScrollButton();

        // Initialize audio context on first user interaction (required by browsers)
        document.addEventListener('click', function initAudioOnClick() {
            initAudio();
            document.removeEventListener('click', initAudioOnClick);
        }, { once: true });

        function setFreq(freq) {
            document.getElementById('frequency').value = freq;
            // Auto-restart decoder with new frequency if currently running
            if (isRunning) {
                fetch('/stop', {method: 'POST'})
                    .then(() => {
                        setTimeout(() => startDecoding(), 500);
                    });
            }
        }

        function refreshDevices() {
            fetch('/devices')
                .then(r => r.json())
                .then(devices => {
                    deviceList = devices;
                    const select = document.getElementById('deviceSelect');
                    if (devices.length === 0) {
                        select.innerHTML = '<option value="0">No devices found</option>';
                    } else {
                        select.innerHTML = devices.map(d =>
                            `<option value="${d.index}">${d.index}: ${d.name}</option>`
                        ).join('');
                    }
                });
        }

        function getSelectedDevice() {
            return document.getElementById('deviceSelect').value;
        }

        function getSelectedProtocols() {
            const protocols = [];
            if (document.getElementById('proto_pocsag512').checked) protocols.push('POCSAG512');
            if (document.getElementById('proto_pocsag1200').checked) protocols.push('POCSAG1200');
            if (document.getElementById('proto_pocsag2400').checked) protocols.push('POCSAG2400');
            if (document.getElementById('proto_flex').checked) protocols.push('FLEX');
            return protocols;
        }

        function startDecoding() {
            const freq = document.getElementById('frequency').value;
            const gain = document.getElementById('gain').value;
            const squelch = document.getElementById('squelch').value;
            const ppm = document.getElementById('ppm').value;
            const device = getSelectedDevice();
            const protocols = getSelectedProtocols();

            if (protocols.length === 0) {
                alert('Please select at least one protocol');
                return;
            }

            const config = {
                frequency: freq,
                gain: gain,
                squelch: squelch,
                ppm: ppm,
                device: device,
                protocols: protocols
            };

            fetch('/start', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(config)
            }).then(r => r.json())
              .then(data => {
                  if (data.status === 'started') {
                      setRunning(true);
                      startStream();
                  } else {
                      alert('Error: ' + data.message);
                  }
              })
              .catch(err => {
                  console.error('Start error:', err);
              });
        }

        function stopDecoding() {
            fetch('/stop', {method: 'POST'})
                .then(r => r.json())
                .then(data => {
                    setRunning(false);
                    if (eventSource) {
                        eventSource.close();
                        eventSource = null;
                    }
                });
        }

        function killAll() {
            fetch('/killall', {method: 'POST'})
                .then(r => r.json())
                .then(data => {
                    setRunning(false);
                    if (eventSource) {
                        eventSource.close();
                        eventSource = null;
                    }
                    showInfo('Killed all processes: ' + (data.processes.length ? data.processes.join(', ') : 'none running'));
                });
        }

        function checkStatus() {
            fetch('/status')
                .then(r => r.json())
                .then(data => {
                    if (data.running !== isRunning) {
                        setRunning(data.running);
                        if (data.running && !eventSource) {
                            startStream();
                        }
                    }
                });
        }

        // Periodic status check every 5 seconds
        setInterval(checkStatus, 5000);

        function toggleLogging() {
            const enabled = document.getElementById('loggingEnabled').checked;
            const logFile = document.getElementById('logFilePath').value;
            fetch('/logging', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({enabled: enabled, log_file: logFile})
            }).then(r => r.json())
              .then(data => {
                  showInfo(data.logging ? 'Logging enabled: ' + data.log_file : 'Logging disabled');
              });
        }

        function setRunning(running) {
            isRunning = running;
            document.getElementById('statusDot').classList.toggle('running', running);
            document.getElementById('statusText').textContent = running ? 'Decoding...' : 'Idle';
            document.getElementById('startBtn').style.display = running ? 'none' : 'block';
            document.getElementById('stopBtn').style.display = running ? 'block' : 'none';
        }

        function startStream() {
            if (eventSource) {
                eventSource.close();
            }

            eventSource = new EventSource('/stream');

            eventSource.onopen = function() {
                showInfo('Stream connected...');
            };

            eventSource.onmessage = function(e) {
                const data = JSON.parse(e.data);

                if (data.type === 'message') {
                    addMessage(data);
                } else if (data.type === 'status') {
                    if (data.text === 'stopped') {
                        setRunning(false);
                    } else if (data.text === 'started') {
                        showInfo('Decoder started, waiting for signals...');
                    }
                } else if (data.type === 'info') {
                    showInfo(data.text);
                } else if (data.type === 'raw') {
                    showInfo(data.text);
                }
            };

            eventSource.onerror = function(e) {
                checkStatus();
            };
        }

        function addMessage(msg) {
            const output = document.getElementById('output');

            // Remove placeholder if present
            const placeholder = output.querySelector('.placeholder');
            if (placeholder) {
                placeholder.remove();
            }

            // Store message for export
            allMessages.push(msg);

            // Play audio alert
            playAlert();

            // Update signal meter
            pulseSignal();

            // Add to waterfall
            addWaterfallPoint(Date.now(), 0.8);

            msgCount++;
            document.getElementById('msgCount').textContent = msgCount;

            let protoClass = '';
            if (msg.protocol.includes('POCSAG')) {
                pocsagCount++;
                protoClass = 'pocsag';
                document.getElementById('pocsagCount').textContent = pocsagCount;
            } else if (msg.protocol.includes('FLEX')) {
                flexCount++;
                protoClass = 'flex';
                document.getElementById('flexCount').textContent = flexCount;
            }

            const isNumeric = /^[0-9\s\-\*\#U]+$/.test(msg.message);
            const relativeTime = getRelativeTime(msg.timestamp);

            const msgEl = document.createElement('div');
            msgEl.className = 'message ' + protoClass;
            msgEl.innerHTML = `
                <div class="header">
                    <span class="protocol">${msg.protocol}</span>
                    <span class="msg-time" data-timestamp="${msg.timestamp}" title="${msg.timestamp}">${relativeTime}</span>
                </div>
                <div class="address">Address: ${msg.address}${msg.function ? ' | Func: ' + msg.function : ''}</div>
                <div class="content ${isNumeric ? 'numeric' : ''}">${escapeHtml(msg.message)}</div>
            `;

            output.insertBefore(msgEl, output.firstChild);

            // Auto-scroll to top (newest messages)
            if (autoScroll) {
                output.scrollTop = 0;
            }

            // Limit messages displayed
            while (output.children.length > 100) {
                output.removeChild(output.lastChild);
            }
        }

        function escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }

        function escapeAttr(text) {
            // Escape for use in HTML attributes (especially onclick handlers)
            if (text === null || text === undefined) return '';
            var s = String(text);
            s = s.replace(/&/g, '&amp;');
            s = s.replace(/'/g, '&#39;');
            s = s.replace(/"/g, '&quot;');
            s = s.replace(/</g, '&lt;');
            s = s.replace(/>/g, '&gt;');
            return s;
        }

        function isValidMac(mac) {
            // Validate MAC address format (XX:XX:XX:XX:XX:XX)
            return /^([0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}$/.test(mac);
        }

        function isValidChannel(ch) {
            // Validate WiFi channel (1-200 covers all bands)
            const num = parseInt(ch, 10);
            return !isNaN(num) && num >= 1 && num <= 200;
        }

        function showInfo(text) {
            const output = document.getElementById('output');

            // Clear placeholder only (has the 'placeholder' class)
            const placeholder = output.querySelector('.placeholder');
            if (placeholder) {
                placeholder.remove();
            }

            const infoEl = document.createElement('div');
            infoEl.className = 'info-msg';
            infoEl.style.cssText = 'padding: 12px 15px; margin-bottom: 8px; background: #0a0a0a; border: 1px solid #1a1a1a; border-left: 2px solid #00d4ff; font-family: "JetBrains Mono", monospace; font-size: 11px; color: #888; word-break: break-all;';
            infoEl.textContent = text;
            output.insertBefore(infoEl, output.firstChild);
        }

        function showError(text) {
            const output = document.getElementById('output');

            // Clear placeholder only (has the 'placeholder' class)
            const placeholder = output.querySelector('.placeholder');
            if (placeholder) {
                placeholder.remove();
            }

            const errorEl = document.createElement('div');
            errorEl.className = 'error-msg';
            errorEl.style.cssText = 'padding: 12px 15px; margin-bottom: 8px; background: #1a0a0a; border: 1px solid #2a1a1a; border-left: 2px solid #ff3366; font-family: "JetBrains Mono", monospace; font-size: 11px; color: #ff6688; word-break: break-all;';
            errorEl.textContent = '⚠ ' + text;
            output.insertBefore(errorEl, output.firstChild);
        }

        function clearMessages() {
            document.getElementById('output').innerHTML = `
                <div class="placeholder" style="color: #888; text-align: center; padding: 50px;">
                    Messages cleared. ${isRunning || isSensorRunning ? 'Waiting for new messages...' : 'Start decoding to receive messages.'}
                </div>
            `;
            msgCount = 0;
            pocsagCount = 0;
            flexCount = 0;
            sensorCount = 0;
            uniqueDevices.clear();
            document.getElementById('msgCount').textContent = '0';
            document.getElementById('pocsagCount').textContent = '0';
            document.getElementById('flexCount').textContent = '0';
            document.getElementById('sensorCount').textContent = '0';
            document.getElementById('deviceCount').textContent = '0';

            // Reset recon data
            deviceDatabase.clear();
            newDeviceAlerts = 0;
            anomalyAlerts = 0;
            document.getElementById('trackedCount').textContent = '0';
            document.getElementById('newDeviceCount').textContent = '0';
            document.getElementById('anomalyCount').textContent = '0';
            document.getElementById('reconContent').innerHTML = '<div style="color: #444; text-align: center; padding: 30px; font-size: 11px;">Device intelligence data will appear here as signals are intercepted.</div>';
        }

        // ============== DEVICE INTELLIGENCE & RECONNAISSANCE ==============

        // Device tracking database
        const deviceDatabase = new Map(); // key: deviceId, value: device profile
        // Default to true if not set, so device intelligence works by default
        let reconEnabled = localStorage.getItem('reconEnabled') !== 'false';
        let newDeviceAlerts = 0;
        let anomalyAlerts = 0;

        // Device profile structure
        function createDeviceProfile(deviceId, protocol, firstSeen) {
            return {
                id: deviceId,
                protocol: protocol,
                firstSeen: firstSeen,
                lastSeen: firstSeen,
                transmissionCount: 1,
                transmissions: [firstSeen], // timestamps of recent transmissions
                avgInterval: null, // average time between transmissions
                addresses: new Set(),
                models: new Set(),
                messages: [],
                isNew: true,
                anomalies: [],
                signalStrength: [],
                encrypted: null // null = unknown, true/false
            };
        }

        // Analyze transmission patterns for anomalies
        function analyzeTransmissions(profile) {
            const anomalies = [];
            const now = Date.now();

            // Need at least 3 transmissions to analyze patterns
            if (profile.transmissions.length < 3) {
                return anomalies;
            }

            // Calculate intervals between transmissions
            const intervals = [];
            for (let i = 1; i < profile.transmissions.length; i++) {
                intervals.push(profile.transmissions[i] - profile.transmissions[i-1]);
            }

            // Calculate average and standard deviation
            const avg = intervals.reduce((a, b) => a + b, 0) / intervals.length;
            profile.avgInterval = avg;

            const variance = intervals.reduce((a, b) => a + Math.pow(b - avg, 2), 0) / intervals.length;
            const stdDev = Math.sqrt(variance);

            // Check for burst transmission (sudden increase in frequency)
            const lastInterval = intervals[intervals.length - 1];
            if (avg > 0 && lastInterval < avg * 0.2) {
                anomalies.push({
                    type: 'burst',
                    severity: 'medium',
                    message: 'Burst transmission detected - interval ' + Math.round(lastInterval/1000) + 's vs avg ' + Math.round(avg/1000) + 's'
                });
            }

            // Check for silence break (device was quiet, now transmitting again)
            if (avg > 0 && lastInterval > avg * 5) {
                anomalies.push({
                    type: 'silence_break',
                    severity: 'low',
                    message: 'Device resumed after ' + Math.round(lastInterval/60000) + ' min silence'
                });
            }

            return anomalies;
        }

        // Check for encryption indicators
        function detectEncryption(message) {
            if (!message || message === '[No Message]' || message === '[Tone Only]') {
                return null; // Can't determine
            }

            // Check for high entropy (random-looking data)
            const printableRatio = (message.match(/[a-zA-Z0-9\s.,!?-]/g) || []).length / message.length;

            // Check for common encrypted patterns (hex strings, base64-like)
            const hexPattern = /^[0-9A-Fa-f\s]+$/;
            const hasNonPrintable = /[^\x20-\x7E]/.test(message);

            if (printableRatio > 0.8 && !hasNonPrintable) {
                return false; // Likely plaintext
            } else if (hexPattern.test(message.replace(/\s/g, '')) || hasNonPrintable) {
                return true; // Likely encrypted or encoded
            }

            return null; // Unknown
        }

        // Generate device fingerprint
        function generateDeviceId(data) {
            if (data.protocol && data.protocol.includes('POCSAG')) {
                return 'PAGER_' + (data.address || 'UNK');
            } else if (data.protocol === 'FLEX') {
                return 'FLEX_' + (data.address || 'UNK');
            } else if (data.protocol === 'WiFi-AP') {
                return 'WIFI_AP_' + (data.address || 'UNK').replace(/:/g, '');
            } else if (data.protocol === 'WiFi-Client') {
                return 'WIFI_CLIENT_' + (data.address || 'UNK').replace(/:/g, '');
            } else if (data.protocol === 'Bluetooth' || data.protocol === 'BLE') {
                return 'BT_' + (data.address || 'UNK').replace(/:/g, '');
            } else if (data.model) {
                // 433MHz sensor
                const id = data.id || data.channel || data.unit || '0';
                return 'SENSOR_' + data.model.replace(/\s+/g, '_') + '_' + id;
            }
            return 'UNKNOWN_' + Date.now();
        }

        // Track a device transmission
        function trackDevice(data) {
            const now = Date.now();
            const deviceId = generateDeviceId(data);
            const protocol = data.protocol || data.model || 'Unknown';

            let profile = deviceDatabase.get(deviceId);
            let isNewDevice = false;

            if (!profile) {
                // New device discovered
                profile = createDeviceProfile(deviceId, protocol, now);
                isNewDevice = true;
                newDeviceAlerts++;
                document.getElementById('newDeviceCount').textContent = newDeviceAlerts;
            } else {
                // Update existing profile
                profile.lastSeen = now;
                profile.transmissionCount++;
                profile.transmissions.push(now);
                profile.isNew = false;

                // Keep only last 100 transmissions for analysis
                if (profile.transmissions.length > 100) {
                    profile.transmissions = profile.transmissions.slice(-100);
                }
            }

            // Track addresses
            if (data.address) profile.addresses.add(data.address);
            if (data.model) profile.models.add(data.model);

            // Store recent messages (keep last 10)
            if (data.message) {
                profile.messages.unshift({
                    text: data.message,
                    time: now
                });
                if (profile.messages.length > 10) profile.messages.pop();

                // Detect encryption
                const encrypted = detectEncryption(data.message);
                if (encrypted !== null) profile.encrypted = encrypted;
            }

            // Analyze for anomalies
            const newAnomalies = analyzeTransmissions(profile);
            if (newAnomalies.length > 0) {
                profile.anomalies = profile.anomalies.concat(newAnomalies);
                anomalyAlerts += newAnomalies.length;
                document.getElementById('anomalyCount').textContent = anomalyAlerts;
            }

            deviceDatabase.set(deviceId, profile);
            document.getElementById('trackedCount').textContent = deviceDatabase.size;

            // Update recon display
            if (reconEnabled) {
                updateReconDisplay(deviceId, profile, isNewDevice, newAnomalies);
            }

            return { deviceId, profile, isNewDevice, anomalies: newAnomalies };
        }

        // Update reconnaissance display
        function updateReconDisplay(deviceId, profile, isNewDevice, anomalies) {
            const content = document.getElementById('reconContent');

            // Remove placeholder if present
            const placeholder = content.querySelector('div[style*="text-align: center"]');
            if (placeholder) placeholder.remove();

            // Check if device row already exists
            let row = document.getElementById('device_' + deviceId.replace(/[^a-zA-Z0-9]/g, '_'));

            if (!row) {
                // Create new row
                row = document.createElement('div');
                row.id = 'device_' + deviceId.replace(/[^a-zA-Z0-9]/g, '_');
                row.className = 'device-row' + (isNewDevice ? ' new-device' : '');
                content.insertBefore(row, content.firstChild);
            }

            // Determine protocol badge class
            let badgeClass = 'proto-unknown';
            if (profile.protocol.includes('POCSAG')) badgeClass = 'proto-pocsag';
            else if (profile.protocol === 'FLEX') badgeClass = 'proto-flex';
            else if (profile.protocol.includes('SENSOR') || profile.models.size > 0) badgeClass = 'proto-433';

            // Calculate transmission rate bar width
            const maxRate = 100; // Max expected transmissions
            const rateWidth = Math.min(100, (profile.transmissionCount / maxRate) * 100);

            // Determine timeline status
            const timeSinceLast = Date.now() - profile.lastSeen;
            let timelineDot = 'recent';
            if (timeSinceLast > 300000) timelineDot = 'old'; // > 5 min
            else if (timeSinceLast > 60000) timelineDot = 'stale'; // > 1 min

            // Build encryption indicator
            let encStatus = 'Unknown';
            let encClass = '';
            if (profile.encrypted === true) { encStatus = 'Encrypted'; encClass = 'encrypted'; }
            else if (profile.encrypted === false) { encStatus = 'Plaintext'; encClass = 'plaintext'; }

            // Format time
            const lastSeenStr = getRelativeTime(new Date(profile.lastSeen).toTimeString().split(' ')[0]);
            const firstSeenStr = new Date(profile.firstSeen).toLocaleTimeString();

            // Update row content
            row.className = 'device-row' + (isNewDevice ? ' new-device' : '') + (anomalies.length > 0 ? ' anomaly' : '');
            row.innerHTML = `
                <div class="device-info">
                    <div class="device-name-row">
                        <span class="timeline-dot ${timelineDot}"></span>
                        <span class="badge ${badgeClass}">${profile.protocol.substring(0, 8)}</span>
                        ${deviceId.substring(0, 30)}
                    </div>
                    <div class="device-id">
                        First: ${firstSeenStr} | Last: ${lastSeenStr} | TX: ${profile.transmissionCount}
                        ${profile.avgInterval ? ' | Interval: ' + Math.round(profile.avgInterval/1000) + 's' : ''}
                    </div>
                </div>
                <div class="device-meta ${encClass}">${encStatus}</div>
                <div>
                    <div class="transmission-bar">
                        <div class="transmission-bar-fill" style="width: ${rateWidth}%"></div>
                    </div>
                </div>
                <div class="device-meta">${Array.from(profile.addresses).slice(0, 2).join(', ')}</div>
            `;

            // Show anomaly alerts
            if (anomalies.length > 0) {
                anomalies.forEach(a => {
                    const alertEl = document.createElement('div');
                    alertEl.style.cssText = 'padding: 5px 15px; background: rgba(255,51,102,0.1); border-left: 2px solid var(--accent-red); font-size: 10px; color: var(--accent-red);';
                    alertEl.textContent = '⚠ ' + a.message;
                    row.appendChild(alertEl);
                });
            }

            // Limit displayed devices
            while (content.children.length > 50) {
                content.removeChild(content.lastChild);
            }
        }

        // Toggle recon panel visibility
        function toggleRecon() {
            reconEnabled = !reconEnabled;
            localStorage.setItem('reconEnabled', reconEnabled);
            document.getElementById('reconPanel').style.display = reconEnabled ? 'block' : 'none';
            document.getElementById('reconBtn').classList.toggle('active', reconEnabled);

            // Populate recon display if enabled and we have data
            if (reconEnabled && deviceDatabase.size > 0) {
                deviceDatabase.forEach((profile, deviceId) => {
                    updateReconDisplay(deviceId, profile, false, []);
                });
            }
        }

        // Initialize recon state
        if (reconEnabled) {
            document.getElementById('reconPanel').style.display = 'block';
            document.getElementById('reconBtn').classList.add('active');
        } else {
            document.getElementById('reconPanel').style.display = 'none';
        }

        // Hook into existing message handlers to track devices
        const originalAddMessage = addMessage;
        addMessage = function(msg) {
            originalAddMessage(msg);
            trackDevice(msg);
        };

        const originalAddSensorReading = addSensorReading;
        addSensorReading = function(data) {
            originalAddSensorReading(data);
            trackDevice(data);
        };

        // Export device database
        function exportDeviceDB() {
            const data = [];
            deviceDatabase.forEach((profile, id) => {
                data.push({
                    id: id,
                    protocol: profile.protocol,
                    firstSeen: new Date(profile.firstSeen).toISOString(),
                    lastSeen: new Date(profile.lastSeen).toISOString(),
                    transmissionCount: profile.transmissionCount,
                    avgIntervalSeconds: profile.avgInterval ? Math.round(profile.avgInterval / 1000) : null,
                    addresses: Array.from(profile.addresses),
                    models: Array.from(profile.models),
                    encrypted: profile.encrypted,
                    anomalyCount: profile.anomalies.length,
                    recentMessages: profile.messages.slice(0, 5).map(m => m.text)
                });
            });
            downloadFile(JSON.stringify(data, null, 2), 'intercept_device_intelligence.json', 'application/json');
        }

        // Toggle recon panel collapse
        function toggleReconCollapse() {
            const panel = document.getElementById('reconPanel');
            const icon = document.getElementById('reconCollapseIcon');
            panel.classList.toggle('collapsed');
            icon.textContent = panel.classList.contains('collapsed') ? '▶' : '▼';
        }

        // ============== WIFI RECONNAISSANCE ==============

        let wifiEventSource = null;
        let isWifiRunning = false;
        let monitorInterface = null;
        let wifiNetworks = {};
        let wifiClients = {};
        let apCount = 0;
        let clientCount = 0;
        let handshakeCount = 0;
        let rogueApCount = 0;
        let droneCount = 0;
        let detectedDrones = {};  // Track detected drones by BSSID
        let ssidToBssids = {};  // Track SSIDs to their BSSIDs for rogue AP detection
        let rogueApDetails = {};  // Store details about rogue APs: {ssid: [{bssid, signal, channel, firstSeen}]}
        let activeCapture = null;  // {bssid, channel, file, startTime, pollInterval}
        let watchMacs = JSON.parse(localStorage.getItem('watchMacs') || '[]');
        let alertedMacs = new Set();  // Prevent duplicate alerts per session

        // 5GHz channel mapping for the graph
        const channels5g = ['36', '40', '44', '48', '52', '56', '60', '64', '100', '149', '153', '157', '161', '165'];

        // Drone SSID patterns for detection
        const dronePatterns = [
            /^DJI[-_]/i, /Mavic/i, /Phantom/i, /^Spark[-_]/i, /^Mini[-_]/i, /^Air[-_]/i,
            /Inspire/i, /Matrice/i, /Avata/i, /^FPV[-_]/i, /Osmo/i, /RoboMaster/i, /Tello/i,
            /Parrot/i, /Bebop/i, /Anafi/i, /^Disco[-_]/i, /Mambo/i, /Swing/i,
            /Autel/i, /^EVO[-_]/i, /Dragonfish/i, /Skydio/i,
            /Holy.?Stone/i, /Potensic/i, /SYMA/i, /Hubsan/i, /Eachine/i, /FIMI/i,
            /Yuneec/i, /Typhoon/i, /PowerVision/i, /PowerEgg/i,
            /Drone/i, /^UAV[-_]/i, /Quadcopter/i, /^RC[-_]Drone/i
        ];

        // Drone OUI prefixes
        const droneOuiPrefixes = {
            '60:60:1F': 'DJI', '48:1C:B9': 'DJI', '34:D2:62': 'DJI', 'E0:DB:55': 'DJI',
            'C8:6C:87': 'DJI', 'A0:14:3D': 'DJI', '70:D7:11': 'DJI', '98:3A:56': 'DJI',
            '90:03:B7': 'Parrot', '00:12:1C': 'Parrot', '00:26:7E': 'Parrot',
            '8C:F5:A3': 'Autel', 'D8:E0:E1': 'Autel', 'F8:0F:6F': 'Skydio'
        };

        // Check if network is a drone
        function isDrone(ssid, bssid) {
            // Check SSID patterns
            if (ssid) {
                for (const pattern of dronePatterns) {
                    if (pattern.test(ssid)) {
                        return { isDrone: true, method: 'SSID', brand: ssid.split(/[-_\s]/)[0] };
                    }
                }
            }
            // Check OUI prefix
            if (bssid) {
                const prefix = bssid.substring(0, 8).toUpperCase();
                if (droneOuiPrefixes[prefix]) {
                    return { isDrone: true, method: 'OUI', brand: droneOuiPrefixes[prefix] };
                }
            }
            return { isDrone: false };
        }

        // Handle drone detection
        function handleDroneDetection(net, droneInfo) {
            if (detectedDrones[net.bssid]) return; // Already detected

            detectedDrones[net.bssid] = {
                ssid: net.essid,
                bssid: net.bssid,
                brand: droneInfo.brand,
                method: droneInfo.method,
                signal: net.power,
                channel: net.channel,
                firstSeen: new Date().toISOString()
            };

            droneCount++;
            document.getElementById('droneCount').textContent = droneCount;

            // Calculate approximate distance from signal strength
            const rssi = parseInt(net.power) || -70;
            const distance = estimateDroneDistance(rssi);

            // Triple alert for drones
            playAlert();
            setTimeout(playAlert, 200);
            setTimeout(playAlert, 400);

            // Show drone alert
            showDroneAlert(net.essid, net.bssid, droneInfo.brand, distance, rssi);
        }

        // Estimate distance from RSSI (rough approximation)
        function estimateDroneDistance(rssi) {
            // Using free-space path loss model (very approximate)
            // Reference: -30 dBm at 1 meter
            const txPower = -30;
            const n = 2.5; // Path loss exponent (2-4, higher for obstacles)
            const distance = Math.pow(10, (txPower - rssi) / (10 * n));
            return Math.round(distance);
        }

        // Show drone alert popup
        function showDroneAlert(ssid, bssid, brand, distance, rssi) {
            const alertDiv = document.createElement('div');
            alertDiv.className = 'drone-alert';
            alertDiv.innerHTML = `
                <div style="font-weight: bold; color: var(--accent-orange); font-size: 16px;">🚁 DRONE DETECTED</div>
                <div style="margin: 10px 0;">
                    <div><strong>SSID:</strong> ${escapeHtml(ssid || 'Unknown')}</div>
                    <div><strong>BSSID:</strong> ${bssid}</div>
                    <div><strong>Brand:</strong> ${brand || 'Unknown'}</div>
                    <div><strong>Signal:</strong> ${rssi} dBm</div>
                    <div><strong>Est. Distance:</strong> ~${distance}m</div>
                </div>
                <button onclick="this.parentElement.remove()" style="padding: 6px 16px; cursor: pointer; background: var(--accent-orange); border: none; color: #000; border-radius: 4px;">Dismiss</button>
            `;
            alertDiv.style.cssText = 'position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%); background: #1a1a2e; border: 2px solid var(--accent-orange); padding: 20px; border-radius: 8px; z-index: 10000; text-align: center; box-shadow: 0 0 30px rgba(255,165,0,0.5); min-width: 280px;';
            document.body.appendChild(alertDiv);
            setTimeout(() => { if (alertDiv.parentElement) alertDiv.remove(); }, 15000);
        }

        // Initialize watch list display
        function initWatchList() {
            updateWatchListDisplay();
        }

        // Add MAC to watch list
        function addWatchMac() {
            const input = document.getElementById('watchMacInput');
            const mac = input.value.trim().toUpperCase();
            if (!mac || !/^([0-9A-F]{2}:){5}[0-9A-F]{2}$/.test(mac)) {
                alert('Please enter a valid MAC address (AA:BB:CC:DD:EE:FF)');
                return;
            }
            if (!watchMacs.includes(mac)) {
                watchMacs.push(mac);
                localStorage.setItem('watchMacs', JSON.stringify(watchMacs));
                updateWatchListDisplay();
            }
            input.value = '';
        }

        // Remove MAC from watch list
        function removeWatchMac(mac) {
            watchMacs = watchMacs.filter(m => m !== mac);
            localStorage.setItem('watchMacs', JSON.stringify(watchMacs));
            alertedMacs.delete(mac);
            updateWatchListDisplay();
        }

        // Update watch list display
        function updateWatchListDisplay() {
            const container = document.getElementById('watchList');
            if (!container) return;
            if (watchMacs.length === 0) {
                container.innerHTML = '<div style="color: #555;">No MACs in watch list</div>';
            } else {
                container.innerHTML = watchMacs.map(mac =>
                    `<div style="display: flex; justify-content: space-between; align-items: center; padding: 2px 0;">
                        <span>${mac}</span>
                        <button onclick="removeWatchMac('${mac}')" style="background: none; border: none; color: var(--accent-red); cursor: pointer; font-size: 10px;">✕</button>
                    </div>`
                ).join('');
            }
        }

        // Check if MAC is in watch list and alert
        function checkWatchList(mac, type) {
            const upperMac = mac.toUpperCase();
            if (watchMacs.includes(upperMac) && !alertedMacs.has(upperMac)) {
                alertedMacs.add(upperMac);
                // Play alert sound multiple times for urgency
                playAlert();
                setTimeout(playAlert, 300);
                setTimeout(playAlert, 600);
                // Show prominent alert
                showProximityAlert(mac, type);
            }
        }

        // Show proximity alert popup
        function showProximityAlert(mac, type) {
            const alertDiv = document.createElement('div');
            alertDiv.className = 'proximity-alert';
            alertDiv.innerHTML = `
                <div style="font-weight: bold; color: var(--accent-red);">⚠ PROXIMITY ALERT</div>
                <div>Watched ${type} detected:</div>
                <div style="font-family: monospace; font-size: 14px;">${mac}</div>
                <button onclick="this.parentElement.remove()" style="margin-top: 8px; padding: 4px 12px; cursor: pointer;">Dismiss</button>
            `;
            alertDiv.style.cssText = 'position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%); background: #1a1a2e; border: 2px solid var(--accent-red); padding: 20px; border-radius: 8px; z-index: 10000; text-align: center; box-shadow: 0 0 30px rgba(255,0,0,0.5);';
            document.body.appendChild(alertDiv);
            // Auto-dismiss after 10 seconds
            setTimeout(() => alertDiv.remove(), 10000);
        }

        // Check for rogue APs (same SSID, different BSSID)
        function checkRogueAP(ssid, bssid, channel, signal) {
            if (!ssid || ssid === 'Hidden' || ssid === '[Hidden]') return false;

            if (!ssidToBssids[ssid]) {
                ssidToBssids[ssid] = new Set();
            }

            // Store details for this BSSID
            if (!rogueApDetails[ssid]) {
                rogueApDetails[ssid] = [];
            }

            // Check if we already have this BSSID stored
            const existingEntry = rogueApDetails[ssid].find(e => e.bssid === bssid);
            if (!existingEntry) {
                rogueApDetails[ssid].push({
                    bssid: bssid,
                    channel: channel || '?',
                    signal: signal || '?',
                    firstSeen: new Date().toLocaleTimeString()
                });
            }

            const isNewBssid = !ssidToBssids[ssid].has(bssid);
            ssidToBssids[ssid].add(bssid);

            // If we have more than one BSSID for this SSID, it could be rogue (or just multiple APs)
            if (ssidToBssids[ssid].size > 1 && isNewBssid) {
                rogueApCount++;
                document.getElementById('rogueApCount').textContent = rogueApCount;
                playAlert();

                // Get the BSSIDs to show in alert
                const bssidList = rogueApDetails[ssid].map(e => e.bssid).join(', ');
                showInfo(`⚠ Rogue AP: "${ssid}" has ${ssidToBssids[ssid].size} BSSIDs: ${bssidList}`);
                showNotification('⚠️ Rogue AP Detected!', `"${ssid}" on multiple BSSIDs`);
                return true;
            }
            return false;
        }

        // Show rogue AP details popup
        function showRogueApDetails() {
            const rogueSSIDs = Object.keys(rogueApDetails).filter(ssid =>
                rogueApDetails[ssid].length > 1
            );

            if (rogueSSIDs.length === 0) {
                showInfo('No rogue APs detected. Rogue AP = same SSID on multiple BSSIDs.');
                return;
            }

            // Remove existing popup if any
            const existing = document.getElementById('rogueApPopup');
            if (existing) existing.remove();

            // Build details HTML
            let html = '<div style="max-height: 300px; overflow-y: auto;">';
            rogueSSIDs.forEach(ssid => {
                const aps = rogueApDetails[ssid];
                html += `<div style="margin-bottom: 12px;">
                    <div style="color: var(--accent-red); font-weight: bold; margin-bottom: 4px;">
                        📡 "${ssid}" (${aps.length} BSSIDs)
                    </div>
                    <table style="width: 100%; font-size: 10px; border-collapse: collapse;">
                        <tr style="color: var(--text-dim);">
                            <th style="text-align: left; padding: 2px 8px;">BSSID</th>
                            <th style="text-align: left; padding: 2px 8px;">CH</th>
                            <th style="text-align: left; padding: 2px 8px;">Signal</th>
                            <th style="text-align: left; padding: 2px 8px;">First Seen</th>
                        </tr>`;
                aps.forEach((ap, idx) => {
                    const bgColor = idx % 2 === 0 ? 'rgba(255,255,255,0.05)' : 'transparent';
                    html += `<tr style="background: ${bgColor};">
                        <td style="padding: 2px 8px; font-family: monospace;">${ap.bssid}</td>
                        <td style="padding: 2px 8px;">${ap.channel}</td>
                        <td style="padding: 2px 8px;">${ap.signal} dBm</td>
                        <td style="padding: 2px 8px;">${ap.firstSeen}</td>
                    </tr>`;
                });
                html += '</table></div>';
            });
            html += '</div>';
            html += '<div style="margin-top: 8px; font-size: 9px; color: var(--text-dim);">⚠ Multiple BSSIDs for same SSID may indicate rogue AP or legitimate multi-AP setup</div>';

            // Create popup
            const popup = document.createElement('div');
            popup.id = 'rogueApPopup';
            popup.style.cssText = `
                position: fixed;
                top: 50%;
                left: 50%;
                transform: translate(-50%, -50%);
                background: var(--bg-primary);
                border: 1px solid var(--accent-red);
                border-radius: 8px;
                padding: 16px;
                z-index: 10000;
                min-width: 400px;
                max-width: 600px;
                box-shadow: 0 4px 20px rgba(0,0,0,0.5);
            `;
            popup.innerHTML = `
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
                    <span style="font-weight: bold; color: var(--accent-red);">🚨 Rogue AP Details</span>
                    <button onclick="this.parentElement.parentElement.remove()"
                            style="background: none; border: none; color: var(--text-dim); cursor: pointer; font-size: 16px;">✕</button>
                </div>
                ${html}
            `;

            document.body.appendChild(popup);
        }

        // Show drone details popup
        function showDroneDetails() {
            const drones = Object.values(detectedDrones);

            if (drones.length === 0) {
                showInfo('No drones detected. Drones are identified by SSID patterns and manufacturer OUI.');
                return;
            }

            // Remove existing popup if any
            const existing = document.getElementById('droneDetailsPopup');
            if (existing) existing.remove();

            // Build details HTML
            let html = '<div style="max-height: 300px; overflow-y: auto;">';
            html += `<table style="width: 100%; font-size: 10px; border-collapse: collapse;">
                <tr style="color: var(--text-dim);">
                    <th style="text-align: left; padding: 4px 8px;">Brand</th>
                    <th style="text-align: left; padding: 4px 8px;">SSID</th>
                    <th style="text-align: left; padding: 4px 8px;">BSSID</th>
                    <th style="text-align: left; padding: 4px 8px;">CH</th>
                    <th style="text-align: left; padding: 4px 8px;">Signal</th>
                    <th style="text-align: left; padding: 4px 8px;">Distance</th>
                    <th style="text-align: left; padding: 4px 8px;">Detected</th>
                </tr>`;

            drones.forEach((drone, idx) => {
                const bgColor = idx % 2 === 0 ? 'rgba(255,165,0,0.1)' : 'transparent';
                const rssi = parseInt(drone.signal) || -70;
                const distance = estimateDroneDistance(rssi);
                const timeStr = new Date(drone.firstSeen).toLocaleTimeString();
                html += `<tr style="background: ${bgColor};">
                    <td style="padding: 4px 8px; font-weight: bold; color: var(--accent-orange);">${drone.brand || 'Unknown'}</td>
                    <td style="padding: 4px 8px;">${drone.ssid || '[Hidden]'}</td>
                    <td style="padding: 4px 8px; font-family: monospace; font-size: 9px;">${drone.bssid}</td>
                    <td style="padding: 4px 8px;">${drone.channel || '?'}</td>
                    <td style="padding: 4px 8px;">${drone.signal || '?'} dBm</td>
                    <td style="padding: 4px 8px;">~${distance}m</td>
                    <td style="padding: 4px 8px;">${timeStr}</td>
                </tr>`;
            });
            html += '</table></div>';
            html += '<div style="margin-top: 8px; font-size: 9px; color: var(--text-dim);">Detection via: SSID pattern matching and manufacturer OUI lookup</div>';

            // Create popup
            const popup = document.createElement('div');
            popup.id = 'droneDetailsPopup';
            popup.style.cssText = `
                position: fixed;
                top: 50%;
                left: 50%;
                transform: translate(-50%, -50%);
                background: var(--bg-primary);
                border: 1px solid var(--accent-orange);
                border-radius: 8px;
                padding: 16px;
                z-index: 10000;
                min-width: 500px;
                max-width: 700px;
                box-shadow: 0 4px 20px rgba(0,0,0,0.5);
            `;
            popup.innerHTML = `
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
                    <span style="font-weight: bold; color: var(--accent-orange);">🚁 Detected Drones (${drones.length})</span>
                    <button onclick="this.parentElement.parentElement.remove()"
                            style="background: none; border: none; color: var(--text-dim); cursor: pointer; font-size: 16px;">✕</button>
                </div>
                ${html}
            `;

            document.body.appendChild(popup);
        }

        // Update 5GHz channel graph
        function updateChannel5gGraph() {
            const bars = document.querySelectorAll('#channelGraph5g .channel-bar');
            const labels = document.querySelectorAll('#channelGraph5g .channel-label');

            // Count networks per 5GHz channel
            const channelCounts = {};
            channels5g.forEach(ch => channelCounts[ch] = 0);

            Object.values(wifiNetworks).forEach(net => {
                const ch = net.channel?.toString().trim();
                if (channels5g.includes(ch)) {
                    channelCounts[ch]++;
                }
            });

            const maxCount = Math.max(1, ...Object.values(channelCounts));

            bars.forEach((bar, i) => {
                const ch = channels5g[i];
                const count = channelCounts[ch] || 0;
                const height = Math.max(2, (count / maxCount) * 50);
                bar.style.height = height + 'px';
                bar.className = 'channel-bar' + (count > 0 ? ' active' : '') + (count > 3 ? ' congested' : '') + (count > 5 ? ' very-congested' : '');
            });
        }

        // ============== NEW FEATURES ==============

        // Signal History Graph
        let signalHistory = {};  // {mac: [{time, signal}]}
        let trackedDevice = null;
        const maxSignalPoints = 60;

        function trackDeviceSignal(mac, signal) {
            if (!signalHistory[mac]) {
                signalHistory[mac] = [];
            }
            signalHistory[mac].push({
                time: Date.now(),
                signal: parseInt(signal) || -100
            });
            // Keep only last N points
            if (signalHistory[mac].length > maxSignalPoints) {
                signalHistory[mac].shift();
            }
            // Update graph if this is the tracked device
            if (trackedDevice === mac) {
                drawSignalGraph();
            }
        }

        function setTrackedDevice(mac, name) {
            trackedDevice = mac;
            document.getElementById('signalGraphDevice').textContent = name || mac;
            drawSignalGraph();
        }

        function drawSignalGraph() {
            const canvas = document.getElementById('signalGraph');
            if (!canvas) return;
            const ctx = canvas.getContext('2d');
            const width = canvas.offsetWidth;
            const height = canvas.offsetHeight;
            canvas.width = width;
            canvas.height = height;

            // Clear
            ctx.fillStyle = '#000';
            ctx.fillRect(0, 0, width, height);

            // Draw grid
            ctx.strokeStyle = '#1a1a1a';
            ctx.lineWidth = 1;
            for (let i = 0; i <= 4; i++) {
                const y = (height / 4) * i;
                ctx.beginPath();
                ctx.moveTo(0, y);
                ctx.lineTo(width, y);
                ctx.stroke();
            }

            // Draw dBm labels
            ctx.fillStyle = '#444';
            ctx.font = '9px monospace';
            ctx.fillText('-30', 2, 12);
            ctx.fillText('-60', 2, height/2);
            ctx.fillText('-90', 2, height - 4);

            if (!trackedDevice || !signalHistory[trackedDevice] || signalHistory[trackedDevice].length < 2) {
                ctx.fillStyle = '#444';
                ctx.font = '12px sans-serif';
                ctx.fillText('Click a device to track signal', width/2 - 80, height/2);
                return;
            }

            const data = signalHistory[trackedDevice];
            const stepX = width / (maxSignalPoints - 1);

            // Draw signal line
            ctx.beginPath();
            ctx.strokeStyle = '#00d4ff';
            ctx.lineWidth = 2;

            data.forEach((point, i) => {
                // Map signal from -30 to -90 dBm to canvas height
                const normalizedSignal = Math.max(-90, Math.min(-30, point.signal));
                const y = height - ((normalizedSignal + 90) / 60) * height;
                const x = i * stepX;

                if (i === 0) {
                    ctx.moveTo(x, y);
                } else {
                    ctx.lineTo(x, y);
                }
            });
            ctx.stroke();

            // Draw glow effect
            ctx.shadowBlur = 10;
            ctx.shadowColor = '#00d4ff';
            ctx.stroke();
            ctx.shadowBlur = 0;

            // Draw current value
            const lastSignal = data[data.length - 1].signal;
            ctx.fillStyle = '#00d4ff';
            ctx.font = 'bold 14px monospace';
            ctx.fillText(lastSignal + ' dBm', width - 70, 20);
        }

        // Network Topology Graph
        function drawNetworkGraph() {
            const canvas = document.getElementById('networkGraph');
            if (!canvas) return;
            const ctx = canvas.getContext('2d');
            const width = canvas.offsetWidth;
            const height = canvas.offsetHeight;
            canvas.width = width;
            canvas.height = height;

            // Clear
            ctx.fillStyle = '#000';
            ctx.fillRect(0, 0, width, height);

            const networks = Object.values(wifiNetworks);
            const clients = Object.values(wifiClients);

            if (networks.length === 0) {
                ctx.fillStyle = '#444';
                ctx.font = '12px sans-serif';
                ctx.fillText('Start scanning to see network topology', width/2 - 100, height/2);
                return;
            }

            // Calculate positions for APs (top row)
            const apPositions = {};
            const apSpacing = width / (networks.length + 1);
            networks.forEach((net, i) => {
                apPositions[net.bssid] = {
                    x: apSpacing * (i + 1),
                    y: 40,
                    ssid: net.essid,
                    isDrone: isDrone(net.essid, net.bssid).isDrone
                };
            });

            // Draw connections from clients to APs
            ctx.strokeStyle = '#1a1a1a';
            ctx.lineWidth = 1;
            clients.forEach(client => {
                if (client.ap && apPositions[client.ap]) {
                    const ap = apPositions[client.ap];
                    const clientY = 120 + (Math.random() * 60);
                    const clientX = ap.x + (Math.random() - 0.5) * 80;

                    ctx.beginPath();
                    ctx.moveTo(ap.x, ap.y + 15);
                    ctx.lineTo(clientX, clientY - 10);
                    ctx.stroke();

                    // Draw client node
                    ctx.beginPath();
                    ctx.arc(clientX, clientY, 6, 0, Math.PI * 2);
                    ctx.fillStyle = '#00ff88';
                    ctx.fill();
                }
            });

            // Draw AP nodes
            Object.entries(apPositions).forEach(([bssid, pos]) => {
                ctx.beginPath();
                ctx.arc(pos.x, pos.y, 12, 0, Math.PI * 2);
                ctx.fillStyle = pos.isDrone ? '#ff8800' : '#00d4ff';
                ctx.fill();

                // Draw label
                ctx.fillStyle = '#888';
                ctx.font = '9px sans-serif';
                ctx.textAlign = 'center';
                const label = (pos.ssid || 'Hidden').substring(0, 12);
                ctx.fillText(label, pos.x, pos.y + 25);
            });

            ctx.textAlign = 'left';
        }

        // Channel Recommendation
        function updateChannelRecommendation() {
            const channelCounts24 = {};
            const channelCounts5 = {};

            // Initialize
            for (let i = 1; i <= 13; i++) channelCounts24[i] = 0;
            channels5g.forEach(ch => channelCounts5[ch] = 0);

            // Count networks per channel
            Object.values(wifiNetworks).forEach(net => {
                const ch = parseInt(net.channel);
                if (ch >= 1 && ch <= 13) {
                    // 2.4 GHz channels overlap, so count neighbors too
                    for (let i = Math.max(1, ch - 2); i <= Math.min(13, ch + 2); i++) {
                        channelCounts24[i] = (channelCounts24[i] || 0) + (i === ch ? 1 : 0.5);
                    }
                } else if (channels5g.includes(ch.toString())) {
                    channelCounts5[ch.toString()]++;
                }
            });

            // Find best 2.4 GHz channel (1, 6, or 11 preferred)
            const preferred24 = [1, 6, 11];
            let best24 = 1;
            let minCount24 = Infinity;
            preferred24.forEach(ch => {
                if (channelCounts24[ch] < minCount24) {
                    minCount24 = channelCounts24[ch];
                    best24 = ch;
                }
            });

            // Find best 5 GHz channel
            let best5 = '36';
            let minCount5 = Infinity;
            channels5g.forEach(ch => {
                if (channelCounts5[ch] < minCount5) {
                    minCount5 = channelCounts5[ch];
                    best5 = ch;
                }
            });

            // Update UI
            document.getElementById('rec24Channel').textContent = best24;
            document.getElementById('rec24Reason').textContent =
                minCount24 === 0 ? '(unused)' : `(${Math.round(minCount24)} networks nearby)`;

            document.getElementById('rec5Channel').textContent = best5;
            document.getElementById('rec5Reason').textContent =
                minCount5 === 0 ? '(unused)' : `(${minCount5} networks)`;
        }

        // Device Correlation (WiFi <-> Bluetooth)
        let deviceCorrelations = [];

        function correlateDevices() {
            deviceCorrelations = [];
            const wifiMacs = Object.keys(wifiNetworks).concat(Object.keys(wifiClients));
            const btMacs = Object.keys(btDevices || {});

            // Compare OUI prefixes
            wifiMacs.forEach(wifiMac => {
                const wifiOui = wifiMac.substring(0, 8).toUpperCase();
                btMacs.forEach(btMac => {
                    const btOui = btMac.substring(0, 8).toUpperCase();
                    if (wifiOui === btOui) {
                        const wifiDev = wifiNetworks[wifiMac] || wifiClients[wifiMac];
                        const btDev = btDevices[btMac];
                        deviceCorrelations.push({
                            wifiMac: wifiMac,
                            btMac: btMac,
                            wifiName: wifiDev?.essid || wifiDev?.mac || wifiMac,
                            btName: btDev?.name || btMac,
                            manufacturer: getManufacturer(wifiOui)
                        });
                    }
                });
            });

            updateCorrelationDisplay();
        }

        function getManufacturer(oui) {
            // Simple lookup - would be expanded
            const lookup = {
                '00:25:DB': 'Apple', 'AC:BC:32': 'Apple', '3C:22:FB': 'Apple',
                '8C:71:F8': 'Samsung', 'C4:73:1E': 'Samsung',
                '54:60:09': 'Google', 'F4:F5:D8': 'Google'
            };
            return lookup[oui] || 'Unknown';
        }

        function updateCorrelationDisplay() {
            const list = document.getElementById('correlationList');
            if (!list) return;

            if (deviceCorrelations.length === 0) {
                list.innerHTML = '<div style="color: var(--text-dim);">No correlated devices found yet</div>';
                return;
            }

            list.innerHTML = deviceCorrelations.map(c => `
                <div style="padding: 4px 0; border-bottom: 1px solid var(--border-color);">
                    <span style="color: var(--accent-cyan);">📶 ${c.wifiName}</span>
                    <span style="color: var(--text-dim);"> ↔ </span>
                    <span style="color: #6495ED;">🔵 ${c.btName}</span>
                    <span class="correlation-badge">${c.manufacturer}</span>
                </div>
            `).join('');
        }

        // Hidden SSID Revealer
        let revealedSsids = {};  // {bssid: ssid}

        function revealHiddenSsid(bssid, ssid) {
            if (ssid && ssid !== '' && ssid !== 'Hidden' && ssid !== '[Hidden]') {
                if (!revealedSsids[bssid]) {
                    revealedSsids[bssid] = ssid;
                    updateHiddenSsidDisplay();
                    showNotification('Hidden SSID Revealed', `"${ssid}" on ${bssid}`);
                }
            }
        }

        function updateHiddenSsidDisplay() {
            const list = document.getElementById('hiddenSsidList');
            if (!list) return;

            const entries = Object.entries(revealedSsids);
            if (entries.length === 0) {
                list.innerHTML = '<div style="color: var(--text-dim);">No hidden SSIDs revealed yet</div>';
                return;
            }

            list.innerHTML = entries.map(([bssid, ssid]) => `
                <div style="padding: 4px 0; border-bottom: 1px solid var(--border-color);">
                    <span class="hidden-ssid-revealed">"${ssid}"</span>
                    <span style="color: var(--text-dim); font-size: 9px;"> (${bssid})</span>
                </div>
            `).join('');
        }

        // Browser Notifications
        let notificationsEnabled = false;

        function requestNotificationPermission() {
            if ('Notification' in window) {
                Notification.requestPermission().then(permission => {
                    notificationsEnabled = permission === 'granted';
                    if (notificationsEnabled) {
                        showInfo('🔔 Desktop notifications enabled');
                    }
                });
            }
        }

        function showNotification(title, body) {
            if (notificationsEnabled && document.hidden) {
                new Notification(title, {
                    body: body,
                    icon: '/favicon.ico',
                    tag: 'intercept-' + Date.now()
                });
            }
        }

        // Request notification permission on load
        if ('Notification' in window && Notification.permission === 'default') {
            // Will request on first interaction
            document.addEventListener('click', function requestOnce() {
                requestNotificationPermission();
                document.removeEventListener('click', requestOnce);
            }, { once: true });
        } else if (Notification.permission === 'granted') {
            notificationsEnabled = true;
        }

        // Update visualizations periodically
        setInterval(() => {
            if (currentMode === 'wifi') {
                drawSignalGraph();
                drawNetworkGraph();
                updateChannelRecommendation();
                correlateDevices();
            }
        }, 2000);

        // Refresh WiFi interfaces
        function refreshWifiInterfaces() {
            fetch('/wifi/interfaces')
                .then(r => r.json())
                .then(data => {
                    const select = document.getElementById('wifiInterfaceSelect');
                    if (data.interfaces.length === 0) {
                        select.innerHTML = '<option value="">No WiFi interfaces found</option>';
                    } else {
                        select.innerHTML = data.interfaces.map(i =>
                            `<option value="${i.name}">${i.name} (${i.type})${i.monitor_capable ? ' [Monitor OK]' : ''}</option>`
                        ).join('');
                    }

                    // Update tool status
                    const statusDiv = document.getElementById('wifiToolStatus');
                    statusDiv.innerHTML = `
                        <span>airmon-ng:</span><span class="tool-status ${data.tools.airmon ? 'ok' : 'missing'}">${data.tools.airmon ? 'OK' : 'Missing'}</span>
                        <span>airodump-ng:</span><span class="tool-status ${data.tools.airodump ? 'ok' : 'missing'}">${data.tools.airodump ? 'OK' : 'Missing'}</span>
                    `;

                    // Update monitor status
                    if (data.monitor_interface) {
                        monitorInterface = data.monitor_interface;
                        updateMonitorStatus(true);
                    }
                });
        }

        // Enable monitor mode
        function enableMonitorMode() {
            const iface = document.getElementById('wifiInterfaceSelect').value;
            if (!iface) {
                alert('Please select an interface');
                return;
            }

            const killProcesses = document.getElementById('killProcesses').checked;

            // Show loading state
            const btn = document.getElementById('monitorStartBtn');
            const originalText = btn.textContent;
            btn.textContent = 'Enabling...';
            btn.disabled = true;

            fetch('/wifi/monitor', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({interface: iface, action: 'start', kill_processes: killProcesses})
            }).then(r => r.json())
              .then(data => {
                  btn.textContent = originalText;
                  btn.disabled = false;

                  if (data.status === 'success') {
                      monitorInterface = data.monitor_interface;
                      updateMonitorStatus(true);
                      showInfo('Monitor mode enabled on ' + monitorInterface + ' - Ready to scan!');

                      // Refresh interface list and auto-select the monitor interface
                      fetch('/wifi/interfaces')
                          .then(r => r.json())
                          .then(ifaceData => {
                              const select = document.getElementById('wifiInterfaceSelect');
                              if (ifaceData.interfaces.length > 0) {
                                  select.innerHTML = ifaceData.interfaces.map(i =>
                                      `<option value="${i.name}" ${i.name === monitorInterface ? 'selected' : ''}>${i.name} (${i.type})${i.monitor_capable ? ' [Monitor OK]' : ''}</option>`
                                  ).join('');
                              }
                          });
                  } else {
                      alert('Error: ' + data.message);
                  }
              })
              .catch(err => {
                  btn.textContent = originalText;
                  btn.disabled = false;
                  alert('Error: ' + err.message);
              });
        }

        // Disable monitor mode
        function disableMonitorMode() {
            const iface = monitorInterface || document.getElementById('wifiInterfaceSelect').value;

            fetch('/wifi/monitor', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({interface: iface, action: 'stop'})
            }).then(r => r.json())
              .then(data => {
                  if (data.status === 'success') {
                      monitorInterface = null;
                      updateMonitorStatus(false);
                      showInfo('Monitor mode disabled');
                  } else {
                      alert('Error: ' + data.message);
                  }
              });
        }

        function updateMonitorStatus(enabled) {
            document.getElementById('monitorStartBtn').style.display = enabled ? 'none' : 'block';
            document.getElementById('monitorStopBtn').style.display = enabled ? 'block' : 'none';
            document.getElementById('monitorStatus').innerHTML = enabled
                ? 'Monitor mode: <span style="color: var(--accent-green);">Active (' + monitorInterface + ')</span>'
                : 'Monitor mode: <span style="color: var(--accent-red);">Inactive</span>';
        }

        // Start WiFi scan
        function startWifiScan() {
            const band = document.getElementById('wifiBand').value;
            const channel = document.getElementById('wifiChannel').value;

            if (!monitorInterface) {
                alert('Enable monitor mode first');
                return;
            }

            fetch('/wifi/scan/start', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    interface: monitorInterface,
                    band: band,
                    channel: channel || null
                })
            }).then(r => r.json())
              .then(data => {
                  if (data.status === 'started') {
                      setWifiRunning(true);
                      startWifiStream();
                  } else {
                      alert('Error: ' + data.message);
                  }
              });
        }

        // Stop WiFi scan
        function stopWifiScan() {
            fetch('/wifi/scan/stop', {method: 'POST'})
                .then(r => r.json())
                .then(data => {
                    setWifiRunning(false);
                    if (wifiEventSource) {
                        wifiEventSource.close();
                        wifiEventSource = null;
                    }
                });
        }

        function setWifiRunning(running) {
            isWifiRunning = running;
            document.getElementById('statusDot').classList.toggle('running', running);
            document.getElementById('statusText').textContent = running ? 'Scanning...' : 'Idle';
            document.getElementById('startWifiBtn').style.display = running ? 'none' : 'block';
            document.getElementById('stopWifiBtn').style.display = running ? 'block' : 'none';
        }

        // Start WiFi event stream
        function startWifiStream() {
            if (wifiEventSource) {
                wifiEventSource.close();
            }

            wifiEventSource = new EventSource('/wifi/stream');

            wifiEventSource.onmessage = function(e) {
                const data = JSON.parse(e.data);

                if (data.type === 'network') {
                    handleWifiNetwork(data);
                } else if (data.type === 'client') {
                    handleWifiClient(data);
                } else if (data.type === 'info' || data.type === 'raw') {
                    showInfo(data.text);
                } else if (data.type === 'error') {
                    showError(data.text);
                } else if (data.type === 'status') {
                    if (data.text === 'stopped') {
                        setWifiRunning(false);
                    }
                }
            };

            wifiEventSource.onerror = function() {
                console.error('WiFi stream error');
            };
        }

        // Handle discovered WiFi network
        function handleWifiNetwork(net) {
            const isNew = !wifiNetworks[net.bssid];
            wifiNetworks[net.bssid] = net;

            // Track signal history for graphs
            trackDeviceSignal(net.bssid, net.power);

            // Check if this reveals a hidden SSID
            if (net.essid && net.essid !== 'Hidden' && net.essid !== '[Hidden]') {
                revealHiddenSsid(net.bssid, net.essid);
            }

            if (isNew) {
                apCount++;
                document.getElementById('apCount').textContent = apCount;
                playAlert();
                pulseSignal();

                // Check for rogue AP (same SSID, different BSSID)
                checkRogueAP(net.essid, net.bssid, net.channel, net.power);

                // Check proximity watch list
                checkWatchList(net.bssid, 'AP');

                // Check for drone
                const droneCheck = isDrone(net.essid, net.bssid);
                if (droneCheck.isDrone) {
                    handleDroneDetection(net, droneCheck);
                    showNotification('🚁 Drone Detected!', `${droneCheck.brand}: ${net.essid}`);
                }
            }

            // Update recon display
            const droneInfo = isDrone(net.essid, net.bssid);
            trackDevice({
                protocol: droneInfo.isDrone ? 'DRONE' : 'WiFi-AP',
                address: net.bssid,
                message: net.essid || '[Hidden SSID]',
                model: net.essid,
                channel: net.channel,
                privacy: net.privacy,
                isDrone: droneInfo.isDrone,
                droneBrand: droneInfo.brand
            });

            // Add to output
            addWifiNetworkCard(net, isNew);

            // Update both channel graphs
            updateChannelGraph();
            updateChannel5gGraph();
        }

        // Handle discovered WiFi client
        function handleWifiClient(client) {
            const isNew = !wifiClients[client.mac];
            wifiClients[client.mac] = client;

            // Track signal history for graphs
            trackDeviceSignal(client.mac, client.power);

            if (isNew) {
                clientCount++;
                document.getElementById('clientCount').textContent = clientCount;

                // Check proximity watch list
                checkWatchList(client.mac, 'Client');
            }

            // Track in device intelligence with vendor info
            const vendorInfo = client.vendor && client.vendor !== 'Unknown' ? ` [${client.vendor}]` : '';
            trackDevice({
                protocol: 'WiFi-Client',
                address: client.mac,
                message: (client.probes || '[No probes]') + vendorInfo,
                bssid: client.bssid,
                vendor: client.vendor
            });

            // Update probe analysis
            updateProbeAnalysis();
        }

        // Update client probe analysis panel
        function updateProbeAnalysis() {
            const list = document.getElementById('probeAnalysisList');
            if (!list) return;

            const clientsWithProbes = Object.values(wifiClients).filter(c => c.probes && c.probes.trim());
            const allProbes = new Set();
            let privacyLeaks = 0;

            // Count unique probes and privacy leaks
            clientsWithProbes.forEach(client => {
                const probes = client.probes.split(',').map(p => p.trim()).filter(p => p);
                probes.forEach(p => allProbes.add(p));

                // Check for sensitive network names (home networks, corporate, etc.)
                probes.forEach(probe => {
                    const lowerProbe = probe.toLowerCase();
                    if (lowerProbe.includes('home') || lowerProbe.includes('office') ||
                        lowerProbe.includes('corp') || lowerProbe.includes('work') ||
                        lowerProbe.includes('private') || lowerProbe.includes('hotel') ||
                        lowerProbe.includes('airport') || lowerProbe.match(/^[a-z]+-[a-z]+$/i)) {
                        privacyLeaks++;
                    }
                });
            });

            // Update counters
            document.getElementById('probeClientCount').textContent = clientsWithProbes.length;
            document.getElementById('probeSSIDCount').textContent = allProbes.size;
            document.getElementById('probePrivacyCount').textContent = privacyLeaks;

            if (clientsWithProbes.length === 0) {
                list.innerHTML = '<div style="color: var(--text-dim);">Waiting for client probe requests...</div>';
                return;
            }

            // Sort by number of probes (most revealing first)
            clientsWithProbes.sort((a, b) => {
                const aCount = (a.probes || '').split(',').length;
                const bCount = (b.probes || '').split(',').length;
                return bCount - aCount;
            });

            let html = '<div style="display: flex; flex-direction: column; gap: 8px;">';

            clientsWithProbes.forEach(client => {
                const probes = client.probes.split(',').map(p => p.trim()).filter(p => p);
                const vendorBadge = client.vendor && client.vendor !== 'Unknown'
                    ? `<span style="background: var(--bg-tertiary); padding: 1px 4px; border-radius: 2px; font-size: 9px; margin-left: 5px;">${escapeHtml(client.vendor)}</span>`
                    : '';

                // Check for privacy-revealing probes
                const probeHtml = probes.map(probe => {
                    const lowerProbe = probe.toLowerCase();
                    const isSensitive = lowerProbe.includes('home') || lowerProbe.includes('office') ||
                        lowerProbe.includes('corp') || lowerProbe.includes('work') ||
                        lowerProbe.includes('private') || lowerProbe.includes('hotel') ||
                        lowerProbe.includes('airport') || lowerProbe.match(/^[a-z]+-[a-z]+$/i);

                    const style = isSensitive
                        ? 'background: var(--accent-orange); color: #000; padding: 1px 4px; border-radius: 2px; margin: 1px;'
                        : 'background: var(--bg-tertiary); padding: 1px 4px; border-radius: 2px; margin: 1px;';

                    return `<span style="${style}" title="${isSensitive ? 'Potentially sensitive - reveals user location history' : ''}">${escapeHtml(probe)}</span>`;
                }).join(' ');

                html += `
                    <div style="border-left: 2px solid var(--accent-cyan); padding-left: 8px;">
                        <div style="display: flex; align-items: center; gap: 5px; margin-bottom: 3px;">
                            <span style="color: var(--accent-cyan); font-family: monospace; font-size: 10px;">${escapeHtml(client.mac)}</span>
                            ${vendorBadge}
                            <span style="color: var(--text-dim); font-size: 9px;">(${probes.length} probe${probes.length !== 1 ? 's' : ''})</span>
                        </div>
                        <div style="display: flex; flex-wrap: wrap; gap: 2px; font-size: 10px;">
                            ${probeHtml}
                        </div>
                    </div>
                `;
            });

            html += '</div>';
            list.innerHTML = html;
        }

        // Add WiFi network card to output
        function addWifiNetworkCard(net, isNew) {
            const output = document.getElementById('output');
            const placeholder = output.querySelector('.placeholder');
            if (placeholder) placeholder.remove();

            // Check if card already exists
            let card = document.getElementById('wifi_' + net.bssid.replace(/:/g, ''));

            if (!card) {
                card = document.createElement('div');
                card.id = 'wifi_' + net.bssid.replace(/:/g, '');
                card.className = 'sensor-card';
                card.style.borderLeftColor = net.privacy.includes('WPA') ? 'var(--accent-orange)' :
                                             net.privacy.includes('WEP') ? 'var(--accent-red)' :
                                             'var(--accent-green)';
                output.insertBefore(card, output.firstChild);
            }

            const signalStrength = parseInt(net.power) || -100;
            const signalBars = Math.max(0, Math.min(5, Math.floor((signalStrength + 100) / 15)));

            const wpsEnabled = net.wps === '1' || net.wps === 'Yes' || (net.privacy || '').includes('WPS');
            const wpsHtml = wpsEnabled ? '<span class="wps-enabled">WPS</span>' : '';

            card.innerHTML = `
                <div class="header" style="display: flex; justify-content: space-between; margin-bottom: 8px;">
                    <span class="device-name">${escapeHtml(net.essid || '[Hidden]')}${wpsHtml}</span>
                    <span style="color: #444; font-size: 10px;">CH ${net.channel}</span>
                </div>
                <div class="sensor-data">
                    <div class="data-item">
                        <div class="data-label">BSSID</div>
                        <div class="data-value" style="font-size: 11px;">${escapeHtml(net.bssid)}</div>
                    </div>
                    <div class="data-item">
                        <div class="data-label">Security</div>
                        <div class="data-value" style="color: ${(net.privacy || '').includes('WPA') ? 'var(--accent-orange)' : net.privacy === 'OPN' ? 'var(--accent-green)' : 'var(--accent-red)'}">${escapeHtml(net.privacy || '')}</div>
                    </div>
                    <div class="data-item">
                        <div class="data-label">Signal</div>
                        <div class="data-value">${net.power} dBm ${'█'.repeat(signalBars)}${'░'.repeat(5-signalBars)}</div>
                    </div>
                    <div class="data-item">
                        <div class="data-label">Beacons</div>
                        <div class="data-value">${net.beacons}</div>
                    </div>
                </div>
                <div style="margin-top: 8px; display: flex; gap: 5px; flex-wrap: wrap;">
                    <button class="preset-btn" onclick="targetNetwork('${escapeAttr(net.bssid)}', '${escapeAttr(net.channel)}')" style="font-size: 10px; padding: 4px 8px;">Target</button>
                    <button class="preset-btn" onclick="captureHandshake('${escapeAttr(net.bssid)}', '${escapeAttr(net.channel)}')" style="font-size: 10px; padding: 4px 8px; border-color: var(--accent-orange); color: var(--accent-orange);">4-Way</button>
                    <button class="preset-btn pmkid-btn" onclick="capturePmkid('${escapeAttr(net.bssid)}', '${escapeAttr(net.channel)}')" style="font-size: 10px; padding: 4px 8px;">PMKID</button>
                    <button class="preset-btn" onclick="setTrackedDevice('${escapeAttr(net.bssid)}', '${escapeAttr(net.essid || net.bssid)}')" style="font-size: 10px; padding: 4px 8px; border-color: var(--accent-cyan); color: var(--accent-cyan);" title="Track signal strength">📈</button>
                </div>
            `;

            if (autoScroll) output.scrollTop = 0;
        }

        // Target a network for attack
        function targetNetwork(bssid, channel) {
            document.getElementById('targetBssid').value = bssid;
            document.getElementById('wifiChannel').value = channel;
            showInfo('Targeted: ' + bssid + ' on channel ' + channel);
        }

        // Start handshake capture
        function captureHandshake(bssid, channel) {
            if (!confirm('Start handshake capture for ' + bssid + '? This will stop the current scan.')) {
                return;
            }

            fetch('/wifi/handshake/capture', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({bssid: bssid, channel: channel})
            }).then(r => r.json())
              .then(data => {
                  if (data.status === 'started') {
                      showInfo('🎯 Capturing handshakes for ' + bssid);
                      setWifiRunning(true);

                      // Update handshake indicator to show active capture
                      const hsSpan = document.getElementById('handshakeCount');
                      hsSpan.style.animation = 'pulse 1s infinite';
                      hsSpan.title = 'Capturing: ' + bssid;

                      // Show capture status panel
                      const panel = document.getElementById('captureStatusPanel');
                      panel.style.display = 'block';
                      document.getElementById('captureTargetBssid').textContent = bssid;
                      document.getElementById('captureTargetChannel').textContent = channel;
                      document.getElementById('captureFilePath').textContent = data.capture_file;
                      document.getElementById('captureStatus').textContent = 'Waiting for handshake...';
                      document.getElementById('captureStatus').style.color = 'var(--accent-orange)';

                      // Store active capture info and start polling
                      activeCapture = {
                          bssid: bssid,
                          channel: channel,
                          file: data.capture_file,
                          startTime: Date.now(),
                          pollInterval: setInterval(checkCaptureStatus, 5000)  // Check every 5 seconds
                      };
                  } else {
                      alert('Error: ' + data.message);
                  }
              });
        }

        // Check handshake capture status
        function checkCaptureStatus() {
            if (!activeCapture) {
                showInfo('No active handshake capture');
                return;
            }

            fetch('/wifi/handshake/status', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({file: activeCapture.file, bssid: activeCapture.bssid})
            }).then(r => r.json())
              .then(data => {
                  const statusSpan = document.getElementById('captureStatus');
                  const elapsed = Math.round((Date.now() - activeCapture.startTime) / 1000);
                  const elapsedStr = elapsed < 60 ? elapsed + 's' : Math.floor(elapsed/60) + 'm ' + (elapsed%60) + 's';

                  if (data.handshake_found) {
                      // Handshake captured!
                      statusSpan.textContent = '✓ HANDSHAKE CAPTURED!';
                      statusSpan.style.color = 'var(--accent-green)';
                      handshakeCount++;
                      document.getElementById('handshakeCount').textContent = handshakeCount;
                      playAlert();
                      showInfo('🎉 Handshake captured for ' + activeCapture.bssid + '! File: ' + data.file);
                      showNotification('🤝 Handshake Captured!', `Target: ${activeCapture.bssid}`);

                      // Stop polling
                      if (activeCapture.pollInterval) {
                          clearInterval(activeCapture.pollInterval);
                      }
                      document.getElementById('handshakeCount').style.animation = '';
                  } else if (data.file_exists) {
                      const sizeKB = (data.file_size / 1024).toFixed(1);
                      statusSpan.textContent = 'Capturing... (' + sizeKB + ' KB, ' + elapsedStr + ')';
                      statusSpan.style.color = 'var(--accent-orange)';
                  } else if (data.status === 'stopped') {
                      statusSpan.textContent = 'Capture stopped';
                      statusSpan.style.color = 'var(--text-dim)';
                      if (activeCapture.pollInterval) {
                          clearInterval(activeCapture.pollInterval);
                      }
                  } else {
                      statusSpan.textContent = 'Waiting for data... (' + elapsedStr + ')';
                      statusSpan.style.color = 'var(--accent-orange)';
                  }
              })
              .catch(err => {
                  console.error('Capture status check failed:', err);
              });
        }

        // Stop handshake capture
        function stopHandshakeCapture() {
            if (activeCapture && activeCapture.pollInterval) {
                clearInterval(activeCapture.pollInterval);
            }

            // Stop the WiFi scan (which stops airodump-ng)
            stopWifiScan();

            document.getElementById('captureStatus').textContent = 'Stopped';
            document.getElementById('captureStatus').style.color = 'var(--text-dim)';
            document.getElementById('handshakeCount').style.animation = '';

            // Keep the panel visible so user can see the file path
            showInfo('Handshake capture stopped. Check ' + (activeCapture ? activeCapture.file : 'capture file'));

            activeCapture = null;
        }

        // PMKID Capture
        let activePmkid = null;

        function capturePmkid(bssid, channel) {
            if (!confirm('Start PMKID capture for ' + bssid + '?\\n\\nThis uses hcxdumptool to capture PMKID without needing clients.\\n\\n⚠ Only use on networks you own or have authorization to test!')) {
                return;
            }

            const iface = document.getElementById('wifiInterfaceSelect').value;

            fetch('/wifi/pmkid/capture', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ interface: iface, bssid: bssid, channel: channel })
            })
            .then(r => r.json())
            .then(data => {
                if (data.status === 'started') {
                    activePmkid = { bssid: bssid, file: data.file, startTime: Date.now() };
                    document.getElementById('pmkidPanel').style.display = 'block';
                    document.getElementById('pmkidTargetBssid').textContent = bssid;
                    document.getElementById('pmkidStatus').textContent = 'Capturing...';
                    document.getElementById('pmkidStatus').style.color = '#9933ff';
                    showInfo('PMKID capture started for ' + bssid);

                    // Poll for PMKID
                    activePmkid.pollInterval = setInterval(checkPmkidStatus, 3000);
                } else {
                    alert('Failed to start PMKID capture: ' + data.message);
                }
            });
        }

        function checkPmkidStatus() {
            if (!activePmkid) return;

            fetch('/wifi/pmkid/status', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ file: activePmkid.file })
            })
            .then(r => r.json())
            .then(data => {
                if (data.pmkid_found) {
                    document.getElementById('pmkidStatus').textContent = '✓ PMKID CAPTURED!';
                    document.getElementById('pmkidStatus').style.color = 'var(--accent-green)';
                    showInfo('🎉 PMKID captured! File: ' + data.file);
                    showNotification('🔐 PMKID Captured!', `Target: ${activePmkid.bssid}`);
                    clearInterval(activePmkid.pollInterval);
                } else {
                    const elapsed = Math.floor((Date.now() - activePmkid.startTime) / 1000);
                    document.getElementById('pmkidStatus').textContent = 'Scanning... (' + elapsed + 's)';
                }
            });
        }

        function stopPmkidCapture() {
            if (activePmkid && activePmkid.pollInterval) {
                clearInterval(activePmkid.pollInterval);
            }

            fetch('/wifi/pmkid/stop', { method: 'POST' })
            .then(() => {
                document.getElementById('pmkidStatus').textContent = 'Stopped';
                document.getElementById('pmkidStatus').style.color = 'var(--text-dim)';
                showInfo('PMKID capture stopped');
                activePmkid = null;
            });
        }

        // Beacon Flood Detection
        let beaconHistory = [];
        let lastBeaconCheck = Date.now();

        function checkBeaconFlood(networks) {
            const now = Date.now();
            const windowMs = 5000; // 5 second window

            // Add current networks to history
            beaconHistory.push({ time: now, count: Object.keys(networks).length });

            // Remove old entries
            beaconHistory = beaconHistory.filter(h => now - h.time < windowMs);

            // Calculate rate of new networks
            if (beaconHistory.length >= 2) {
                const oldest = beaconHistory[0];
                const newest = beaconHistory[beaconHistory.length - 1];
                const timeDiff = (newest.time - oldest.time) / 1000;
                const countDiff = newest.count - oldest.count;

                if (timeDiff > 0) {
                    const rate = countDiff / timeDiff;

                    // Alert if more than 10 new networks per second
                    if (rate > 10) {
                        document.getElementById('beaconFloodAlert').style.display = 'block';
                        document.getElementById('beaconFloodRate').textContent = rate.toFixed(1);
                        if (!muted) playAlertSound();
                    } else if (rate < 2) {
                        document.getElementById('beaconFloodAlert').style.display = 'none';
                    }
                }
            }
        }

        // Send deauth
        function sendDeauth() {
            const bssid = document.getElementById('targetBssid').value;
            const client = document.getElementById('targetClient').value || 'FF:FF:FF:FF:FF:FF';
            const count = document.getElementById('deauthCount').value || '5';

            if (!bssid) {
                alert('Enter target BSSID');
                return;
            }

            if (!confirm('Send ' + count + ' deauth packets to ' + bssid + '?\\n\\n⚠ Only use on networks you own or have authorization to test!')) {
                return;
            }

            fetch('/wifi/deauth', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({bssid: bssid, client: client, count: parseInt(count)})
            }).then(r => r.json())
              .then(data => {
                  if (data.status === 'success') {
                      showInfo(data.message);
                  } else {
                      alert('Error: ' + data.message);
                  }
              });
        }

        // ============== WIFI VISUALIZATIONS ==============

        let radarCtx = null;
        let radarAngle = 0;
        let radarAnimFrame = null;
        let radarNetworks = [];  // {x, y, strength, ssid, bssid}
        let targetBssidForSignal = null;

        // Initialize radar canvas
        function initRadar() {
            const canvas = document.getElementById('radarCanvas');
            if (!canvas) return;

            radarCtx = canvas.getContext('2d');
            canvas.width = 150;
            canvas.height = 150;

            // Start animation
            if (!radarAnimFrame) {
                animateRadar();
            }
        }

        // Animate radar sweep
        function animateRadar() {
            if (!radarCtx) {
                radarAnimFrame = null;
                return;
            }

            const canvas = radarCtx.canvas;
            const cx = canvas.width / 2;
            const cy = canvas.height / 2;
            const radius = Math.min(cx, cy) - 5;

            // Clear canvas
            radarCtx.fillStyle = 'rgba(0, 10, 10, 0.1)';
            radarCtx.fillRect(0, 0, canvas.width, canvas.height);

            // Draw grid circles
            radarCtx.strokeStyle = 'rgba(0, 212, 255, 0.2)';
            radarCtx.lineWidth = 1;
            for (let r = radius / 4; r <= radius; r += radius / 4) {
                radarCtx.beginPath();
                radarCtx.arc(cx, cy, r, 0, Math.PI * 2);
                radarCtx.stroke();
            }

            // Draw crosshairs
            radarCtx.beginPath();
            radarCtx.moveTo(cx, cy - radius);
            radarCtx.lineTo(cx, cy + radius);
            radarCtx.moveTo(cx - radius, cy);
            radarCtx.lineTo(cx + radius, cy);
            radarCtx.stroke();

            // Draw sweep line
            radarCtx.strokeStyle = 'rgba(0, 255, 136, 0.8)';
            radarCtx.lineWidth = 2;
            radarCtx.beginPath();
            radarCtx.moveTo(cx, cy);
            radarCtx.lineTo(
                cx + Math.cos(radarAngle) * radius,
                cy + Math.sin(radarAngle) * radius
            );
            radarCtx.stroke();

            // Draw sweep gradient
            const gradient = radarCtx.createConicalGradient ?
                null : // Not supported in all browsers
                radarCtx.createRadialGradient(cx, cy, 0, cx, cy, radius);

            radarCtx.fillStyle = 'rgba(0, 255, 136, 0.05)';
            radarCtx.beginPath();
            radarCtx.moveTo(cx, cy);
            radarCtx.arc(cx, cy, radius, radarAngle - 0.5, radarAngle);
            radarCtx.closePath();
            radarCtx.fill();

            // Draw network blips
            radarNetworks.forEach(net => {
                const age = Date.now() - net.timestamp;
                const alpha = Math.max(0.1, 1 - age / 10000);

                radarCtx.fillStyle = `rgba(0, 255, 136, ${alpha})`;
                radarCtx.beginPath();
                radarCtx.arc(net.x, net.y, 4 + (1 - alpha) * 3, 0, Math.PI * 2);
                radarCtx.fill();

                // Glow effect
                radarCtx.fillStyle = `rgba(0, 255, 136, ${alpha * 0.3})`;
                radarCtx.beginPath();
                radarCtx.arc(net.x, net.y, 8 + (1 - alpha) * 5, 0, Math.PI * 2);
                radarCtx.fill();
            });

            // Update angle
            radarAngle += 0.03;
            if (radarAngle > Math.PI * 2) radarAngle = 0;

            radarAnimFrame = requestAnimationFrame(animateRadar);
        }

        // Add network to radar
        function addNetworkToRadar(net) {
            const canvas = document.getElementById('radarCanvas');
            if (!canvas) return;

            const cx = canvas.width / 2;
            const cy = canvas.height / 2;
            const radius = Math.min(cx, cy) - 10;

            // Convert signal strength to distance (stronger = closer)
            const power = parseInt(net.power) || -80;
            const distance = Math.max(0.1, Math.min(1, (power + 100) / 60));
            const r = radius * (1 - distance);

            // Random angle based on BSSID hash
            let angle = 0;
            for (let i = 0; i < net.bssid.length; i++) {
                angle += net.bssid.charCodeAt(i);
            }
            angle = (angle % 360) * Math.PI / 180;

            const x = cx + Math.cos(angle) * r;
            const y = cy + Math.sin(angle) * r;

            // Update or add
            const existing = radarNetworks.find(n => n.bssid === net.bssid);
            if (existing) {
                existing.x = x;
                existing.y = y;
                existing.timestamp = Date.now();
            } else {
                radarNetworks.push({
                    x, y,
                    bssid: net.bssid,
                    ssid: net.essid,
                    timestamp: Date.now()
                });
            }

            // Limit to 50 networks
            if (radarNetworks.length > 50) {
                radarNetworks.shift();
            }
        }

        // Update channel graph
        function updateChannelGraph() {
            const channels = {};
            for (let i = 1; i <= 13; i++) channels[i] = 0;

            // Count networks per channel
            Object.values(wifiNetworks).forEach(net => {
                const ch = parseInt(net.channel);
                if (ch >= 1 && ch <= 13) {
                    channels[ch]++;
                }
            });

            // Find max for scaling
            const maxCount = Math.max(1, ...Object.values(channels));

            // Update bars
            const bars = document.querySelectorAll('#channelGraph .channel-bar');
            bars.forEach((bar, i) => {
                const ch = i + 1;
                const count = channels[ch] || 0;
                const height = Math.max(2, (count / maxCount) * 55);
                bar.style.height = height + 'px';

                bar.classList.remove('active', 'congested', 'very-congested');
                if (count > 0) bar.classList.add('active');
                if (count >= 3) bar.classList.add('congested');
                if (count >= 5) bar.classList.add('very-congested');
            });
        }

        // Update security donut chart
        function updateSecurityDonut() {
            const canvas = document.getElementById('securityCanvas');
            if (!canvas) return;

            const ctx = canvas.getContext('2d');
            const cx = canvas.width / 2;
            const cy = canvas.height / 2;
            const radius = Math.min(cx, cy) - 2;
            const innerRadius = radius * 0.6;

            // Count security types
            let wpa3 = 0, wpa2 = 0, wep = 0, open = 0;
            Object.values(wifiNetworks).forEach(net => {
                const priv = (net.privacy || '').toUpperCase();
                if (priv.includes('WPA3')) wpa3++;
                else if (priv.includes('WPA')) wpa2++;
                else if (priv.includes('WEP')) wep++;
                else if (priv === 'OPN' || priv === '' || priv === 'OPEN') open++;
                else wpa2++; // Default to WPA2
            });

            const total = wpa3 + wpa2 + wep + open;

            // Update legend
            document.getElementById('wpa3Count').textContent = wpa3;
            document.getElementById('wpa2Count').textContent = wpa2;
            document.getElementById('wepCount').textContent = wep;
            document.getElementById('openCount').textContent = open;

            // Clear canvas
            ctx.clearRect(0, 0, canvas.width, canvas.height);

            if (total === 0) {
                // Draw empty circle
                ctx.strokeStyle = '#1a1a1a';
                ctx.lineWidth = radius - innerRadius;
                ctx.beginPath();
                ctx.arc(cx, cy, (radius + innerRadius) / 2, 0, Math.PI * 2);
                ctx.stroke();
                return;
            }

            // Draw segments
            const colors = {
                wpa3: '#00ff88',
                wpa2: '#ff8800',
                wep: '#ff3366',
                open: '#00d4ff'
            };

            const data = [
                { value: wpa3, color: colors.wpa3 },
                { value: wpa2, color: colors.wpa2 },
                { value: wep, color: colors.wep },
                { value: open, color: colors.open }
            ];

            let startAngle = -Math.PI / 2;

            data.forEach(segment => {
                if (segment.value === 0) return;

                const sliceAngle = (segment.value / total) * Math.PI * 2;

                ctx.fillStyle = segment.color;
                ctx.beginPath();
                ctx.moveTo(cx, cy);
                ctx.arc(cx, cy, radius, startAngle, startAngle + sliceAngle);
                ctx.closePath();
                ctx.fill();

                startAngle += sliceAngle;
            });

            // Draw inner circle (donut hole)
            ctx.fillStyle = '#000';
            ctx.beginPath();
            ctx.arc(cx, cy, innerRadius, 0, Math.PI * 2);
            ctx.fill();

            // Draw total in center
            ctx.fillStyle = '#fff';
            ctx.font = 'bold 16px JetBrains Mono';
            ctx.textAlign = 'center';
            ctx.textBaseline = 'middle';
            ctx.fillText(total, cx, cy);
        }

        // Update signal strength meter for targeted network
        function updateSignalMeter(net) {
            if (!net) return;

            targetBssidForSignal = net.bssid;

            const ssidEl = document.getElementById('targetSsid');
            const valueEl = document.getElementById('signalValue');
            const barsEl = document.querySelectorAll('.signal-bar-large');

            ssidEl.textContent = net.essid || net.bssid;

            const power = parseInt(net.power) || -100;
            valueEl.textContent = power + ' dBm';

            // Determine signal quality
            let quality = 'weak';
            let activeBars = 1;

            if (power >= -50) { quality = 'strong'; activeBars = 5; }
            else if (power >= -60) { quality = 'strong'; activeBars = 4; }
            else if (power >= -70) { quality = 'medium'; activeBars = 3; }
            else if (power >= -80) { quality = 'medium'; activeBars = 2; }
            else { quality = 'weak'; activeBars = 1; }

            valueEl.className = 'signal-value ' + quality;

            barsEl.forEach((bar, i) => {
                bar.className = 'signal-bar-large';
                if (i < activeBars) {
                    bar.classList.add('active', quality);
                }
            });
        }

        // Hook into handleWifiNetwork to update visualizations
        const originalHandleWifiNetwork = handleWifiNetwork;
        handleWifiNetwork = function(net) {
            originalHandleWifiNetwork(net);

            // Update radar
            addNetworkToRadar(net);

            // Update channel graph
            updateChannelGraph();

            // Update security donut
            updateSecurityDonut();

            // Update signal meter if this is the targeted network
            if (targetBssidForSignal === net.bssid) {
                updateSignalMeter(net);
            }
        };

        // Update targetNetwork to also set signal meter
        const originalTargetNetwork = targetNetwork;
        targetNetwork = function(bssid, channel) {
            originalTargetNetwork(bssid, channel);

            const net = wifiNetworks[bssid];
            if (net) {
                updateSignalMeter(net);
            }
        };

        // ============== BLUETOOTH RECONNAISSANCE ==============

        let btEventSource = null;
        let isBtRunning = false;
        let btDevices = {};
        let btDeviceCount = 0;
        let btBeaconCount = 0;
        let btRadarCtx = null;
        let btRadarAngle = 0;
        let btRadarAnimFrame = null;
        let btRadarDevices = [];

        // Refresh Bluetooth interfaces
        function refreshBtInterfaces() {
            fetch('/bt/interfaces')
                .then(r => r.json())
                .then(data => {
                    const select = document.getElementById('btInterfaceSelect');
                    if (data.interfaces.length === 0) {
                        select.innerHTML = '<option value="">No BT interfaces found</option>';
                    } else {
                        select.innerHTML = data.interfaces.map(i =>
                            `<option value="${i.name}">${i.name} (${i.type}) [${i.status}]</option>`
                        ).join('');
                    }

                    // Update tool status
                    const statusDiv = document.getElementById('btToolStatus');
                    statusDiv.innerHTML = `
                        <span>hcitool:</span><span class="tool-status ${data.tools.hcitool ? 'ok' : 'missing'}">${data.tools.hcitool ? 'OK' : 'Missing'}</span>
                        <span>bluetoothctl:</span><span class="tool-status ${data.tools.bluetoothctl ? 'ok' : 'missing'}">${data.tools.bluetoothctl ? 'OK' : 'Missing'}</span>
                    `;
                });
        }

        // Start Bluetooth scan
        function startBtScan() {
            const scanMode = document.querySelector('input[name="btScanMode"]:checked').value;
            const iface = document.getElementById('btInterfaceSelect').value;
            const duration = document.getElementById('btScanDuration').value;
            const scanBLE = document.getElementById('btScanBLE').checked;
            const scanClassic = document.getElementById('btScanClassic').checked;

            fetch('/bt/scan/start', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    mode: scanMode,
                    interface: iface,
                    duration: parseInt(duration),
                    scan_ble: scanBLE,
                    scan_classic: scanClassic
                })
            }).then(r => r.json())
              .then(data => {
                  if (data.status === 'started') {
                      setBtRunning(true);
                      startBtStream();
                  } else {
                      alert('Error: ' + data.message);
                  }
              });
        }

        // Stop Bluetooth scan
        function stopBtScan() {
            fetch('/bt/scan/stop', {method: 'POST'})
                .then(r => r.json())
                .then(data => {
                    setBtRunning(false);
                    if (btEventSource) {
                        btEventSource.close();
                        btEventSource = null;
                    }
                });
        }

        function resetBtAdapter() {
            const iface = document.getElementById('btInterfaceSelect')?.value || 'hci0';
            fetch('/bt/reset', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({interface: iface})
            }).then(r => r.json())
              .then(data => {
                  setBtRunning(false);
                  if (btEventSource) {
                      btEventSource.close();
                      btEventSource = null;
                  }
                  if (data.status === 'success') {
                      showInfo('Bluetooth adapter reset. Status: ' + (data.is_up ? 'UP' : 'DOWN'));
                      // Refresh interface list
                      if (typeof refreshBtInterfaces === 'function') refreshBtInterfaces();
                  } else {
                      showError('Reset failed: ' + data.message);
                  }
              });
        }

        function setBtRunning(running) {
            isBtRunning = running;
            document.getElementById('statusDot').classList.toggle('running', running);
            document.getElementById('statusText').textContent = running ? 'Scanning...' : 'Idle';
            document.getElementById('startBtBtn').style.display = running ? 'none' : 'block';
            document.getElementById('stopBtBtn').style.display = running ? 'block' : 'none';
        }

        // Start Bluetooth event stream
        function startBtStream() {
            if (btEventSource) btEventSource.close();

            btEventSource = new EventSource('/bt/stream');

            btEventSource.onmessage = function(e) {
                const data = JSON.parse(e.data);

                if (data.type === 'device') {
                    handleBtDevice(data);
                } else if (data.type === 'info' || data.type === 'raw') {
                    showInfo(data.text);
                } else if (data.type === 'error') {
                    showError(data.text);
                } else if (data.type === 'status') {
                    if (data.text === 'stopped') {
                        setBtRunning(false);
                    }
                }
            };

            btEventSource.onerror = function() {
                console.error('BT stream error');
            };
        }

        // Tracker following detection
        let trackerHistory = {};  // MAC -> { firstSeen, lastSeen, seenCount, locations: [] }
        const FOLLOWING_THRESHOLD_MINUTES = 30;
        const FOLLOWING_MIN_DETECTIONS = 5;

        // Find My network detection patterns
        const FINDMY_PATTERNS = {
            // Apple Find My / AirTag
            apple: {
                prefixes: ['4C:00'],
                mfgData: [0x004C],  // Apple company ID
                names: ['AirTag', 'Find My']
            },
            // Samsung SmartTag
            samsung: {
                prefixes: ['58:4D', 'A0:75', 'DC:0C', 'E4:5F'],
                mfgData: [0x0075],  // Samsung company ID
                names: ['SmartTag', 'Galaxy SmartTag']
            },
            // Tile
            tile: {
                prefixes: ['C4:E7', 'DC:54', 'E4:B0', 'F8:8A', 'D0:03'],
                names: ['Tile', 'Tile Pro', 'Tile Mate', 'Tile Slim']
            },
            // Chipolo
            chipolo: {
                prefixes: ['00:0D'],
                names: ['Chipolo', 'CHIPOLO']
            }
        };

        function detectFindMyDevice(device) {
            const mac = device.mac.toUpperCase();
            const macPrefix = mac.substring(0, 5);
            const name = (device.name || '').toLowerCase();

            for (const [network, patterns] of Object.entries(FINDMY_PATTERNS)) {
                // Check MAC prefix
                if (patterns.prefixes && patterns.prefixes.some(p => mac.startsWith(p))) {
                    return { network: network, type: 'Find My Network', icon: '📍' };
                }
                // Check name patterns
                if (patterns.names && patterns.names.some(n => name.includes(n.toLowerCase()))) {
                    return { network: network, type: 'Find My Network', icon: '📍' };
                }
            }

            // Check manufacturer data for Apple continuity
            if (device.manufacturer_data) {
                const mfgData = device.manufacturer_data;
                if (mfgData.includes('4c00') || mfgData.includes('004c')) {
                    // Check for Find My payload (manufacturer specific data type 0x12)
                    if (mfgData.includes('12') || mfgData.length > 40) {
                        return { network: 'apple', type: 'Apple Find My', icon: '🍎' };
                    }
                }
            }

            return null;
        }

        function checkTrackerFollowing(device) {
            if (!device.tracker && !detectFindMyDevice(device)) return;

            const mac = device.mac;
            const now = Date.now();

            if (!trackerHistory[mac]) {
                trackerHistory[mac] = {
                    firstSeen: now,
                    lastSeen: now,
                    seenCount: 1,
                    name: device.name || device.mac
                };
            } else {
                trackerHistory[mac].lastSeen = now;
                trackerHistory[mac].seenCount++;
            }

            const tracker = trackerHistory[mac];
            const durationMinutes = (now - tracker.firstSeen) / 60000;

            // Alert if tracker has been following for a while
            if (durationMinutes >= FOLLOWING_THRESHOLD_MINUTES && tracker.seenCount >= FOLLOWING_MIN_DETECTIONS) {
                showTrackerFollowingAlert(mac, tracker);
            }
        }

        function showTrackerFollowingAlert(mac, tracker) {
            const alertDiv = document.getElementById('trackerFollowingAlert');
            if (!alertDiv) return;

            const durationMinutes = Math.floor((Date.now() - tracker.firstSeen) / 60000);

            alertDiv.style.display = 'block';
            alertDiv.innerHTML = `
                <h4>⚠️ POSSIBLE TRACKING DETECTED</h4>
                <div style="font-size: 12px;">
                    <div><strong>Device:</strong> ${escapeHtml(tracker.name)}</div>
                    <div><strong>MAC:</strong> ${escapeHtml(mac)}</div>
                    <div><strong>Duration:</strong> ${durationMinutes} minutes</div>
                    <div><strong>Detections:</strong> ${tracker.seenCount}</div>
                    <div style="margin-top: 10px; color: #ff6666;">
                        This tracker has been detected near you for an extended period.
                        If you don't recognize this device, consider your safety.
                    </div>
                    <button onclick="dismissTrackerAlert('${mac}')" class="preset-btn" style="margin-top: 10px; border-color: #ff4444; color: #ff4444;">
                        Dismiss
                    </button>
                </div>
            `;

            if (!muted) {
                // Play warning sound
                for (let i = 0; i < 3; i++) {
                    setTimeout(() => playAlertSound(), i * 300);
                }
            }

            showNotification('⚠️ Tracking Alert', `${tracker.name} detected for ${durationMinutes} min`);
        }

        function dismissTrackerAlert(mac) {
            document.getElementById('trackerFollowingAlert').style.display = 'none';
            // Reset the tracker history for this device
            if (trackerHistory[mac]) {
                trackerHistory[mac].firstSeen = Date.now();
                trackerHistory[mac].seenCount = 0;
            }
        }

        // Handle discovered Bluetooth device
        function handleBtDevice(device) {
            const isNew = !btDevices[device.mac];

            // Check for Find My network
            const findMyInfo = detectFindMyDevice(device);
            if (findMyInfo) {
                device.findmy = findMyInfo;
                device.tracker = device.tracker || { name: findMyInfo.type };
            }

            btDevices[device.mac] = device;

            if (isNew) {
                btDeviceCount++;
                document.getElementById('btDeviceCount').textContent = btDeviceCount;
                playAlert();
                pulseSignal();
            }

            // Check for tracker following
            checkTrackerFollowing(device);

            // Track in device intelligence
            trackDevice({
                protocol: 'Bluetooth',
                address: device.mac,
                message: device.name,
                model: device.manufacturer,
                device_type: device.device_type || device.type || 'other'
            });

            // Update visualizations
            addBtDeviceToRadar(device);

            // Add device card
            addBtDeviceCard(device, isNew);
        }

        // Add Bluetooth device card to output
        function addBtDeviceCard(device, isNew) {
            const output = document.getElementById('output');
            const placeholder = output.querySelector('.placeholder');
            if (placeholder) placeholder.remove();

            let card = document.getElementById('bt_' + device.mac.replace(/:/g, ''));

            if (!card) {
                card = document.createElement('div');
                card.id = 'bt_' + device.mac.replace(/:/g, '');
                card.className = 'sensor-card' + (device.findmy ? ' findmy-device' : '');
                const devType = device.device_type || device.type || 'other';
                card.style.borderLeftColor = device.findmy ? '#007aff' :
                                             device.tracker ? 'var(--accent-red)' :
                                             devType === 'phone' ? 'var(--accent-cyan)' :
                                             devType === 'audio' ? 'var(--accent-green)' :
                                             'var(--accent-orange)';
                output.insertBefore(card, output.firstChild);
            }

            const devType = device.device_type || device.type || 'other';
            const typeIcon = {
                'phone': '📱', 'audio': '🎧', 'wearable': '⌚', 'tracker': '📍',
                'computer': '💻', 'input': '⌨️', 'other': '📶'
            }[devType] || '📶';

            const findMyBadge = device.findmy
                ? `<span class="findmy-badge">${device.findmy.icon || '📍'} ${device.findmy.network.toUpperCase()}</span>`
                : '';

            card.innerHTML = `
                <div class="header" style="display: flex; justify-content: space-between; margin-bottom: 8px;">
                    <span class="device-name">${typeIcon} ${escapeHtml(device.name)} ${findMyBadge}</span>
                    <span style="color: #444; font-size: 10px;">${escapeHtml(devType.toUpperCase())}</span>
                </div>
                <div class="sensor-data">
                    <div class="data-item">
                        <div class="data-label">MAC</div>
                        <div class="data-value" style="font-size: 11px;">${escapeHtml(device.mac)}</div>
                    </div>
                    <div class="data-item">
                        <div class="data-label">Manufacturer</div>
                        <div class="data-value">${escapeHtml(device.manufacturer)}</div>
                    </div>
                    ${device.findmy ? `
                    <div class="data-item">
                        <div class="data-label">Find My</div>
                        <div class="data-value" style="color: #007aff;">${escapeHtml(device.findmy.type)}</div>
                    </div>` : ''}
                    ${device.tracker && !device.findmy ? `
                    <div class="data-item">
                        <div class="data-label">Tracker</div>
                        <div class="data-value" style="color: var(--accent-red);">${escapeHtml(device.tracker.name)}</div>
                    </div>` : ''}
                </div>
                <div style="margin-top: 8px; display: flex; gap: 5px;">
                    <button class="preset-btn" onclick="btTargetDevice('${escapeAttr(device.mac)}')" style="font-size: 10px; padding: 4px 8px;">Target</button>
                    <button class="preset-btn" onclick="btEnumServicesFor('${escapeAttr(device.mac)}')" style="font-size: 10px; padding: 4px 8px;">Services</button>
                </div>
            `;

            if (autoScroll) output.scrollTop = 0;
        }

        // Target a Bluetooth device
        function btTargetDevice(mac) {
            document.getElementById('btTargetMac').value = mac;
            showInfo('Targeted: ' + mac);
        }

        // Enumerate services for a device
        function btEnumServicesFor(mac) {
            document.getElementById('btTargetMac').value = mac;
            btEnumServices();
        }

        // Enumerate services
        function btEnumServices() {
            const mac = document.getElementById('btTargetMac').value;
            if (!mac) { alert('Enter target MAC'); return; }

            showInfo('Enumerating services for ' + mac + '...');

            fetch('/bt/enum', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({mac: mac})
            }).then(r => r.json())
              .then(data => {
                  if (data.status === 'success') {
                      let msg = 'Services for ' + mac + ': ';
                      if (data.services.length === 0) {
                          msg += 'None found';
                      } else {
                          msg += data.services.map(s => s.name).join(', ');
                      }
                      showInfo(msg);
                  } else {
                      showInfo('Error: ' + data.message);
                  }
              });
        }

        // Initialize Bluetooth radar
        function initBtRadar() {
            const canvas = document.getElementById('btRadarCanvas');
            if (!canvas) return;

            btRadarCtx = canvas.getContext('2d');
            canvas.width = 150;
            canvas.height = 150;

            if (!btRadarAnimFrame) {
                animateBtRadar();
            }
        }

        // Animate Bluetooth radar
        function animateBtRadar() {
            if (!btRadarCtx) { btRadarAnimFrame = null; return; }

            const canvas = btRadarCtx.canvas;
            const cx = canvas.width / 2;
            const cy = canvas.height / 2;
            const radius = Math.min(cx, cy) - 5;

            btRadarCtx.fillStyle = 'rgba(0, 10, 20, 0.1)';
            btRadarCtx.fillRect(0, 0, canvas.width, canvas.height);

            // Grid circles
            btRadarCtx.strokeStyle = 'rgba(138, 43, 226, 0.2)';
            btRadarCtx.lineWidth = 1;
            for (let r = radius / 4; r <= radius; r += radius / 4) {
                btRadarCtx.beginPath();
                btRadarCtx.arc(cx, cy, r, 0, Math.PI * 2);
                btRadarCtx.stroke();
            }

            // Sweep line (purple for BT)
            btRadarCtx.strokeStyle = 'rgba(138, 43, 226, 0.8)';
            btRadarCtx.lineWidth = 2;
            btRadarCtx.beginPath();
            btRadarCtx.moveTo(cx, cy);
            btRadarCtx.lineTo(cx + Math.cos(btRadarAngle) * radius, cy + Math.sin(btRadarAngle) * radius);
            btRadarCtx.stroke();

            // Device blips
            btRadarDevices.forEach(dev => {
                const age = Date.now() - dev.timestamp;
                const alpha = Math.max(0.1, 1 - age / 15000);
                const color = dev.isTracker ? '255, 51, 102' : '138, 43, 226';

                btRadarCtx.fillStyle = `rgba(${color}, ${alpha})`;
                btRadarCtx.beginPath();
                btRadarCtx.arc(dev.x, dev.y, dev.isTracker ? 6 : 4, 0, Math.PI * 2);
                btRadarCtx.fill();
            });

            btRadarAngle += 0.025;
            if (btRadarAngle > Math.PI * 2) btRadarAngle = 0;

            btRadarAnimFrame = requestAnimationFrame(animateBtRadar);
        }

        // Add device to BT radar
        function addBtDeviceToRadar(device) {
            const canvas = document.getElementById('btRadarCanvas');
            if (!canvas) return;

            const cx = canvas.width / 2;
            const cy = canvas.height / 2;
            const radius = Math.min(cx, cy) - 10;

            // Random position based on MAC hash
            let angle = 0;
            for (let i = 0; i < device.mac.length; i++) {
                angle += device.mac.charCodeAt(i);
            }
            angle = (angle % 360) * Math.PI / 180;
            const r = radius * (0.3 + Math.random() * 0.6);

            const x = cx + Math.cos(angle) * r;
            const y = cy + Math.sin(angle) * r;

            const existing = btRadarDevices.find(d => d.mac === device.mac);
            if (existing) {
                existing.timestamp = Date.now();
            } else {
                btRadarDevices.push({
                    x, y,
                    mac: device.mac,
                    isTracker: !!device.tracker,
                    timestamp: Date.now()
                });
            }

            if (btRadarDevices.length > 50) btRadarDevices.shift();
        }

        // ============================================
        // AIRCRAFT (ADS-B) MODE FUNCTIONS
        // ============================================

        function checkAdsbTools() {
            fetch('/adsb/tools')
                .then(r => r.json())
                .then(data => {
                    // Update aircraft mode panel status
                    const dump1090Status = document.getElementById('dump1090Status');
                    const rtlAdsbStatus = document.getElementById('rtlAdsbStatus');
                    if (dump1090Status) {
                        dump1090Status.textContent = data.dump1090 ? 'OK' : 'Missing';
                        dump1090Status.className = 'tool-status ' + (data.dump1090 ? 'ok' : 'missing');
                    }
                    if (rtlAdsbStatus) {
                        rtlAdsbStatus.textContent = data.rtl_adsb ? 'OK' : 'Missing';
                        rtlAdsbStatus.className = 'tool-status ' + (data.rtl_adsb ? 'ok' : 'missing');
                    }
                    // Update sidebar status
                    const dump1090Sidebar = document.getElementById('dump1090StatusSidebar');
                    const rtlAdsbSidebar = document.getElementById('rtlAdsbStatusSidebar');
                    if (dump1090Sidebar) {
                        dump1090Sidebar.textContent = data.dump1090 ? 'OK' : 'Missing';
                        dump1090Sidebar.className = 'tool-status ' + (data.dump1090 ? 'ok' : 'missing');
                    }
                    if (rtlAdsbSidebar) {
                        rtlAdsbSidebar.textContent = data.rtl_adsb ? 'OK' : 'Missing';
                        rtlAdsbSidebar.className = 'tool-status ' + (data.rtl_adsb ? 'ok' : 'missing');
                    }
                });
        }

        // Leaflet map for aircraft tracking
        let aircraftMap = null;
        let aircraftMarkers = {};
        let aircraftClusterGroup = null;
        let clusteringEnabled = false;
        let mapRefreshInterval = null;

        function initAircraftRadar() {
            const mapContainer = document.getElementById('aircraftMap');
            if (!mapContainer || aircraftMap) return;

            // Initialize Leaflet map
            aircraftMap = L.map('aircraftMap', {
                center: [51.5, -0.1], // Default to London
                zoom: 5,
                zoomControl: true,
                attributionControl: true
            });

            // Add OpenStreetMap tiles (will be inverted by CSS for dark theme)
            L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                attribution: '&copy; OpenStreetMap',
                maxZoom: 18
            }).addTo(aircraftMap);

            // Initialize cluster group (but don't add to map yet)
            aircraftClusterGroup = L.markerClusterGroup({
                maxClusterRadius: 50,
                spiderfyOnMaxZoom: true,
                showCoverageOnHover: false,
                iconCreateFunction: function(cluster) {
                    const count = cluster.getChildCount();
                    let size = 'small';
                    if (count > 10) size = 'medium';
                    if (count > 25) size = 'large';

                    return L.divIcon({
                        html: '<div class="marker-cluster marker-cluster-' + size + '">' + count + '</div>',
                        className: '',
                        iconSize: L.point(40, 40)
                    });
                }
            });

            // Update time display
            updateRadarTime();
            setInterval(updateRadarTime, 1000);

            // Refresh aircraft markers every second
            if (!mapRefreshInterval) {
                mapRefreshInterval = setInterval(updateAircraftMarkers, 1000);
            }

            // Setup interaction tracking
            setupMapInteraction();

            // Initial update
            updateAircraftMarkers();
        }

        function toggleAircraftClustering() {
            clusteringEnabled = document.getElementById('adsbEnableClustering').checked;

            if (!aircraftMap || !aircraftClusterGroup) return;

            if (clusteringEnabled) {
                // Move all markers to cluster group
                Object.values(aircraftMarkers).forEach(marker => {
                    if (aircraftMap.hasLayer(marker)) {
                        aircraftMap.removeLayer(marker);
                    }
                    aircraftClusterGroup.addLayer(marker);
                });
                aircraftMap.addLayer(aircraftClusterGroup);
            } else {
                // Move all markers back to map directly
                aircraftClusterGroup.clearLayers();
                aircraftMap.removeLayer(aircraftClusterGroup);
                Object.values(aircraftMarkers).forEach(marker => {
                    marker.addTo(aircraftMap);
                });
            }
        }

        function updateRadarTime() {
            const now = new Date();
            const time = now.toTimeString().substring(0, 8);
            const el = document.getElementById('radarTime');
            if (el) el.textContent = time;
        }

        function createAircraftIcon(heading, emergency, customColor) {
            const color = customColor || (emergency ? '#ff4444' : '#00d4ff');
            const rotation = heading || 0;

            return L.divIcon({
                className: 'aircraft-marker' + (emergency ? ' squawk-emergency' : ''),
                html: `<svg width="24" height="24" viewBox="0 0 24 24" style="transform: rotate(${rotation}deg); color: ${color};">
                    <path fill="currentColor" d="M12 2L8 10H4v2l8 4 8-4v-2h-4L12 2zm0 14l-6 3v1h12v-1l-6-3z"/>
                </svg>`,
                iconSize: [24, 24],
                iconAnchor: [12, 12]
            });
        }

        let aircraftTrailLines = {};  // ICAO -> Leaflet polyline

        function updateAircraftMarkers() {
            if (!aircraftMap) return;

            const showLabels = document.getElementById('adsbShowLabels')?.checked;
            const showAltitude = document.getElementById('adsbShowAltitude')?.checked;
            const showTrails = document.getElementById('adsbShowTrails')?.checked ?? true;
            const currentIds = new Set();

            // Update or create markers for each aircraft
            Object.entries(adsbAircraft).forEach(([icao, aircraft]) => {
                if (aircraft.lat == null || aircraft.lon == null) return;

                currentIds.add(icao);

                // Update trail history
                updateAircraftTrail(icao, aircraft.lat, aircraft.lon);

                // Check for emergency squawk codes
                const squawkInfo = checkSquawkCode(aircraft);

                // Check for military aircraft
                const militaryInfo = isMilitaryAircraft(icao, aircraft.callsign);
                aircraft.military = militaryInfo.military;

                // Determine icon color
                let iconColor = '#00d4ff';  // Default cyan
                if (squawkInfo) iconColor = squawkInfo.color;
                else if (militaryInfo.military) iconColor = '#556b2f';  // Olive drab
                else if (aircraft.emergency) iconColor = '#ff4444';

                const icon = createAircraftIcon(aircraft.heading, squawkInfo || aircraft.emergency, iconColor);

                if (aircraftMarkers[icao]) {
                    // Update existing marker
                    aircraftMarkers[icao].setLatLng([aircraft.lat, aircraft.lon]);
                    aircraftMarkers[icao].setIcon(icon);
                } else {
                    // Create new marker
                    const marker = L.marker([aircraft.lat, aircraft.lon], { icon: icon });
                    if (clusteringEnabled && aircraftClusterGroup) {
                        aircraftClusterGroup.addLayer(marker);
                    } else {
                        marker.addTo(aircraftMap);
                    }
                    aircraftMarkers[icao] = marker;
                }

                // Draw flight trail
                if (showTrails && aircraftTrails[icao] && aircraftTrails[icao].length > 1) {
                    const trailCoords = aircraftTrails[icao].map(p => [p.lat, p.lon]);

                    if (aircraftTrailLines[icao]) {
                        aircraftTrailLines[icao].setLatLngs(trailCoords);
                    } else {
                        aircraftTrailLines[icao] = L.polyline(trailCoords, {
                            color: militaryInfo.military ? '#556b2f' : '#00d4ff',
                            weight: 2,
                            opacity: 0.6,
                            dashArray: '5, 5'
                        }).addTo(aircraftMap);
                    }
                } else if (aircraftTrailLines[icao]) {
                    aircraftMap.removeLayer(aircraftTrailLines[icao]);
                    delete aircraftTrailLines[icao];
                }

                // Update popup content
                let popupContent = '<div class="aircraft-popup">';

                // Military badge
                if (militaryInfo.military) {
                    popupContent += `<div style="background: #556b2f; color: white; padding: 2px 8px; border-radius: 3px; font-size: 10px; margin-bottom: 5px;">🎖️ MILITARY${militaryInfo.country ? ' (' + militaryInfo.country + ')' : ''}</div>`;
                }

                // Squawk alert
                if (squawkInfo) {
                    popupContent += `<div style="background: ${squawkInfo.color}; color: white; padding: 4px 8px; border-radius: 3px; font-size: 11px; margin-bottom: 5px; font-weight: bold;">⚠️ ${squawkInfo.name}</div>`;
                }

                popupContent += `<div class="callsign">${aircraft.callsign || icao}</div>`;

                if (aircraft.altitude) {
                    popupContent += `<div class="data-row"><span class="label">Altitude:</span><span class="value">${aircraft.altitude.toLocaleString()} ft</span></div>`;
                }
                if (aircraft.speed) {
                    popupContent += `<div class="data-row"><span class="label">Speed:</span><span class="value">${aircraft.speed} kts</span></div>`;
                }
                if (aircraft.heading !== undefined) {
                    popupContent += `<div class="data-row"><span class="label">Heading:</span><span class="value">${aircraft.heading}°</span></div>`;
                }
                if (aircraft.squawk) {
                    const squawkStyle = squawkInfo ? `color: ${squawkInfo.color}; font-weight: bold;` : '';
                    popupContent += `<div class="data-row"><span class="label">Squawk:</span><span class="value" style="${squawkStyle}">${aircraft.squawk}</span></div>`;
                }
                popupContent += '</div>';

                aircraftMarkers[icao].bindPopup(popupContent);

                // Add tooltip if labels enabled
                if (showLabels || showAltitude) {
                    let tooltipText = '';
                    if (showLabels && aircraft.callsign) tooltipText = aircraft.callsign;
                    if (showAltitude && aircraft.altitude) {
                        if (tooltipText) tooltipText += ' ';
                        tooltipText += 'FL' + Math.round(aircraft.altitude / 100).toString().padStart(3, '0');
                    }
                    if (tooltipText) {
                        aircraftMarkers[icao].bindTooltip(tooltipText, {
                            permanent: true,
                            direction: 'right',
                            className: 'aircraft-tooltip'
                        });
                    }
                } else {
                    aircraftMarkers[icao].unbindTooltip();
                }
            });

            // Remove markers for aircraft no longer tracked
            Object.keys(aircraftMarkers).forEach(icao => {
                if (!currentIds.has(icao)) {
                    if (clusteringEnabled && aircraftClusterGroup) {
                        aircraftClusterGroup.removeLayer(aircraftMarkers[icao]);
                    } else {
                        aircraftMap.removeLayer(aircraftMarkers[icao]);
                    }
                    // Also remove trail
                    if (aircraftTrailLines[icao]) {
                        aircraftMap.removeLayer(aircraftTrailLines[icao]);
                        delete aircraftTrailLines[icao];
                    }
                    delete aircraftTrails[icao];
                    delete aircraftMarkers[icao];
                    delete activeSquawkAlerts[icao];
                }
            });

            // Update status display
            const aircraftCount = Object.keys(adsbAircraft).length;
            document.getElementById('radarStatus').textContent = isAdsbRunning ?
                `TRACKING ${aircraftCount}` : 'STANDBY';
            document.getElementById('aircraftCount').textContent = aircraftCount;

            // Update map center display
            const center = aircraftMap.getCenter();
            document.getElementById('mapCenter').textContent =
                `${center.lat.toFixed(2)}, ${center.lng.toFixed(2)}`;

            // Auto-fit bounds if we have aircraft
            if (aircraftCount > 0 && !aircraftMap._userInteracted) {
                const bounds = [];
                Object.values(adsbAircraft).forEach(a => {
                    if (a.lat !== undefined && a.lon !== undefined) {
                        bounds.push([a.lat, a.lon]);
                    }
                });
                if (bounds.length > 0) {
                    aircraftMap.fitBounds(bounds, { padding: [30, 30], maxZoom: 10 });
                }
            }
        }

        // Track user interaction to stop auto-fitting
        function setupMapInteraction() {
            if (aircraftMap) {
                aircraftMap.on('dragstart zoomstart', () => {
                    aircraftMap._userInteracted = true;
                });
            }
        }

        function startAdsbScan() {
            const gain = document.getElementById('adsbGain').value;
            const device = getSelectedDevice();

            fetch('/adsb/start', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ gain, device })
            })
            .then(r => r.json())
            .then(data => {
                if (data.status === 'started') {
                    isAdsbRunning = true;
                    document.getElementById('startAdsbBtn').style.display = 'none';
                    document.getElementById('stopAdsbBtn').style.display = 'block';
                    document.getElementById('statusDot').className = 'status-dot active';
                    document.getElementById('statusText').textContent = 'ADS-B Tracking';
                    startAdsbStream();
                } else {
                    alert('Error: ' + data.message);
                }
            });
        }

        function stopAdsbScan() {
            fetch('/adsb/stop', { method: 'POST' })
                .then(r => r.json())
                .then(data => {
                    isAdsbRunning = false;
                    document.getElementById('startAdsbBtn').style.display = 'block';
                    document.getElementById('stopAdsbBtn').style.display = 'none';
                    document.getElementById('statusDot').className = 'status-dot';
                    document.getElementById('statusText').textContent = 'Idle';
                    if (adsbEventSource) {
                        adsbEventSource.close();
                        adsbEventSource = null;
                    }
                });
        }

        function startAdsbStream() {
            if (adsbEventSource) adsbEventSource.close();
            adsbEventSource = new EventSource('/adsb/stream');

            adsbEventSource.onmessage = function(e) {
                const data = JSON.parse(e.data);
                if (data.type === 'aircraft') {
                    adsbAircraft[data.icao] = {
                        ...adsbAircraft[data.icao],
                        ...data,
                        lastSeen: Date.now()
                    };
                    adsbMsgCount++;
                    updateAdsbStats();
                    updateAircraftMarkers();
                    addAircraftToOutput(data);
                }
            };

            // Periodic cleanup of stale aircraft
            setInterval(() => {
                const now = Date.now();
                Object.keys(adsbAircraft).forEach(icao => {
                    if (now - adsbAircraft[icao].lastSeen > 60000) {
                        delete adsbAircraft[icao];
                    }
                });
                updateAircraftMarkers();
            }, 5000);
        }

        function updateAdsbStats() {
            document.getElementById('aircraftCount').textContent = Object.keys(adsbAircraft).length;
            document.getElementById('adsbMsgCount').textContent = adsbMsgCount;
            document.getElementById('icaoCount').textContent = Object.keys(adsbAircraft).length;
        }

        function addAircraftToOutput(aircraft) {
            const output = document.getElementById('output');
            const placeholder = output.querySelector('.placeholder');
            if (placeholder) placeholder.remove();

            const card = document.createElement('div');
            card.className = 'aircraft-card';
            card.innerHTML = `
                <div class="aircraft-icon" style="--heading: ${aircraft.heading || 0}deg;">✈️</div>
                <div class="aircraft-info">
                    <div class="aircraft-callsign">${aircraft.callsign || aircraft.icao}</div>
                    <div class="aircraft-data">ICAO: <span>${aircraft.icao}</span></div>
                    <div class="aircraft-data">Alt: <span>${aircraft.altitude ? aircraft.altitude + ' ft' : 'N/A'}</span></div>
                    <div class="aircraft-data">Speed: <span>${aircraft.speed ? aircraft.speed + ' kts' : 'N/A'}</span></div>
                    <div class="aircraft-data">Heading: <span>${aircraft.heading ? aircraft.heading + '°' : 'N/A'}</span></div>
                </div>
            `;
            output.insertBefore(card, output.firstChild);

            // Limit cards
            while (output.children.length > 50) {
                output.removeChild(output.lastChild);
            }
        }

        // ============================================
        // SATELLITE MODE FUNCTIONS
        // ============================================

        function switchSatelliteTab(tab) {
            document.querySelectorAll('.satellite-tab').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.satellite-content').forEach(c => c.classList.remove('active'));
            document.querySelector(`.satellite-tab:nth-child(${tab === 'predictor' ? 1 : 2})`).classList.add('active');
            document.getElementById(tab === 'predictor' ? 'predictorTab' : 'iridiumTab').classList.add('active');

            // Toggle Iridium burst log visibility
            document.getElementById('iridiumBurstLog').style.display = tab === 'iridium' ? 'block' : 'none';
        }

        function getLocation() {
            if (navigator.geolocation) {
                navigator.geolocation.getCurrentPosition(
                    position => {
                        document.getElementById('obsLat').value = position.coords.latitude.toFixed(4);
                        document.getElementById('obsLon').value = position.coords.longitude.toFixed(4);
                        showInfo('Location updated!');
                    },
                    error => {
                        alert('Could not get location: ' + error.message);
                    }
                );
            } else {
                alert('Geolocation not supported by browser');
            }
        }

        function initPolarPlot() {
            const canvas = document.getElementById('polarPlotCanvas');
            if (!canvas) return;
            const container = canvas.parentElement;
            const size = Math.min(container.offsetWidth, 400);
            canvas.width = size;
            canvas.height = size;
            drawPolarPlot();
        }

        function drawPolarPlot(pass = null) {
            const canvas = document.getElementById('polarPlotCanvas');
            if (!canvas) return;
            const ctx = canvas.getContext('2d');
            const size = canvas.width;
            const cx = size / 2;
            const cy = size / 2;
            const radius = size / 2 - 30;

            // Clear
            ctx.fillStyle = '#0a0a0a';
            ctx.fillRect(0, 0, size, size);

            // Draw elevation rings
            ctx.strokeStyle = 'rgba(0, 255, 255, 0.2)';
            ctx.lineWidth = 1;
            for (let el = 0; el <= 90; el += 30) {
                const r = radius * (90 - el) / 90;
                ctx.beginPath();
                ctx.arc(cx, cy, r, 0, Math.PI * 2);
                ctx.stroke();

                // Label
                if (el > 0) {
                    ctx.fillStyle = '#444';
                    ctx.font = '10px JetBrains Mono';
                    ctx.textAlign = 'center';
                    ctx.fillText(el + '°', cx, cy - r + 12);
                }
            }

            // Draw azimuth lines
            for (let az = 0; az < 360; az += 45) {
                const rad = az * Math.PI / 180;
                ctx.beginPath();
                ctx.moveTo(cx, cy);
                ctx.lineTo(cx + Math.sin(rad) * radius, cy - Math.cos(rad) * radius);
                ctx.stroke();
            }

            // Draw cardinal directions
            ctx.fillStyle = '#00ffff';
            ctx.font = 'bold 14px Rajdhani';
            ctx.textAlign = 'center';
            ctx.fillText('N', cx, cy - radius - 8);
            ctx.fillStyle = '#888';
            ctx.fillText('S', cx, cy + radius + 16);
            ctx.fillText('E', cx + radius + 12, cy + 4);
            ctx.fillText('W', cx - radius - 12, cy + 4);

            // Draw zenith
            ctx.fillStyle = '#00ffff';
            ctx.beginPath();
            ctx.arc(cx, cy, 3, 0, Math.PI * 2);
            ctx.fill();

            // Draw selected pass trajectory
            if (pass && pass.trajectory) {
                ctx.strokeStyle = pass.color || '#00ff00';
                ctx.lineWidth = 2;
                ctx.setLineDash([5, 3]);
                ctx.beginPath();

                pass.trajectory.forEach((point, i) => {
                    const r = radius * (90 - point.elevation) / 90;
                    const rad = point.azimuth * Math.PI / 180;
                    const x = cx + Math.sin(rad) * r;
                    const y = cy - Math.cos(rad) * r;

                    if (i === 0) ctx.moveTo(x, y);
                    else ctx.lineTo(x, y);
                });
                ctx.stroke();
                ctx.setLineDash([]);

                // Draw max elevation point
                const maxPoint = pass.trajectory.reduce((max, p) => p.elevation > max.elevation ? p : max, { elevation: 0 });
                const maxR = radius * (90 - maxPoint.elevation) / 90;
                const maxRad = maxPoint.azimuth * Math.PI / 180;
                const maxX = cx + Math.sin(maxRad) * maxR;
                const maxY = cy - Math.cos(maxRad) * maxR;

                ctx.fillStyle = pass.color || '#00ff00';
                ctx.beginPath();
                ctx.arc(maxX, maxY, 6, 0, Math.PI * 2);
                ctx.fill();

                // Label
                ctx.fillStyle = '#fff';
                ctx.font = '11px JetBrains Mono';
                ctx.fillText(pass.satellite, maxX + 10, maxY - 5);
            }
        }

        function calculatePasses() {
            const lat = parseFloat(document.getElementById('obsLat').value);
            const lon = parseFloat(document.getElementById('obsLon').value);
            const hours = parseInt(document.getElementById('predictionHours').value);
            const minEl = parseInt(document.getElementById('minElevation').value);

            const satellites = getSelectedSatellites();

            if (satellites.length === 0) {
                alert('Please select at least one satellite to track');
                return;
            }

            fetch('/satellite/predict', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ lat, lon, hours, minEl, satellites })
            })
            .then(r => r.json())
            .then(data => {
                if (data.status === 'success') {
                    satellitePasses = data.passes;
                    renderPassList();
                    document.getElementById('passCount').textContent = data.passes.length;
                    if (data.passes.length > 0) {
                        selectPass(0);
                        document.getElementById('satelliteCountdown').style.display = 'block';
                        updateSatelliteCountdown();
                        startCountdownTimer();
                    } else {
                        document.getElementById('satelliteCountdown').style.display = 'none';
                    }
                } else {
                    alert('Error: ' + data.message);
                }
            });
        }

        function renderPassList() {
            const container = document.getElementById('passList');
            container.innerHTML = '';

            if (satellitePasses.length === 0) {
                container.innerHTML = '<div style="color: #666; text-align: center; padding: 30px;">No passes found for selected criteria.</div>';
                return;
            }

            document.getElementById('passListCount').textContent = satellitePasses.length + ' passes';

            satellitePasses.forEach((pass, index) => {
                const card = document.createElement('div');
                card.className = 'pass-card' + (index === 0 ? ' active' : '');
                card.onclick = () => selectPass(index);

                const quality = pass.maxEl >= 60 ? 'excellent' : pass.maxEl >= 30 ? 'good' : 'fair';

                card.innerHTML = `
                    <div class="pass-satellite">${pass.satellite}</div>
                    <div class="pass-time">${pass.startTime}</div>
                    <div class="pass-details">
                        <div>Max El: <span>${pass.maxEl}°</span></div>
                        <div>Duration: <span>${pass.duration}m</span></div>
                        <div class="pass-quality ${quality}">${quality.toUpperCase()}</div>
                    </div>
                `;
                container.appendChild(card);
            });
        }

        function selectPass(index) {
            selectedPass = satellitePasses[index];
            selectedPassIndex = index;
            document.querySelectorAll('.pass-card').forEach((card, i) => {
                card.classList.toggle('active', i === index);
            });
            drawPolarPlot(selectedPass);
            updateGroundTrack(selectedPass);
            // Update countdown to show selected pass
            updateSatelliteCountdown();
        }

        // Ground Track Map
        let groundTrackMap = null;
        let groundTrackLine = null;
        let satMarker = null;
        let observerMarker = null;
        let satPositionInterval = null;

        function initGroundTrackMap() {
            const mapContainer = document.getElementById('groundTrackMap');
            if (!mapContainer || groundTrackMap) return;

            groundTrackMap = L.map('groundTrackMap', {
                center: [20, 0],
                zoom: 1,
                zoomControl: true,
                attributionControl: false
            });

            L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
                maxZoom: 19
            }).addTo(groundTrackMap);

            // Add observer marker
            const lat = parseFloat(document.getElementById('obsLat').value) || 51.5;
            const lon = parseFloat(document.getElementById('obsLon').value) || -0.1;
            observerMarker = L.circleMarker([lat, lon], {
                radius: 8,
                fillColor: '#ff6600',
                color: '#fff',
                weight: 2,
                fillOpacity: 1
            }).addTo(groundTrackMap).bindPopup('Observer Location');
        }

        function updateGroundTrack(pass) {
            if (!groundTrackMap) initGroundTrackMap();
            if (!pass || !pass.groundTrack) return;

            // Remove old track and marker
            if (groundTrackLine) groundTrackMap.removeLayer(groundTrackLine);
            if (satMarker) groundTrackMap.removeLayer(satMarker);

            // Draw ground track
            const coords = pass.groundTrack.map(p => [p.lat, p.lon]);
            groundTrackLine = L.polyline(coords, {
                color: pass.color || '#00ff00',
                weight: 2,
                opacity: 0.8,
                dashArray: '5, 5'
            }).addTo(groundTrackMap);

            // Add current position marker
            if (pass.currentPosition) {
                satMarker = L.marker([pass.currentPosition.lat, pass.currentPosition.lon], {
                    icon: L.divIcon({
                        className: 'sat-marker',
                        html: '<div style="background:#ffff00;width:12px;height:12px;border-radius:50%;border:2px solid #000;box-shadow:0 0 10px #ffff00;"></div>',
                        iconSize: [12, 12],
                        iconAnchor: [6, 6]
                    })
                }).addTo(groundTrackMap).bindPopup(pass.satellite);
            }

            // Update observer marker position
            const lat = parseFloat(document.getElementById('obsLat').value) || 51.5;
            const lon = parseFloat(document.getElementById('obsLon').value) || -0.1;
            if (observerMarker) {
                observerMarker.setLatLng([lat, lon]);
            }

            // Fit bounds to show track
            if (coords.length > 0) {
                groundTrackMap.fitBounds(groundTrackLine.getBounds(), { padding: [20, 20] });
            }
        }

        function toggleGroundTrack() {
            const show = document.getElementById('showGroundTrack').checked;
            document.getElementById('groundTrackMap').style.display = show ? 'block' : 'none';
            if (show && groundTrackMap) {
                groundTrackMap.invalidateSize();
            }
        }

        function startSatellitePositionUpdates() {
            if (satPositionInterval) clearInterval(satPositionInterval);
            satPositionInterval = setInterval(() => {
                if (selectedPass) {
                    updateRealTimePosition();
                }
            }, 5000);
        }

        function updateRealTimePosition() {
            const satellites = getSelectedSatellites();
            if (satellites.length === 0) return;

            const lat = parseFloat(document.getElementById('obsLat').value);
            const lon = parseFloat(document.getElementById('obsLon').value);

            fetch('/satellite/position', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ lat, lon, satellites })
            })
            .then(r => r.json())
            .then(data => {
                if (data.status === 'success' && data.positions) {
                    updateRealTimeIndicators(data.positions);
                }
            });
        }

        let orbitTrackLine = null;
        let pastOrbitLine = null;

        function updateRealTimeIndicators(positions) {
            // Update ground track map markers
            positions.forEach(pos => {
                if (selectedPass && pos.satellite === selectedPass.satellite) {
                    // Update satellite marker position
                    if (satMarker) {
                        satMarker.setLatLng([pos.lat, pos.lon]);
                        satMarker.setPopupContent(pos.satellite + '<br>Alt: ' + pos.altitude.toFixed(0) + ' km<br>El: ' + pos.elevation.toFixed(1) + '°');
                    } else if (groundTrackMap) {
                        satMarker = L.marker([pos.lat, pos.lon], {
                            icon: L.divIcon({
                                className: 'sat-marker',
                                html: '<div style="background:#ffff00;width:14px;height:14px;border-radius:50%;border:2px solid #000;box-shadow:0 0 15px #ffff00;animation:pulse-sat 1s infinite;"></div>',
                                iconSize: [14, 14],
                                iconAnchor: [7, 7]
                            })
                        }).addTo(groundTrackMap).bindPopup(pos.satellite + '<br>Alt: ' + pos.altitude.toFixed(0) + ' km');
                    }

                    // Draw full orbit track from position endpoint
                    if (pos.orbitTrack && pos.orbitTrack.length > 0 && groundTrackMap) {
                        // Split into past and future segments
                        const pastCoords = pos.orbitTrack.filter(p => p.past).map(p => [p.lat, p.lon]);
                        const futureCoords = pos.orbitTrack.filter(p => !p.past).map(p => [p.lat, p.lon]);

                        // Remove old lines
                        if (orbitTrackLine) groundTrackMap.removeLayer(orbitTrackLine);
                        if (pastOrbitLine) groundTrackMap.removeLayer(pastOrbitLine);

                        // Draw past track (dimmer)
                        if (pastCoords.length > 1) {
                            pastOrbitLine = L.polyline(pastCoords, {
                                color: '#666666',
                                weight: 2,
                                opacity: 0.5,
                                dashArray: '3, 6'
                            }).addTo(groundTrackMap);
                        }

                        // Draw future track (brighter)
                        if (futureCoords.length > 1) {
                            orbitTrackLine = L.polyline(futureCoords, {
                                color: selectedPass.color || '#00ff00',
                                weight: 3,
                                opacity: 0.8
                            }).addTo(groundTrackMap);
                        }
                    }

                    // Update polar plot with real-time position
                    if (pos.elevation > 0) {
                        drawRealTimePositionOnPolar(pos);
                    }
                }
            });
        }

        function drawRealTimePositionOnPolar(pos) {
            const canvas = document.getElementById('polarPlotCanvas');
            if (!canvas) return;
            const ctx = canvas.getContext('2d');
            const size = canvas.width;
            const cx = size / 2;
            const cy = size / 2;
            const radius = size / 2 - 30;

            // Draw pulsing indicator for current position
            const r = radius * (90 - pos.elevation) / 90;
            const rad = pos.azimuth * Math.PI / 180;
            const x = cx + Math.sin(rad) * r;
            const y = cy - Math.cos(rad) * r;

            ctx.fillStyle = '#ffff00';
            ctx.beginPath();
            ctx.arc(x, y, 8, 0, Math.PI * 2);
            ctx.fill();

            ctx.strokeStyle = '#ffff00';
            ctx.lineWidth = 2;
            ctx.beginPath();
            ctx.arc(x, y, 12, 0, Math.PI * 2);
            ctx.stroke();
        }

        function updateTLE() {
            fetch('/satellite/update-tle', { method: 'POST' })
                .then(r => r.json())
                .then(data => {
                    if (data.status === 'success') {
                        showInfo('TLE data updated!');
                    } else {
                        alert('Error updating TLE: ' + data.message);
                    }
                });
        }

        // Satellite management
        let trackedSatellites = [
            { id: 'ISS', name: 'ISS (ZARYA)', norad: '25544', builtin: true, checked: true },
            { id: 'NOAA-15', name: 'NOAA 15', norad: '25338', builtin: true, checked: true },
            { id: 'NOAA-18', name: 'NOAA 18', norad: '28654', builtin: true, checked: true },
            { id: 'NOAA-19', name: 'NOAA 19', norad: '33591', builtin: true, checked: true },
            { id: 'METEOR-M2', name: 'Meteor-M 2', norad: '40069', builtin: true, checked: true }
        ];

        function renderSatelliteList() {
            const list = document.getElementById('satelliteList');
            if (!list) return;

            list.innerHTML = trackedSatellites.map((sat, idx) => `
                <div class="sat-item ${sat.builtin ? 'builtin' : ''}">
                    <label>
                        <input type="checkbox" ${sat.checked ? 'checked' : ''} onchange="toggleSatellite(${idx})">
                        <span class="sat-name">${sat.name}</span>
                        <span class="sat-norad">#${sat.norad}</span>
                    </label>
                    <button class="sat-remove" onclick="removeSatellite(${idx})" title="Remove">✕</button>
                </div>
            `).join('');
        }

        function toggleSatellite(idx) {
            trackedSatellites[idx].checked = !trackedSatellites[idx].checked;
        }

        function removeSatellite(idx) {
            if (!trackedSatellites[idx].builtin) {
                trackedSatellites.splice(idx, 1);
                renderSatelliteList();
            }
        }

        function getSelectedSatellites() {
            return trackedSatellites.filter(s => s.checked).map(s => s.id);
        }

        function showAddSatelliteModal() {
            document.getElementById('satModal').classList.add('active');
        }

        function closeSatModal() {
            document.getElementById('satModal').classList.remove('active');
        }

        function switchSatModalTab(tab) {
            document.querySelectorAll('.sat-modal-tab').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.sat-modal-section').forEach(s => s.classList.remove('active'));

            if (tab === 'tle') {
                document.querySelector('.sat-modal-tab:first-child').classList.add('active');
                document.getElementById('tleSection').classList.add('active');
            } else {
                document.querySelector('.sat-modal-tab:last-child').classList.add('active');
                document.getElementById('celestrakSection').classList.add('active');
            }
        }

        function addFromTLE() {
            const tleText = document.getElementById('tleInput').value.trim();
            if (!tleText) {
                alert('Please paste TLE data');
                return;
            }

            const lines = tleText.split('\\n').map(l => l.trim()).filter(l => l);
            let added = 0;

            for (let i = 0; i < lines.length; i += 3) {
                if (i + 2 < lines.length) {
                    const name = lines[i];
                    const line1 = lines[i + 1];
                    const line2 = lines[i + 2];

                    if (line1.startsWith('1 ') && line2.startsWith('2 ')) {
                        const norad = line1.substring(2, 7).trim();
                        const id = name.replace(/[^a-zA-Z0-9]/g, '-').toUpperCase();

                        // Check if already exists
                        if (!trackedSatellites.find(s => s.norad === norad)) {
                            trackedSatellites.push({
                                id: id,
                                name: name,
                                norad: norad,
                                builtin: false,
                                checked: true,
                                tle: [name, line1, line2]
                            });
                            added++;
                        }
                    }
                }
            }

            if (added > 0) {
                renderSatelliteList();
                document.getElementById('tleInput').value = '';
                closeSatModal();
                showInfo(`Added ${added} satellite(s)`);
            } else {
                alert('No valid TLE data found. Format: Name, Line 1, Line 2 (3 lines per satellite)');
            }
        }

        function fetchCelestrak() {
            showAddSatelliteModal();
            switchSatModalTab('celestrak');
        }

        function fetchCelestrakCategory(category) {
            const status = document.getElementById('celestrakStatus');
            status.innerHTML = '<span style="color: var(--accent-cyan);">Fetching ' + category + '...</span>';

            fetch('/satellite/celestrak/' + category)
                .then(r => r.json())
                .then(data => {
                    if (data.status === 'success' && data.satellites) {
                        let added = 0;
                        data.satellites.forEach(sat => {
                            if (!trackedSatellites.find(s => s.norad === sat.norad)) {
                                trackedSatellites.push({
                                    id: sat.id,
                                    name: sat.name,
                                    norad: sat.norad,
                                    builtin: false,
                                    checked: false, // Don't auto-select
                                    tle: sat.tle
                                });
                                added++;
                            }
                        });
                        renderSatelliteList();
                        status.innerHTML = `<span style="color: var(--accent-green);">Added ${added} satellites (${data.satellites.length} total in category)</span>`;
                    } else {
                        status.innerHTML = `<span style="color: var(--accent-red);">Error: ${data.message || 'Failed to fetch'}</span>`;
                    }
                })
                .catch(err => {
                    status.innerHTML = `<span style="color: var(--accent-red);">Network error</span>`;
                });
        }

        // Initialize satellite list when satellite mode is loaded
        function initSatelliteList() {
            renderSatelliteList();
        }

        function popoutSatellite() {
            document.getElementById('satellitePopout').classList.add('active');
            document.body.style.overflow = 'hidden';

            // Initialize popout canvas
            setTimeout(() => {
                const canvas = document.getElementById('polarPlotCanvasPopout');
                if (canvas) {
                    const container = canvas.parentElement;
                    const size = Math.min(container.offsetWidth, container.offsetHeight - 50);
                    canvas.width = size;
                    canvas.height = size;
                    drawPolarPlotPopout(selectedPass);
                }

                // Render pass list in popout with working click handlers
                renderPassListPopout();

                // Show countdown in popout if passes exist
                if (satellitePasses.length > 0) {
                    document.getElementById('satelliteCountdownPopout').style.display = 'block';
                    updateCountdownDisplay('Popout');
                }
            }, 100);
        }

        function renderPassListPopout() {
            const container = document.getElementById('passListPopout');
            container.innerHTML = '';

            if (satellitePasses.length === 0) {
                container.innerHTML = '<div style="color: #666; text-align: center; padding: 30px;">No passes found.</div>';
                return;
            }

            satellitePasses.forEach((pass, index) => {
                const card = document.createElement('div');
                card.className = 'pass-card' + (pass === selectedPass ? ' active' : '');
                card.onclick = () => selectPassPopout(index);

                const quality = pass.maxEl >= 60 ? 'excellent' : pass.maxEl >= 30 ? 'good' : 'fair';

                card.innerHTML = `
                    <div class="pass-satellite">${pass.satellite}</div>
                    <div class="pass-time">${pass.startTime}</div>
                    <div class="pass-details">
                        <div>Max El: <span>${pass.maxEl}°</span></div>
                        <div>Duration: <span>${pass.duration}m</span></div>
                        <div class="pass-quality ${quality}">${quality.toUpperCase()}</div>
                    </div>
                `;
                container.appendChild(card);
            });
        }

        function selectPassPopout(index) {
            selectedPass = satellitePasses[index];
            selectedPassIndex = index;

            // Update active state in popout
            document.querySelectorAll('#passListPopout .pass-card').forEach((card, i) => {
                card.classList.toggle('active', i === index);
            });

            // Also update main list
            document.querySelectorAll('#passList .pass-card').forEach((card, i) => {
                card.classList.toggle('active', i === index);
            });

            // Update polar plot in popout
            drawPolarPlotPopout(selectedPass);

            // Update countdown
            updateSatelliteCountdown();
        }

        function closeSatellitePopout() {
            document.getElementById('satellitePopout').classList.remove('active');
            document.body.style.overflow = '';
        }

        function drawPolarPlotPopout(pass) {
            const canvas = document.getElementById('polarPlotCanvasPopout');
            if (!canvas) return;
            // Same as drawPolarPlot but for popout canvas
            const ctx = canvas.getContext('2d');
            const size = canvas.width;
            const cx = size / 2;
            const cy = size / 2;
            const radius = size / 2 - 40;

            ctx.fillStyle = '#0a0a0a';
            ctx.fillRect(0, 0, size, size);

            // Elevation rings
            ctx.strokeStyle = 'rgba(0, 255, 255, 0.2)';
            ctx.lineWidth = 1;
            for (let el = 0; el <= 90; el += 15) {
                const r = radius * (90 - el) / 90;
                ctx.beginPath();
                ctx.arc(cx, cy, r, 0, Math.PI * 2);
                ctx.stroke();
                if (el > 0 && el % 30 === 0) {
                    ctx.fillStyle = '#444';
                    ctx.font = '12px JetBrains Mono';
                    ctx.textAlign = 'center';
                    ctx.fillText(el + '°', cx, cy - r + 14);
                }
            }

            // Azimuth lines
            for (let az = 0; az < 360; az += 30) {
                const rad = az * Math.PI / 180;
                ctx.beginPath();
                ctx.moveTo(cx, cy);
                ctx.lineTo(cx + Math.sin(rad) * radius, cy - Math.cos(rad) * radius);
                ctx.stroke();
            }

            // Cardinals
            ctx.fillStyle = '#00ffff';
            ctx.font = 'bold 16px Rajdhani';
            ctx.textAlign = 'center';
            ctx.fillText('N', cx, cy - radius - 12);
            ctx.fillStyle = '#888';
            ctx.fillText('S', cx, cy + radius + 20);
            ctx.fillText('E', cx + radius + 16, cy + 5);
            ctx.fillText('W', cx - radius - 16, cy + 5);

            ctx.fillStyle = '#00ffff';
            ctx.beginPath();
            ctx.arc(cx, cy, 4, 0, Math.PI * 2);
            ctx.fill();

            if (pass && pass.trajectory) {
                ctx.strokeStyle = pass.color || '#00ff00';
                ctx.lineWidth = 3;
                ctx.setLineDash([8, 4]);
                ctx.beginPath();
                pass.trajectory.forEach((point, i) => {
                    const r = radius * (90 - point.elevation) / 90;
                    const rad = point.azimuth * Math.PI / 180;
                    const x = cx + Math.sin(rad) * r;
                    const y = cy - Math.cos(rad) * r;
                    if (i === 0) ctx.moveTo(x, y);
                    else ctx.lineTo(x, y);
                });
                ctx.stroke();
                ctx.setLineDash([]);

                const maxPoint = pass.trajectory.reduce((max, p) => p.elevation > max.elevation ? p : max, { elevation: 0 });
                const maxR = radius * (90 - maxPoint.elevation) / 90;
                const maxRad = maxPoint.azimuth * Math.PI / 180;
                ctx.fillStyle = pass.color || '#00ff00';
                ctx.beginPath();
                ctx.arc(cx + Math.sin(maxRad) * maxR, cy - Math.cos(maxRad) * maxR, 8, 0, Math.PI * 2);
                ctx.fill();

                ctx.fillStyle = '#fff';
                ctx.font = '14px JetBrains Mono';
                ctx.fillText(pass.satellite, cx + Math.sin(maxRad) * maxR + 15, cy - Math.cos(maxRad) * maxR - 10);
            }
        }

        // Iridium functions
        function checkIridiumTools() {
            fetch('/iridium/tools')
                .then(r => r.json())
                .then(data => {
                    const status = document.getElementById('iridiumExtractorStatus');
                    status.textContent = data.available ? 'OK' : 'Not found';
                    status.className = 'tool-status ' + (data.available ? 'ok' : 'missing');
                });
        }

        function startIridiumCapture() {
            const freq = document.getElementById('iridiumFreq').value;
            const gain = document.getElementById('iridiumGain').value;
            const sampleRate = document.getElementById('iridiumSampleRate').value;
            const device = getSelectedDevice();

            fetch('/iridium/start', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ freq, gain, sampleRate, device })
            })
            .then(r => r.json())
            .then(data => {
                if (data.status === 'started') {
                    isIridiumRunning = true;
                    document.getElementById('startIridiumBtn').style.display = 'none';
                    document.getElementById('stopIridiumBtn').style.display = 'block';
                    document.getElementById('statusDot').className = 'status-dot active';
                    document.getElementById('statusText').textContent = 'Iridium Capture';
                    startIridiumStream();
                } else {
                    alert('Error: ' + data.message);
                }
            });
        }

        function stopIridiumCapture() {
            fetch('/iridium/stop', { method: 'POST' })
                .then(r => r.json())
                .then(data => {
                    isIridiumRunning = false;
                    document.getElementById('startIridiumBtn').style.display = 'block';
                    document.getElementById('stopIridiumBtn').style.display = 'none';
                    document.getElementById('statusDot').className = 'status-dot';
                    document.getElementById('statusText').textContent = 'Idle';
                    if (iridiumEventSource) {
                        iridiumEventSource.close();
                        iridiumEventSource = null;
                    }
                });
        }

        function startIridiumStream() {
            if (iridiumEventSource) iridiumEventSource.close();
            iridiumEventSource = new EventSource('/iridium/stream');

            iridiumEventSource.onmessage = function(e) {
                const data = JSON.parse(e.data);
                if (data.type === 'burst') {
                    iridiumBursts.unshift(data);
                    document.getElementById('burstCount').textContent = iridiumBursts.length;
                    addBurstToLog(data);
                }
            };
        }

        function addBurstToLog(burst) {
            const container = document.getElementById('burstList');
            const placeholder = container.querySelector('div[style*="color: #666"]');
            if (placeholder) placeholder.remove();

            const card = document.createElement('div');
            card.className = 'burst-card';
            card.innerHTML = `
                <div class="burst-time">${burst.time}</div>
                <div class="burst-freq">${burst.frequency} MHz</div>
                <div class="burst-data">${burst.data || 'No payload data'}</div>
            `;
            container.insertBefore(card, container.firstChild);

            while (container.children.length > 100) {
                container.removeChild(container.lastChild);
            }
        }

        function clearIridiumLog() {
            iridiumBursts = [];
            document.getElementById('burstCount').textContent = '0';
            document.getElementById('burstList').innerHTML = '<div style="color: #666; text-align: center; padding: 30px; font-size: 11px;">Iridium bursts will appear here when detected.</div>';
        }

        // Utility function
        function showInfo(message) {
            // Simple notification - could be enhanced
            const existing = document.querySelector('.info-toast');
            if (existing) existing.remove();

            const toast = document.createElement('div');
            toast.className = 'info-toast';
            toast.textContent = message;
            toast.style.cssText = 'position: fixed; bottom: 20px; right: 20px; background: var(--accent-cyan); color: #000; padding: 10px 20px; border-radius: 4px; z-index: 10001; font-size: 12px;';
            document.body.appendChild(toast);
            setTimeout(() => toast.remove(), 3000);
        }

        // Theme toggle functions
        function toggleTheme() {
            const html = document.documentElement;
            const currentTheme = html.getAttribute('data-theme');
            const newTheme = currentTheme === 'light' ? 'dark' : 'light';

            if (newTheme === 'dark') {
                html.removeAttribute('data-theme');
            } else {
                html.setAttribute('data-theme', newTheme);
            }

            localStorage.setItem('intercept-theme', newTheme);
        }

        // Load saved theme on page load
        (function() {
            const savedTheme = localStorage.getItem('intercept-theme');
            if (savedTheme === 'light') {
                document.documentElement.setAttribute('data-theme', 'light');
            }
        })();

        // Help modal functions
        function showHelp() {
            document.getElementById('helpModal').classList.add('active');
            document.body.style.overflow = 'hidden';
        }

        function hideHelp() {
            document.getElementById('helpModal').classList.remove('active');
            document.body.style.overflow = '';
        }

        function switchHelpTab(tab) {
            document.querySelectorAll('.help-tab').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.help-section').forEach(s => s.classList.remove('active'));
            document.querySelector(`.help-tab[data-tab="${tab}"]`).classList.add('active');
            document.getElementById(`help-${tab}`).classList.add('active');
        }

        // Keyboard shortcuts for help
        document.addEventListener('keydown', function(e) {
            if (e.key === 'Escape') hideHelp();
            // Open help with F1 or ? key (when not typing in an input)
            if ((e.key === 'F1' || (e.key === '?' && !e.target.matches('input, textarea, select'))) && !document.getElementById('helpModal').classList.contains('active')) {
                e.preventDefault();
                showHelp();
            }
        });
    </script>

    <!-- Help Modal -->
    <div id="helpModal" class="help-modal" onclick="if(event.target === this) hideHelp()">
        <div class="help-content">
            <button class="help-close" onclick="hideHelp()">×</button>
            <h2>📡 INTERCEPT Help</h2>

            <div class="help-tabs">
                <button class="help-tab active" data-tab="icons" onclick="switchHelpTab('icons')">Icons</button>
                <button class="help-tab" data-tab="modes" onclick="switchHelpTab('modes')">Modes</button>
                <button class="help-tab" data-tab="wifi" onclick="switchHelpTab('wifi')">WiFi</button>
                <button class="help-tab" data-tab="tips" onclick="switchHelpTab('tips')">Tips</button>
            </div>

            <!-- Icons Section -->
            <div id="help-icons" class="help-section active">
                <h3>Stats Bar Icons</h3>
                <div class="icon-grid">
                    <div class="icon-item"><span class="icon">📟</span><span class="desc">POCSAG messages decoded</span></div>
                    <div class="icon-item"><span class="icon">📠</span><span class="desc">FLEX messages decoded</span></div>
                    <div class="icon-item"><span class="icon">📨</span><span class="desc">Total messages received</span></div>
                    <div class="icon-item"><span class="icon">🌡️</span><span class="desc">Unique sensors detected</span></div>
                    <div class="icon-item"><span class="icon">📊</span><span class="desc">Device types found</span></div>
                    <div class="icon-item"><span class="icon">✈️</span><span class="desc">Aircraft being tracked</span></div>
                    <div class="icon-item"><span class="icon">🛰️</span><span class="desc">Satellites monitored</span></div>
                    <div class="icon-item"><span class="icon">📡</span><span class="desc">WiFi Access Points</span></div>
                    <div class="icon-item"><span class="icon">👤</span><span class="desc">Connected WiFi clients</span></div>
                    <div class="icon-item"><span class="icon">🤝</span><span class="desc">Captured handshakes</span></div>
                    <div class="icon-item"><span class="icon">🚁</span><span class="desc">Detected drones (click for details)</span></div>
                    <div class="icon-item"><span class="icon">⚠️</span><span class="desc">Rogue APs (click for details)</span></div>
                    <div class="icon-item"><span class="icon">🔵</span><span class="desc">Bluetooth devices</span></div>
                    <div class="icon-item"><span class="icon">📍</span><span class="desc">BLE beacons detected</span></div>
                </div>

                <h3>Mode Tab Icons</h3>
                <div class="icon-grid">
                    <div class="icon-item"><span class="icon">📟</span><span class="desc">Pager - POCSAG/FLEX decoder</span></div>
                    <div class="icon-item"><span class="icon">📡</span><span class="desc">433MHz - Sensor decoder</span></div>
                    <div class="icon-item"><span class="icon">✈️</span><span class="desc">Aircraft - ADS-B tracker</span></div>
                    <div class="icon-item"><span class="icon">🛰️</span><span class="desc">Satellite - Pass prediction</span></div>
                    <div class="icon-item"><span class="icon">📶</span><span class="desc">WiFi - Network scanner</span></div>
                    <div class="icon-item"><span class="icon">🔵</span><span class="desc">Bluetooth - BT/BLE scanner</span></div>
                </div>
            </div>

            <!-- Modes Section -->
            <div id="help-modes" class="help-section">
                <h3>📟 Pager Mode</h3>
                <ul class="tip-list">
                    <li>Decodes POCSAG and FLEX pager signals using RTL-SDR</li>
                    <li>Set frequency to local pager frequencies (common: 152-158 MHz)</li>
                    <li>Messages are displayed in real-time as they're decoded</li>
                    <li>Use presets for common pager frequencies</li>
                </ul>

                <h3>📡 433MHz Sensor Mode</h3>
                <ul class="tip-list">
                    <li>Decodes wireless sensors on 433.92 MHz ISM band</li>
                    <li>Detects temperature, humidity, weather stations, tire pressure monitors</li>
                    <li>Supports many common protocols (Acurite, LaCrosse, Oregon Scientific, etc.)</li>
                    <li>Device intelligence builds profiles of recurring devices</li>
                </ul>

                <h3>✈️ Aircraft Mode</h3>
                <ul class="tip-list">
                    <li>Tracks aircraft via ADS-B using dump1090 or rtl_adsb</li>
                    <li>Interactive map with real OpenStreetMap tiles</li>
                    <li>Click aircraft markers to see callsign, altitude, speed, heading</li>
                    <li>Map auto-fits to show all tracked aircraft</li>
                    <li>Emergency squawk codes highlighted in red</li>
                </ul>

                <h3>🛰️ Satellite Mode</h3>
                <ul class="tip-list">
                    <li>Track satellites using TLE (Two-Line Element) data</li>
                    <li>Add satellites manually or fetch from Celestrak by category</li>
                    <li>Categories: Amateur, Weather, ISS, Starlink, GPS, and more</li>
                    <li>View next pass predictions with elevation and duration</li>
                    <li>Monitor for Iridium satellite bursts</li>
                </ul>

                <h3>📶 WiFi Mode</h3>
                <ul class="tip-list">
                    <li>Requires a WiFi adapter capable of monitor mode</li>
                    <li>Click "Enable Monitor" to put adapter in monitor mode</li>
                    <li>Scans all channels or lock to a specific channel</li>
                    <li>Detects drones by SSID patterns and manufacturer OUI</li>
                    <li>Rogue AP detection flags same SSID on multiple BSSIDs</li>
                    <li>Click network rows to target for deauth or handshake capture</li>
                </ul>

                <h3>🔵 Bluetooth Mode</h3>
                <ul class="tip-list">
                    <li>Scans for classic Bluetooth and BLE devices</li>
                    <li>Shows device names, addresses, and signal strength</li>
                    <li>Manufacturer lookup from MAC address OUI</li>
                    <li>Radar visualization shows device proximity</li>
                </ul>
            </div>

            <!-- WiFi Section -->
            <div id="help-wifi" class="help-section">
                <h3>Monitor Mode</h3>
                <ul class="tip-list">
                    <li><strong>Enable Monitor:</strong> Puts WiFi adapter in monitor mode for passive scanning</li>
                    <li><strong>Kill Processes:</strong> Optional - stops NetworkManager/wpa_supplicant (may drop other connections)</li>
                    <li>Some adapters rename when entering monitor mode (e.g., wlan0 → wlan0mon)</li>
                </ul>

                <h3>Handshake Capture</h3>
                <ul class="tip-list">
                    <li>Click "Capture" on a network to start targeted handshake capture</li>
                    <li>Status panel shows capture progress and file location</li>
                    <li>Use deauth to force clients to reconnect (only on authorized networks!)</li>
                    <li>Handshake files saved to /tmp/intercept_handshake_*.cap</li>
                </ul>

                <h3>Drone Detection</h3>
                <ul class="tip-list">
                    <li>Drones detected by SSID patterns (DJI, Parrot, Autel, etc.)</li>
                    <li>Also detected by manufacturer OUI in MAC address</li>
                    <li>Distance estimated from signal strength (approximate)</li>
                    <li>Click drone count in stats bar to see all detected drones</li>
                </ul>

                <h3>Rogue AP Detection</h3>
                <ul class="tip-list">
                    <li>Flags networks where same SSID appears on multiple BSSIDs</li>
                    <li>Could indicate evil twin attack or legitimate multi-AP setup</li>
                    <li>Click rogue count to see which SSIDs are flagged</li>
                </ul>

                <h3>Proximity Alerts</h3>
                <ul class="tip-list">
                    <li>Add MAC addresses to watch list for alerts when detected</li>
                    <li>Watch list persists in browser localStorage</li>
                    <li>Useful for tracking specific devices</li>
                </ul>

                <h3>Client Probe Analysis</h3>
                <ul class="tip-list">
                    <li>Shows what networks client devices are looking for</li>
                    <li>Orange highlights indicate sensitive/private network names</li>
                    <li>Reveals user location history (home, work, hotels, airports)</li>
                    <li>Useful for security awareness and pen test reports</li>
                </ul>
            </div>

            <!-- Tips Section -->
            <div id="help-tips" class="help-section">
                <h3>General Tips</h3>
                <ul class="tip-list">
                    <li><strong>Collapsible sections:</strong> Click any section header (▼) to collapse/expand</li>
                    <li><strong>Sound alerts:</strong> Toggle sound on/off in settings for each mode</li>
                    <li><strong>Export data:</strong> Use export buttons to save captured data as JSON</li>
                    <li><strong>Device Intelligence:</strong> Tracks device patterns over time</li>
                    <li><strong>Theme toggle:</strong> Click 🌙/☀️ button in header to switch dark/light mode</li>
                </ul>

                <h3>Keyboard Shortcuts</h3>
                <ul class="tip-list">
                    <li><strong>F1</strong> - Open this help page</li>
                    <li><strong>?</strong> - Open help (when not typing in a field)</li>
                    <li><strong>Escape</strong> - Close help and modal dialogs</li>
                </ul>

                <h3>Requirements</h3>
                <ul class="tip-list">
                    <li><strong>Pager/433MHz:</strong> RTL-SDR dongle, rtl_fm, multimon-ng, rtl_433</li>
                    <li><strong>Aircraft:</strong> RTL-SDR dongle, dump1090 or rtl_adsb</li>
                    <li><strong>Satellite:</strong> Internet connection for Celestrak (optional)</li>
                    <li><strong>WiFi:</strong> Monitor-mode capable adapter, aircrack-ng suite</li>
                    <li><strong>Bluetooth:</strong> Bluetooth adapter, hcitool/bluetoothctl</li>
                    <li>Run as root/sudo for full functionality</li>
                </ul>

                <h3>Legal Notice</h3>
                <ul class="tip-list">
                    <li>Only use on networks and devices you own or have authorization to test</li>
                    <li>Passive monitoring may be legal; active attacks require authorization</li>
                    <li>Check local laws regarding radio frequency monitoring</li>
                </ul>
            </div>
        </div>
    </div>

    <!-- Dependencies Modal -->
    <div id="depsModal" class="help-modal" onclick="if(event.target === this) hideDependencies()">
        <div class="help-content" style="max-width: 800px;">
            <button class="help-close" onclick="hideDependencies()">×</button>
            <h2>🔧 Tool Dependencies</h2>
            <p style="color: var(--text-dim); margin-bottom: 15px;">Check which tools are installed for each mode. <span style="color: var(--accent-green);">●</span> = Installed, <span style="color: var(--accent-red);">●</span> = Missing</p>
            <div id="depsContent" style="max-height: 60vh; overflow-y: auto;">
                <div style="text-align: center; padding: 40px; color: var(--text-dim);">
                    Loading dependencies...
                </div>
            </div>
            <div style="margin-top: 20px; padding-top: 15px; border-top: 1px solid var(--border-color);">
                <h3 style="margin-bottom: 10px;">Quick Install (Debian/Ubuntu)</h3>
                <div style="background: var(--bg-tertiary); padding: 10px; border-radius: 4px; font-family: monospace; font-size: 11px; overflow-x: auto;">
                    <div>sudo apt install rtl-sdr multimon-ng rtl-433 aircrack-ng bluez dump1090-mutability hcxtools</div>
                    <div style="margin-top: 5px;">pip install skyfield flask</div>
                </div>
            </div>
        </div>
    </div>

    <script>
        function showDependencies() {
            document.getElementById('depsModal').classList.add('active');
            loadDependencies();
        }

        function hideDependencies() {
            document.getElementById('depsModal').classList.remove('active');
        }

        function loadDependencies() {
            const content = document.getElementById('depsContent');
            content.innerHTML = '<div style="text-align: center; padding: 40px; color: var(--text-dim);">Loading dependencies...</div>';

            fetch('/dependencies')
                .then(r => r.json())
                .then(data => {
                    if (data.status !== 'success') {
                        content.innerHTML = '<div style="color: var(--accent-red);">Error loading dependencies</div>';
                        return;
                    }

                    let html = '';
                    let totalMissing = 0;

                    for (const [modeKey, mode] of Object.entries(data.modes)) {
                        const statusColor = mode.ready ? 'var(--accent-green)' : 'var(--accent-red)';
                        const statusIcon = mode.ready ? '✓' : '✗';

                        html += `
                            <div style="background: var(--bg-tertiary); border-radius: 8px; padding: 15px; margin-bottom: 15px; border-left: 3px solid ${statusColor};">
                                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
                                    <h3 style="margin: 0; color: var(--accent-cyan);">${mode.name}</h3>
                                    <span style="color: ${statusColor}; font-weight: bold;">${statusIcon} ${mode.ready ? 'Ready' : 'Missing Required'}</span>
                                </div>
                                <div style="display: grid; gap: 8px;">
                        `;

                        for (const [toolName, tool] of Object.entries(mode.tools)) {
                            const installed = tool.installed;
                            const dotColor = installed ? 'var(--accent-green)' : 'var(--accent-red)';
                            const requiredBadge = tool.required ? '<span style="background: var(--accent-orange); color: #000; padding: 1px 5px; border-radius: 3px; font-size: 9px; margin-left: 5px;">REQUIRED</span>' : '';

                            if (!installed) totalMissing++;

                            // Get install command for current OS
                            let installCmd = '';
                            if (tool.install) {
                                if (tool.install.pip) {
                                    installCmd = tool.install.pip;
                                } else if (data.pkg_manager && tool.install[data.pkg_manager]) {
                                    installCmd = tool.install[data.pkg_manager];
                                } else if (tool.install.manual) {
                                    installCmd = tool.install.manual;
                                }
                            }

                            html += `
                                <div style="display: flex; align-items: center; gap: 10px; padding: 8px; background: var(--bg-secondary); border-radius: 4px;">
                                    <span style="color: ${dotColor}; font-size: 16px;">●</span>
                                    <div style="flex: 1;">
                                        <div style="font-weight: bold;">${toolName}${requiredBadge}</div>
                                        <div style="font-size: 11px; color: var(--text-dim);">${tool.description}</div>
                                    </div>
                                    ${!installed && installCmd ? `
                                        <code style="font-size: 10px; background: var(--bg-tertiary); padding: 4px 8px; border-radius: 3px; max-width: 250px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${installCmd}">${installCmd}</code>
                                    ` : ''}
                                    <span style="font-size: 11px; color: ${dotColor}; font-weight: bold;">${installed ? 'OK' : 'MISSING'}</span>
                                </div>
                            `;
                        }

                        html += '</div></div>';
                    }

                    // Summary at top
                    const summaryHtml = `
                        <div style="background: ${totalMissing > 0 ? 'rgba(255, 100, 0, 0.1)' : 'rgba(0, 255, 100, 0.1)'}; border: 1px solid ${totalMissing > 0 ? 'var(--accent-orange)' : 'var(--accent-green)'}; border-radius: 8px; padding: 15px; margin-bottom: 20px;">
                            <div style="font-size: 16px; font-weight: bold; color: ${totalMissing > 0 ? 'var(--accent-orange)' : 'var(--accent-green)'};">
                                ${totalMissing > 0 ? '⚠️ ' + totalMissing + ' tool(s) not found' : '✓ All tools installed'}
                            </div>
                            <div style="font-size: 12px; color: var(--text-dim); margin-top: 5px;">
                                OS: ${data.os} | Package Manager: ${data.pkg_manager}
                            </div>
                        </div>
                    `;

                    content.innerHTML = summaryHtml + html;

                    // Update button indicator
                    const btn = document.getElementById('depsBtn');
                    if (btn) {
                        btn.style.color = totalMissing > 0 ? 'var(--accent-orange)' : 'var(--accent-green)';
                    }
                })
                .catch(err => {
                    content.innerHTML = '<div style="color: var(--accent-red);">Error loading dependencies: ' + err.message + '</div>';
                });
        }

        // Check dependencies on page load
        document.addEventListener('DOMContentLoaded', function() {
            // Check if user dismissed the startup check
            const dismissed = localStorage.getItem('depsCheckDismissed');

            // Quick check for missing dependencies
            fetch('/dependencies')
                .then(r => r.json())
                .then(data => {
                    if (data.status === 'success') {
                        let missingModes = 0;
                        let missingTools = [];

                        for (const [modeKey, mode] of Object.entries(data.modes)) {
                            if (!mode.ready) {
                                missingModes++;
                                mode.missing_required.forEach(tool => {
                                    if (!missingTools.includes(tool)) {
                                        missingTools.push(tool);
                                    }
                                });
                            }
                        }

                        const btn = document.getElementById('depsBtn');
                        if (btn && missingModes > 0) {
                            btn.style.color = 'var(--accent-orange)';
                            btn.title = missingModes + ' mode(s) have missing tools - click to see details';
                        }

                        // Show startup prompt if tools are missing and not dismissed
                        if (missingModes > 0 && !dismissed) {
                            showStartupDepsPrompt(missingModes, missingTools.length);
                        }
                    }
                });
        });

        function showStartupDepsPrompt(modeCount, toolCount) {
            const notice = document.createElement('div');
            notice.id = 'startupDepsModal';
            notice.style.cssText = `
                position: fixed;
                top: 20px;
                left: 20px;
                z-index: 10000;
                background: var(--bg-secondary);
                border: 1px solid var(--accent-orange);
                border-radius: 8px;
                box-shadow: 0 4px 20px rgba(0, 0, 0, 0.5), 0 0 15px rgba(255, 165, 0, 0.2);
                max-width: 380px;
                animation: slideIn 0.3s ease-out;
            `;
            notice.innerHTML = `
                <style>
                    @keyframes slideIn {
                        from { transform: translateX(-100%); opacity: 0; }
                        to { transform: translateX(0); opacity: 1; }
                    }
                </style>
                <div style="padding: 15px;">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
                        <h3 style="margin: 0; color: var(--accent-orange); font-size: 14px; display: flex; align-items: center; gap: 8px;">
                            <span>🔧</span> Missing Dependencies
                        </h3>
                        <button onclick="closeStartupDeps()" style="background: none; border: none; color: var(--text-dim); cursor: pointer; font-size: 18px; padding: 0; line-height: 1;">&times;</button>
                    </div>
                    <p style="color: var(--text-secondary); margin: 0 0 15px 0; font-size: 13px; line-height: 1.4;">
                        <strong style="color: var(--accent-orange);">${modeCount} mode(s)</strong> require tools that aren't installed.
                    </p>
                    <div style="display: flex; flex-direction: column; gap: 8px;">
                        <button class="action-btn" onclick="closeStartupDeps(); showDependencies();" style="padding: 10px 16px; font-size: 12px;">
                            View Details & Install
                        </button>
                        <label style="display: flex; align-items: center; gap: 8px; font-size: 11px; color: var(--text-dim); cursor: pointer;">
                            <input type="checkbox" id="dontShowAgain" style="cursor: pointer;">
                            Don't show again
                        </label>
                    </div>
                </div>
            `;
            document.body.appendChild(notice);
        }

        function closeStartupDeps() {
            const modal = document.getElementById('startupDepsModal');
            if (modal) {
                if (document.getElementById('dontShowAgain')?.checked) {
                    localStorage.setItem('depsCheckDismissed', 'true');
                }
                modal.remove();
            }
        }
    </script>
</body>
</html>
'''


def check_tool(name):
    """Check if a tool is installed."""
    return shutil.which(name) is not None


# Comprehensive tool dependency definitions
TOOL_DEPENDENCIES = {
    'pager': {
        'name': 'Pager Decoding',
        'tools': {
            'rtl_fm': {
                'required': True,
                'description': 'RTL-SDR FM demodulator',
                'install': {
                    'apt': 'sudo apt install rtl-sdr',
                    'brew': 'brew install librtlsdr',
                    'manual': 'https://osmocom.org/projects/rtl-sdr/wiki'
                }
            },
            'multimon-ng': {
                'required': True,
                'description': 'Digital transmission decoder',
                'install': {
                    'apt': 'sudo apt install multimon-ng',
                    'brew': 'brew install multimon-ng',
                    'manual': 'https://github.com/EliasOewornal/multimon-ng'
                }
            },
            'rtl_test': {
                'required': False,
                'description': 'RTL-SDR device detection',
                'install': {
                    'apt': 'sudo apt install rtl-sdr',
                    'brew': 'brew install librtlsdr',
                    'manual': 'https://osmocom.org/projects/rtl-sdr/wiki'
                }
            }
        }
    },
    'sensor': {
        'name': '433MHz Sensors',
        'tools': {
            'rtl_433': {
                'required': True,
                'description': 'ISM band decoder for sensors, weather stations, TPMS',
                'install': {
                    'apt': 'sudo apt install rtl-433',
                    'brew': 'brew install rtl_433',
                    'manual': 'https://github.com/merbanan/rtl_433'
                }
            }
        }
    },
    'wifi': {
        'name': 'WiFi Reconnaissance',
        'tools': {
            'airmon-ng': {
                'required': True,
                'description': 'Monitor mode controller',
                'install': {
                    'apt': 'sudo apt install aircrack-ng',
                    'brew': 'Not available on macOS',
                    'manual': 'https://aircrack-ng.org'
                }
            },
            'airodump-ng': {
                'required': True,
                'description': 'WiFi network scanner',
                'install': {
                    'apt': 'sudo apt install aircrack-ng',
                    'brew': 'Not available on macOS',
                    'manual': 'https://aircrack-ng.org'
                }
            },
            'aireplay-ng': {
                'required': False,
                'description': 'Deauthentication / packet injection',
                'install': {
                    'apt': 'sudo apt install aircrack-ng',
                    'brew': 'Not available on macOS',
                    'manual': 'https://aircrack-ng.org'
                }
            },
            'aircrack-ng': {
                'required': False,
                'description': 'Handshake verification',
                'install': {
                    'apt': 'sudo apt install aircrack-ng',
                    'brew': 'brew install aircrack-ng',
                    'manual': 'https://aircrack-ng.org'
                }
            },
            'hcxdumptool': {
                'required': False,
                'description': 'PMKID capture tool',
                'install': {
                    'apt': 'sudo apt install hcxdumptool',
                    'brew': 'brew install hcxtools',
                    'manual': 'https://github.com/ZerBea/hcxdumptool'
                }
            },
            'hcxpcapngtool': {
                'required': False,
                'description': 'PMKID hash extractor',
                'install': {
                    'apt': 'sudo apt install hcxtools',
                    'brew': 'brew install hcxtools',
                    'manual': 'https://github.com/ZerBea/hcxtools'
                }
            }
        }
    },
    'bluetooth': {
        'name': 'Bluetooth Scanning',
        'tools': {
            'hcitool': {
                'required': False,
                'description': 'Bluetooth HCI tool (legacy)',
                'install': {
                    'apt': 'sudo apt install bluez',
                    'brew': 'Not available on macOS (use native)',
                    'manual': 'http://www.bluez.org'
                }
            },
            'bluetoothctl': {
                'required': True,
                'description': 'Modern Bluetooth controller',
                'install': {
                    'apt': 'sudo apt install bluez',
                    'brew': 'Not available on macOS (use native)',
                    'manual': 'http://www.bluez.org'
                }
            },
            'hciconfig': {
                'required': False,
                'description': 'Bluetooth adapter configuration',
                'install': {
                    'apt': 'sudo apt install bluez',
                    'brew': 'Not available on macOS',
                    'manual': 'http://www.bluez.org'
                }
            }
        }
    },
    'aircraft': {
        'name': 'Aircraft Tracking (ADS-B)',
        'tools': {
            'dump1090': {
                'required': False,
                'description': 'Mode S / ADS-B decoder (preferred)',
                'install': {
                    'apt': 'sudo apt install dump1090-mutability',
                    'brew': 'brew install dump1090-mutability',
                    'manual': 'https://github.com/flightaware/dump1090'
                },
                'alternatives': ['dump1090-mutability', 'dump1090-fa']
            },
            'rtl_adsb': {
                'required': False,
                'description': 'Simple ADS-B decoder',
                'install': {
                    'apt': 'sudo apt install rtl-sdr',
                    'brew': 'brew install librtlsdr',
                    'manual': 'https://osmocom.org/projects/rtl-sdr/wiki'
                }
            }
        }
    },
    'satellite': {
        'name': 'Satellite Tracking',
        'tools': {
            'skyfield': {
                'required': True,
                'description': 'Python orbital mechanics library',
                'install': {
                    'pip': 'pip install skyfield',
                    'manual': 'https://rhodesmill.org/skyfield/'
                },
                'python_module': True
            }
        }
    },
    'iridium': {
        'name': 'Iridium Monitoring',
        'tools': {
            'iridium-extractor': {
                'required': False,
                'description': 'Iridium burst extractor',
                'install': {
                    'manual': 'https://github.com/muccc/gr-iridium'
                }
            }
        }
    }
}


def check_all_dependencies():
    """Check all tool dependencies and return status."""
    results = {}

    for mode, config in TOOL_DEPENDENCIES.items():
        mode_result = {
            'name': config['name'],
            'tools': {},
            'ready': True,
            'missing_required': []
        }

        for tool, tool_config in config['tools'].items():
            # Check if it's a Python module
            if tool_config.get('python_module'):
                try:
                    __import__(tool)
                    installed = True
                except Exception as e:
                    print(f"[Dependency] Failed to import {tool}: {type(e).__name__}: {e}")
                    installed = False
            else:
                # Check for alternatives
                alternatives = tool_config.get('alternatives', [])
                installed = check_tool(tool) or any(check_tool(alt) for alt in alternatives)

            mode_result['tools'][tool] = {
                'installed': installed,
                'required': tool_config['required'],
                'description': tool_config['description'],
                'install': tool_config['install']
            }

            if tool_config['required'] and not installed:
                mode_result['ready'] = False
                mode_result['missing_required'].append(tool)

        results[mode] = mode_result

    return results


@app.route('/dependencies')
def get_dependencies():
    """Get status of all tool dependencies."""
    import platform

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


def is_valid_mac(mac):
    """Validate MAC address format."""
    import re
    if not mac:
        return False
    return bool(re.match(r'^([0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}$', mac))


def is_valid_channel(channel):
    """Validate WiFi channel number."""
    try:
        ch = int(channel)
        return 1 <= ch <= 200
    except (ValueError, TypeError):
        return False


def detect_devices():
    """Detect RTL-SDR devices."""
    devices = []

    if not check_tool('rtl_test'):
        return devices

    try:
        result = subprocess.run(
            ['rtl_test', '-t'],
            capture_output=True,
            text=True,
            timeout=5
        )
        output = result.stderr + result.stdout

        # Parse device info
        device_pattern = r'(\d+):\s+(.+?)(?:,\s*SN:\s*(\S+))?$'

        for line in output.split('\n'):
            line = line.strip()
            match = re.match(device_pattern, line)
            if match:
                devices.append({
                    'index': int(match.group(1)),
                    'name': match.group(2).strip().rstrip(','),
                    'serial': match.group(3) or 'N/A'
                })

        if not devices:
            found_match = re.search(r'Found (\d+) device', output)
            if found_match:
                count = int(found_match.group(1))
                for i in range(count):
                    devices.append({
                        'index': i,
                        'name': f'RTL-SDR Device {i}',
                        'serial': 'Unknown'
                    })

    except Exception:
        pass

    return devices


def parse_multimon_output(line):
    """Parse multimon-ng output line."""
    # POCSAG formats:
    # POCSAG512: Address: 1234567  Function: 0  Alpha:   Message here
    # POCSAG1200: Address: 1234567  Function: 0  Numeric: 123-456-7890
    # POCSAG2400: Address: 1234567  Function: 0  (no message)
    # FLEX formats:
    # FLEX: NNNN-NN-NN NN:NN:NN NNNN/NN/C NN.NNN [NNNNNNN] ALN Message here
    # FLEX|NNNN-NN-NN|NN:NN:NN|NNNN/NN/C|NN.NNN|NNNNNNN|ALN|Message

    line = line.strip()

    # POCSAG parsing - with message content
    pocsag_match = re.match(
        r'(POCSAG\d+):\s*Address:\s*(\d+)\s+Function:\s*(\d+)\s+(Alpha|Numeric):\s*(.*)',
        line
    )
    if pocsag_match:
        return {
            'protocol': pocsag_match.group(1),
            'address': pocsag_match.group(2),
            'function': pocsag_match.group(3),
            'msg_type': pocsag_match.group(4),
            'message': pocsag_match.group(5).strip() or '[No Message]'
        }

    # POCSAG parsing - address only (no message content)
    pocsag_addr_match = re.match(
        r'(POCSAG\d+):\s*Address:\s*(\d+)\s+Function:\s*(\d+)\s*$',
        line
    )
    if pocsag_addr_match:
        return {
            'protocol': pocsag_addr_match.group(1),
            'address': pocsag_addr_match.group(2),
            'function': pocsag_addr_match.group(3),
            'msg_type': 'Tone',
            'message': '[Tone Only]'
        }

    # FLEX parsing (standard format)
    flex_match = re.match(
        r'FLEX[:\|]\s*[\d\-]+[\s\|]+[\d:]+[\s\|]+([\d/A-Z]+)[\s\|]+([\d.]+)[\s\|]+\[?(\d+)\]?[\s\|]+(\w+)[\s\|]+(.*)',
        line
    )
    if flex_match:
        return {
            'protocol': 'FLEX',
            'address': flex_match.group(3),
            'function': flex_match.group(1),
            'msg_type': flex_match.group(4),
            'message': flex_match.group(5).strip() or '[No Message]'
        }

    # Simple FLEX format
    flex_simple = re.match(r'FLEX:\s*(.+)', line)
    if flex_simple:
        return {
            'protocol': 'FLEX',
            'address': 'Unknown',
            'function': '',
            'msg_type': 'Unknown',
            'message': flex_simple.group(1).strip()
        }

    return None


def stream_decoder(master_fd, process):
    """Stream decoder output to queue using PTY for unbuffered output."""
    global current_process

    try:
        output_queue.put({'type': 'status', 'text': 'started'})

        buffer = ""
        while True:
            try:
                ready, _, _ = select.select([master_fd], [], [], 1.0)
            except Exception:
                break

            if ready:
                try:
                    data = os.read(master_fd, 1024)
                    if not data:
                        break
                    buffer += data.decode('utf-8', errors='replace')

                    while '\n' in buffer:
                        line, buffer = buffer.split('\n', 1)
                        line = line.strip()
                        if not line:
                            continue

                        parsed = parse_multimon_output(line)
                        if parsed:
                            from datetime import datetime
                            parsed['timestamp'] = datetime.now().strftime('%H:%M:%S')
                            output_queue.put({'type': 'message', **parsed})
                            log_message(parsed)
                        else:
                            output_queue.put({'type': 'raw', 'text': line})
                except OSError:
                    break

            if process.poll() is not None:
                break

    except Exception as e:
        output_queue.put({'type': 'error', 'text': str(e)})
    finally:
        try:
            os.close(master_fd)
        except:
            pass
        process.wait()
        output_queue.put({'type': 'status', 'text': 'stopped'})
        with process_lock:
            current_process = None


@app.route('/')
def index():
    tools = {
        'rtl_fm': check_tool('rtl_fm'),
        'multimon': check_tool('multimon-ng'),
        'rtl_433': check_tool('rtl_433')
    }
    devices = detect_devices()
    return render_template_string(HTML_TEMPLATE, tools=tools, devices=devices)


@app.route('/favicon.svg')
def favicon():
    return send_file('favicon.svg', mimetype='image/svg+xml')


@app.route('/devices')
def get_devices():
    return jsonify(detect_devices())


@app.route('/start', methods=['POST'])
def start_decoding():
    global current_process

    with process_lock:
        if current_process:
            return jsonify({'status': 'error', 'message': 'Already running'})

        data = request.json
        freq = data.get('frequency', '929.6125')
        gain = data.get('gain', '0')
        squelch = data.get('squelch', '0')
        ppm = data.get('ppm', '0')
        device = data.get('device', '0')
        protocols = data.get('protocols', ['POCSAG512', 'POCSAG1200', 'POCSAG2400', 'FLEX'])

        # Clear queue
        while not output_queue.empty():
            try:
                output_queue.get_nowait()
            except:
                break

        # Build multimon-ng decoder arguments
        decoders = []
        for proto in protocols:
            if proto == 'POCSAG512':
                decoders.extend(['-a', 'POCSAG512'])
            elif proto == 'POCSAG1200':
                decoders.extend(['-a', 'POCSAG1200'])
            elif proto == 'POCSAG2400':
                decoders.extend(['-a', 'POCSAG2400'])
            elif proto == 'FLEX':
                decoders.extend(['-a', 'FLEX'])

        # Build rtl_fm command
        # rtl_fm -d <device> -f <freq>M -M fm -s 22050 -g <gain> -p <ppm> -l <squelch> - | multimon-ng -t raw -a POCSAG512 -a POCSAG1200 -a FLEX -f alpha -
        rtl_cmd = [
            'rtl_fm',
            '-d', str(device),
            '-f', f'{freq}M',
            '-M', 'fm',
            '-s', '22050',
        ]

        if gain and gain != '0':
            rtl_cmd.extend(['-g', str(gain)])

        if ppm and ppm != '0':
            rtl_cmd.extend(['-p', str(ppm)])

        if squelch and squelch != '0':
            rtl_cmd.extend(['-l', str(squelch)])

        rtl_cmd.append('-')

        multimon_cmd = ['multimon-ng', '-t', 'raw'] + decoders + ['-f', 'alpha', '-']

        # Log the command being run
        full_cmd = ' '.join(rtl_cmd) + ' | ' + ' '.join(multimon_cmd)
        print(f"Running: {full_cmd}")

        try:
            # Create pipe: rtl_fm | multimon-ng
            # Use PTY for multimon-ng to get unbuffered output
            rtl_process = subprocess.Popen(
                rtl_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

            # Start a thread to monitor rtl_fm stderr for errors
            def monitor_rtl_stderr():
                for line in rtl_process.stderr:
                    err_text = line.decode('utf-8', errors='replace').strip()
                    if err_text:
                        print(f"[RTL_FM] {err_text}", flush=True)
                        output_queue.put({'type': 'raw', 'text': f'[rtl_fm] {err_text}'})

            rtl_stderr_thread = threading.Thread(target=monitor_rtl_stderr)
            rtl_stderr_thread.daemon = True
            rtl_stderr_thread.start()

            # Create a pseudo-terminal for multimon-ng output
            # This tricks it into thinking it's connected to a terminal,
            # which disables output buffering
            master_fd, slave_fd = pty.openpty()

            multimon_process = subprocess.Popen(
                multimon_cmd,
                stdin=rtl_process.stdout,
                stdout=slave_fd,
                stderr=slave_fd,
                close_fds=True
            )

            os.close(slave_fd)  # Close slave fd in parent process
            rtl_process.stdout.close()  # Allow rtl_process to receive SIGPIPE

            current_process = multimon_process
            current_process._rtl_process = rtl_process  # Store reference to kill later
            current_process._master_fd = master_fd  # Store for cleanup

            # Start output thread with PTY master fd
            thread = threading.Thread(target=stream_decoder, args=(master_fd, multimon_process))
            thread.daemon = True
            thread.start()

            # Send the command info to the client
            output_queue.put({'type': 'info', 'text': f'Command: {full_cmd}'})

            return jsonify({'status': 'started', 'command': full_cmd})

        except FileNotFoundError as e:
            return jsonify({'status': 'error', 'message': f'Tool not found: {e.filename}'})
        except Exception as e:
            return jsonify({'status': 'error', 'message': str(e)})


@app.route('/stop', methods=['POST'])
def stop_decoding():
    global current_process

    with process_lock:
        if current_process:
            # Kill rtl_fm process first
            if hasattr(current_process, '_rtl_process'):
                try:
                    current_process._rtl_process.terminate()
                    current_process._rtl_process.wait(timeout=2)
                except:
                    try:
                        current_process._rtl_process.kill()
                    except:
                        pass

            # Close PTY master fd
            if hasattr(current_process, '_master_fd'):
                try:
                    os.close(current_process._master_fd)
                except:
                    pass

            # Kill multimon-ng
            current_process.terminate()
            try:
                current_process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                current_process.kill()

            current_process = None
            return jsonify({'status': 'stopped'})

        return jsonify({'status': 'not_running'})


@app.route('/status')
def get_status():
    """Check if decoder is currently running."""
    with process_lock:
        if current_process and current_process.poll() is None:
            return jsonify({'running': True, 'logging': logging_enabled, 'log_file': log_file_path})
        return jsonify({'running': False, 'logging': logging_enabled, 'log_file': log_file_path})


@app.route('/logging', methods=['POST'])
def toggle_logging():
    """Toggle message logging."""
    global logging_enabled, log_file_path
    data = request.json
    if 'enabled' in data:
        logging_enabled = data['enabled']
    if 'log_file' in data and data['log_file']:
        log_file_path = data['log_file']
    return jsonify({'logging': logging_enabled, 'log_file': log_file_path})


def log_message(msg):
    """Log a message to file if logging is enabled."""
    if not logging_enabled:
        return
    try:
        with open(log_file_path, 'a') as f:
            from datetime import datetime
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            f.write(f"{timestamp} | {msg.get('protocol', 'UNKNOWN')} | {msg.get('address', '')} | {msg.get('message', '')}\n")
    except Exception as e:
        print(f"[ERROR] Failed to log message: {e}", flush=True)


@app.route('/killall', methods=['POST'])
def kill_all():
    """Kill all decoder and WiFi processes."""
    global current_process, sensor_process, wifi_process

    killed = []
    processes_to_kill = [
        'rtl_fm', 'multimon-ng', 'rtl_433',
        'airodump-ng', 'aireplay-ng', 'airmon-ng'
    ]

    for proc in processes_to_kill:
        try:
            result = subprocess.run(['pkill', '-f', proc], capture_output=True)
            if result.returncode == 0:
                killed.append(proc)
        except:
            pass

    with process_lock:
        current_process = None

    with sensor_lock:
        sensor_process = None

    with wifi_lock:
        wifi_process = None

    return jsonify({'status': 'killed', 'processes': killed})


@app.route('/stream')
def stream():
    def generate():
        import json
        while True:
            try:
                msg = output_queue.get(timeout=1)
                yield f"data: {json.dumps(msg)}\n\n"
            except queue.Empty:
                yield f"data: {json.dumps({'type': 'keepalive'})}\n\n"

    response = Response(generate(), mimetype='text/event-stream')
    response.headers['Cache-Control'] = 'no-cache'
    response.headers['X-Accel-Buffering'] = 'no'
    response.headers['Connection'] = 'keep-alive'
    return response


# ============== RTL_433 SENSOR ROUTES ==============

def stream_sensor_output(process):
    """Stream rtl_433 JSON output to queue."""
    global sensor_process
    import json as json_module

    try:
        sensor_queue.put({'type': 'status', 'text': 'started'})

        for line in iter(process.stdout.readline, b''):
            line = line.decode('utf-8', errors='replace').strip()
            if not line:
                continue

            try:
                # rtl_433 outputs JSON objects, one per line
                data = json_module.loads(line)
                data['type'] = 'sensor'
                sensor_queue.put(data)

                # Log if enabled
                if logging_enabled:
                    try:
                        with open(log_file_path, 'a') as f:
                            from datetime import datetime
                            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            f.write(f"{timestamp} | {data.get('model', 'Unknown')} | {json_module.dumps(data)}\n")
                    except Exception:
                        pass
            except json_module.JSONDecodeError:
                # Not JSON, send as raw
                sensor_queue.put({'type': 'raw', 'text': line})

    except Exception as e:
        sensor_queue.put({'type': 'error', 'text': str(e)})
    finally:
        process.wait()
        sensor_queue.put({'type': 'status', 'text': 'stopped'})
        with sensor_lock:
            sensor_process = None


@app.route('/start_sensor', methods=['POST'])
def start_sensor():
    global sensor_process

    with sensor_lock:
        if sensor_process:
            return jsonify({'status': 'error', 'message': 'Sensor already running'})

        data = request.json
        freq = data.get('frequency', '433.92')
        gain = data.get('gain', '0')
        ppm = data.get('ppm', '0')
        device = data.get('device', '0')

        # Clear queue
        while not sensor_queue.empty():
            try:
                sensor_queue.get_nowait()
            except:
                break

        # Build rtl_433 command
        # rtl_433 -d <device> -f <freq>M -g <gain> -p <ppm> -F json
        cmd = [
            'rtl_433',
            '-d', str(device),
            '-f', f'{freq}M',
            '-F', 'json'
        ]

        if gain and gain != '0':
            cmd.extend(['-g', str(gain)])

        if ppm and ppm != '0':
            cmd.extend(['-p', str(ppm)])

        full_cmd = ' '.join(cmd)
        print(f"Running: {full_cmd}")

        try:
            sensor_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=1
            )

            # Start output thread
            thread = threading.Thread(target=stream_sensor_output, args=(sensor_process,))
            thread.daemon = True
            thread.start()

            # Monitor stderr
            def monitor_stderr():
                for line in sensor_process.stderr:
                    err = line.decode('utf-8', errors='replace').strip()
                    if err:
                        print(f"[rtl_433] {err}")
                        sensor_queue.put({'type': 'info', 'text': f'[rtl_433] {err}'})

            stderr_thread = threading.Thread(target=monitor_stderr)
            stderr_thread.daemon = True
            stderr_thread.start()

            sensor_queue.put({'type': 'info', 'text': f'Command: {full_cmd}'})

            return jsonify({'status': 'started', 'command': full_cmd})

        except FileNotFoundError:
            return jsonify({'status': 'error', 'message': 'rtl_433 not found. Install with: brew install rtl_433'})
        except Exception as e:
            return jsonify({'status': 'error', 'message': str(e)})


@app.route('/stop_sensor', methods=['POST'])
def stop_sensor():
    global sensor_process

    with sensor_lock:
        if sensor_process:
            sensor_process.terminate()
            try:
                sensor_process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                sensor_process.kill()
            sensor_process = None
            return jsonify({'status': 'stopped'})

        return jsonify({'status': 'not_running'})


@app.route('/stream_sensor')
def stream_sensor():
    def generate():
        import json
        while True:
            try:
                msg = sensor_queue.get(timeout=1)
                yield f"data: {json.dumps(msg)}\n\n"
            except queue.Empty:
                yield f"data: {json.dumps({'type': 'keepalive'})}\n\n"

    response = Response(generate(), mimetype='text/event-stream')
    response.headers['Cache-Control'] = 'no-cache'
    response.headers['X-Accel-Buffering'] = 'no'
    response.headers['Connection'] = 'keep-alive'
    return response


# ============== WIFI RECONNAISSANCE ROUTES ==============

def detect_wifi_interfaces():
    """Detect available WiFi interfaces."""
    interfaces = []
    import platform

    if platform.system() == 'Darwin':  # macOS
        try:
            # Get list of network interfaces
            result = subprocess.run(['networksetup', '-listallhardwareports'],
                                    capture_output=True, text=True, timeout=5)
            lines = result.stdout.split('\n')
            current_device = None
            for i, line in enumerate(lines):
                if 'Wi-Fi' in line or 'AirPort' in line:
                    # Next line should have the device
                    for j in range(i+1, min(i+3, len(lines))):
                        if 'Device:' in lines[j]:
                            device = lines[j].split('Device:')[1].strip()
                            interfaces.append({
                                'name': device,
                                'type': 'internal',
                                'monitor_capable': False,  # macOS internal usually can't
                                'status': 'up'
                            })
                            break
        except Exception as e:
            print(f"[WiFi] Error detecting macOS interfaces: {e}")

        # Check for USB WiFi adapters
        try:
            result = subprocess.run(['system_profiler', 'SPUSBDataType'],
                                    capture_output=True, text=True, timeout=10)
            if 'Wireless' in result.stdout or 'WLAN' in result.stdout or '802.11' in result.stdout:
                interfaces.append({
                    'name': 'USB WiFi Adapter',
                    'type': 'usb',
                    'monitor_capable': True,
                    'status': 'detected'
                })
        except Exception:
            pass

    else:  # Linux
        try:
            # Use iw to list wireless interfaces
            result = subprocess.run(['iw', 'dev'], capture_output=True, text=True, timeout=5)
            current_iface = None
            for line in result.stdout.split('\n'):
                line = line.strip()
                if line.startswith('Interface'):
                    current_iface = line.split()[1]
                elif current_iface and 'type' in line:
                    iface_type = line.split()[-1]
                    interfaces.append({
                        'name': current_iface,
                        'type': iface_type,
                        'monitor_capable': True,
                        'status': 'up'
                    })
                    current_iface = None
        except FileNotFoundError:
            # Try iwconfig instead
            try:
                result = subprocess.run(['iwconfig'], capture_output=True, text=True, timeout=5)
                for line in result.stdout.split('\n'):
                    if 'IEEE 802.11' in line:
                        iface = line.split()[0]
                        interfaces.append({
                            'name': iface,
                            'type': 'managed',
                            'monitor_capable': True,
                            'status': 'up'
                        })
            except Exception:
                pass
        except Exception as e:
            print(f"[WiFi] Error detecting Linux interfaces: {e}")

    return interfaces


@app.route('/wifi/interfaces')
def get_wifi_interfaces():
    """Get available WiFi interfaces."""
    interfaces = detect_wifi_interfaces()
    tools = {
        'airmon': check_tool('airmon-ng'),
        'airodump': check_tool('airodump-ng'),
        'aireplay': check_tool('aireplay-ng'),
        'iw': check_tool('iw')
    }
    return jsonify({'interfaces': interfaces, 'tools': tools, 'monitor_interface': wifi_monitor_interface})


@app.route('/wifi/monitor', methods=['POST'])
def toggle_monitor_mode():
    """Enable or disable monitor mode on an interface."""
    global wifi_monitor_interface

    data = request.json
    interface = data.get('interface')
    action = data.get('action', 'start')  # 'start' or 'stop'

    if not interface:
        return jsonify({'status': 'error', 'message': 'No interface specified'})

    if action == 'start':
        # Try airmon-ng first
        if check_tool('airmon-ng'):
            try:
                import re

                # Get list of wireless interfaces BEFORE enabling monitor mode
                def get_wireless_interfaces():
                    """Get all wireless interface names."""
                    interfaces = set()
                    try:
                        # Try iwconfig first (shows wireless interfaces)
                        result = subprocess.run(['iwconfig'], capture_output=True, text=True, timeout=5)
                        for line in result.stdout.split('\n'):
                            if line and not line.startswith(' ') and 'no wireless' not in line.lower():
                                iface = line.split()[0] if line.split() else None
                                if iface:
                                    interfaces.add(iface)
                    except:
                        pass

                    try:
                        # Also check /sys/class/net for interfaces with wireless dir
                        import os
                        for iface in os.listdir('/sys/class/net'):
                            if os.path.exists(f'/sys/class/net/{iface}/wireless'):
                                interfaces.add(iface)
                    except:
                        pass

                    try:
                        # Also try ip link to find any interface
                        result = subprocess.run(['ip', 'link', 'show'], capture_output=True, text=True, timeout=5)
                        for match in re.finditer(r'^\d+:\s+(\S+):', result.stdout, re.MULTILINE):
                            iface = match.group(1).rstrip(':')
                            # Include interfaces that look like wireless (wl*, wlan*, etc)
                            if iface.startswith('wl') or 'mon' in iface:
                                interfaces.add(iface)
                    except:
                        pass

                    return interfaces

                interfaces_before = get_wireless_interfaces()
                print(f"[WiFi] Interfaces before monitor mode: {interfaces_before}", flush=True)

                # Optionally kill interfering processes (can drop other connections)
                kill_processes = data.get('kill_processes', False)
                if kill_processes:
                    print("[WiFi] Killing interfering processes...", flush=True)
                    subprocess.run(['airmon-ng', 'check', 'kill'], capture_output=True, timeout=10)
                else:
                    print("[WiFi] Skipping process kill (other connections preserved)", flush=True)

                # Start monitor mode
                result = subprocess.run(['airmon-ng', 'start', interface],
                                        capture_output=True, text=True, timeout=15)

                output = result.stdout + result.stderr
                print(f"[WiFi] airmon-ng output:\n{output}", flush=True)

                # Get interfaces AFTER enabling monitor mode
                import time
                time.sleep(1)  # Give system time to register new interface
                interfaces_after = get_wireless_interfaces()
                print(f"[WiFi] Interfaces after monitor mode: {interfaces_after}", flush=True)

                # Find the new interface (the monitor mode one)
                new_interfaces = interfaces_after - interfaces_before
                print(f"[WiFi] New interfaces detected: {new_interfaces}", flush=True)

                # Determine monitor interface
                monitor_iface = None

                # Method 1: New interface appeared
                if new_interfaces:
                    # Prefer interface with 'mon' in name
                    for iface in new_interfaces:
                        if 'mon' in iface:
                            monitor_iface = iface
                            break
                    if not monitor_iface:
                        monitor_iface = list(new_interfaces)[0]

                # Method 2: Parse airmon-ng output
                if not monitor_iface:
                    # Look for various patterns in airmon-ng output
                    patterns = [
                        r'monitor mode.*enabled.*on\s+(\S+)',  # "monitor mode enabled on wlan0mon"
                        r'\(monitor mode.*enabled.*?(\S+mon)\)',  # "(monitor mode enabled on wlan0mon)"
                        r'created\s+(\S+mon)',  # "created wlan0mon"
                        r'\bon\s+(\S+mon)\b',  # "on wlan0mon"
                        r'\b(\S+mon)\b.*monitor',  # "wlan0mon in monitor"
                        r'\b(' + re.escape(interface) + r'mon)\b',  # exact match: interfacemon
                    ]
                    for pattern in patterns:
                        match = re.search(pattern, output, re.IGNORECASE)
                        if match:
                            monitor_iface = match.group(1)
                            print(f"[WiFi] Found monitor interface via pattern '{pattern}': {monitor_iface}", flush=True)
                            break

                # Method 3: Check if original interface now in monitor mode
                if not monitor_iface:
                    # Check if original interface is now in monitor mode
                    try:
                        result = subprocess.run(['iwconfig', interface], capture_output=True, text=True, timeout=5)
                        if 'Mode:Monitor' in result.stdout:
                            monitor_iface = interface
                            print(f"[WiFi] Original interface {interface} is now in monitor mode", flush=True)
                    except:
                        pass

                # Method 4: Check interface + 'mon'
                if not monitor_iface:
                    potential = interface + 'mon'
                    if potential in interfaces_after:
                        monitor_iface = potential

                # Method 5: Last resort - assume interface + 'mon'
                if not monitor_iface:
                    monitor_iface = interface + 'mon'
                    print(f"[WiFi] Assuming monitor interface: {monitor_iface}", flush=True)

                # Verify the interface actually exists
                try:
                    result = subprocess.run(['ip', 'link', 'show', monitor_iface], capture_output=True, text=True, timeout=5)
                    if result.returncode != 0:
                        # Interface doesn't exist - try to find any mon interface
                        for iface in interfaces_after:
                            if 'mon' in iface or iface.startswith('wl'):
                                # Check if it's in monitor mode
                                try:
                                    check = subprocess.run(['iwconfig', iface], capture_output=True, text=True, timeout=5)
                                    if 'Mode:Monitor' in check.stdout:
                                        monitor_iface = iface
                                        print(f"[WiFi] Found working monitor interface: {monitor_iface}", flush=True)
                                        break
                                except:
                                    pass
                except:
                    pass

                wifi_monitor_interface = monitor_iface
                wifi_queue.put({'type': 'info', 'text': f'Monitor mode enabled on {wifi_monitor_interface}'})
                return jsonify({'status': 'success', 'monitor_interface': wifi_monitor_interface})

            except Exception as e:
                import traceback
                print(f"[WiFi] Error enabling monitor mode: {e}\n{traceback.format_exc()}", flush=True)
                return jsonify({'status': 'error', 'message': str(e)})

        # Fallback to iw (Linux)
        elif check_tool('iw'):
            try:
                subprocess.run(['ip', 'link', 'set', interface, 'down'], capture_output=True)
                subprocess.run(['iw', interface, 'set', 'monitor', 'control'], capture_output=True)
                subprocess.run(['ip', 'link', 'set', interface, 'up'], capture_output=True)
                wifi_monitor_interface = interface
                return jsonify({'status': 'success', 'monitor_interface': interface})
            except Exception as e:
                return jsonify({'status': 'error', 'message': str(e)})
        else:
            return jsonify({'status': 'error', 'message': 'No monitor mode tools available. Install aircrack-ng (brew install aircrack-ng) or iw.'})

    else:  # stop
        if check_tool('airmon-ng'):
            try:
                result = subprocess.run(['airmon-ng', 'stop', wifi_monitor_interface or interface],
                                        capture_output=True, text=True, timeout=15)
                wifi_monitor_interface = None
                return jsonify({'status': 'success', 'message': 'Monitor mode disabled'})
            except Exception as e:
                return jsonify({'status': 'error', 'message': str(e)})
        elif check_tool('iw'):
            try:
                subprocess.run(['ip', 'link', 'set', interface, 'down'], capture_output=True)
                subprocess.run(['iw', interface, 'set', 'type', 'managed'], capture_output=True)
                subprocess.run(['ip', 'link', 'set', interface, 'up'], capture_output=True)
                wifi_monitor_interface = None
                return jsonify({'status': 'success', 'message': 'Monitor mode disabled'})
            except Exception as e:
                return jsonify({'status': 'error', 'message': str(e)})

    return jsonify({'status': 'error', 'message': 'Unknown action'})


def parse_airodump_csv(csv_path):
    """Parse airodump-ng CSV output file."""
    networks = {}
    clients = {}

    try:
        with open(csv_path, 'r', errors='replace') as f:
            content = f.read()

        # Split into networks and clients sections
        sections = content.split('\n\n')

        for section in sections:
            lines = section.strip().split('\n')
            if not lines:
                continue

            header = lines[0] if lines else ''

            if 'BSSID' in header and 'ESSID' in header:
                # Networks section
                for line in lines[1:]:
                    parts = [p.strip() for p in line.split(',')]
                    if len(parts) >= 14:
                        bssid = parts[0]
                        if bssid and ':' in bssid:
                            networks[bssid] = {
                                'bssid': bssid,
                                'first_seen': parts[1],
                                'last_seen': parts[2],
                                'channel': parts[3],
                                'speed': parts[4],
                                'privacy': parts[5],
                                'cipher': parts[6],
                                'auth': parts[7],
                                'power': parts[8],
                                'beacons': parts[9],
                                'ivs': parts[10],
                                'lan_ip': parts[11],
                                'essid': parts[13] or 'Hidden'
                            }

            elif 'Station MAC' in header:
                # Clients section
                for line in lines[1:]:
                    parts = [p.strip() for p in line.split(',')]
                    if len(parts) >= 6:
                        station = parts[0]
                        if station and ':' in station:
                            # Lookup vendor from OUI database
                            vendor = get_manufacturer(station)
                            clients[station] = {
                                'mac': station,
                                'first_seen': parts[1],
                                'last_seen': parts[2],
                                'power': parts[3],
                                'packets': parts[4],
                                'bssid': parts[5],
                                'probes': parts[6] if len(parts) > 6 else '',
                                'vendor': vendor
                            }
    except Exception as e:
        print(f"[WiFi] Error parsing CSV: {e}")

    return networks, clients


def stream_airodump_output(process, csv_path):
    """Stream airodump-ng output to queue."""
    global wifi_process, wifi_networks, wifi_clients
    import time
    import select

    try:
        wifi_queue.put({'type': 'status', 'text': 'started'})
        last_parse = 0
        start_time = time.time()
        csv_found = False

        while process.poll() is None:
            # Check for stderr output (non-blocking)
            try:
                import fcntl
                # Make stderr non-blocking
                fd = process.stderr.fileno()
                fl = fcntl.fcntl(fd, fcntl.F_GETFL)
                fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

                stderr_data = process.stderr.read()
                if stderr_data:
                    stderr_text = stderr_data.decode('utf-8', errors='replace').strip()
                    if stderr_text:
                        # Filter out progress updates, report actual errors
                        for line in stderr_text.split('\n'):
                            line = line.strip()
                            if line and not line.startswith('CH') and not line.startswith('Elapsed'):
                                wifi_queue.put({'type': 'error', 'text': f'airodump-ng: {line}'})
            except Exception:
                pass

            # Parse CSV file periodically
            current_time = time.time()
            if current_time - last_parse >= 2:  # Parse every 2 seconds
                csv_file = csv_path + '-01.csv'
                if os.path.exists(csv_file):
                    csv_found = True
                    networks, clients = parse_airodump_csv(csv_file)

                    # Detect new networks
                    for bssid, net in networks.items():
                        if bssid not in wifi_networks:
                            wifi_queue.put({
                                'type': 'network',
                                'action': 'new',
                                **net
                            })
                        else:
                            # Update existing
                            wifi_queue.put({
                                'type': 'network',
                                'action': 'update',
                                **net
                            })

                    # Detect new clients
                    for mac, client in clients.items():
                        if mac not in wifi_clients:
                            wifi_queue.put({
                                'type': 'client',
                                'action': 'new',
                                **client
                            })

                    wifi_networks = networks
                    wifi_clients = clients
                    last_parse = current_time

                if current_time - start_time > 5 and not csv_found:
                    # No CSV after 5 seconds - likely a problem
                    wifi_queue.put({'type': 'error', 'text': 'No scan data after 5 seconds. Check if monitor mode is properly enabled.'})
                    start_time = current_time + 30  # Don't spam this message

            time.sleep(0.5)

        # Process exited - capture any remaining stderr
        try:
            remaining_stderr = process.stderr.read()
            if remaining_stderr:
                stderr_text = remaining_stderr.decode('utf-8', errors='replace').strip()
                if stderr_text:
                    wifi_queue.put({'type': 'error', 'text': f'airodump-ng exited: {stderr_text}'})
        except Exception:
            pass

        # Check exit code
        exit_code = process.returncode
        if exit_code != 0 and exit_code is not None:
            wifi_queue.put({'type': 'error', 'text': f'airodump-ng exited with code {exit_code}'})

    except Exception as e:
        wifi_queue.put({'type': 'error', 'text': str(e)})
    finally:
        process.wait()
        wifi_queue.put({'type': 'status', 'text': 'stopped'})
        with wifi_lock:
            wifi_process = None


@app.route('/wifi/scan/start', methods=['POST'])
def start_wifi_scan():
    """Start WiFi scanning with airodump-ng."""
    global wifi_process, wifi_networks, wifi_clients

    with wifi_lock:
        if wifi_process:
            return jsonify({'status': 'error', 'message': 'Scan already running'})

        data = request.json
        interface = data.get('interface') or wifi_monitor_interface
        channel = data.get('channel')  # None = channel hopping
        band = data.get('band', 'abg')  # 'a' = 5GHz, 'bg' = 2.4GHz, 'abg' = both

        if not interface:
            return jsonify({'status': 'error', 'message': 'No monitor interface available. Enable monitor mode first.'})

        # Clear previous data
        wifi_networks = {}
        wifi_clients = {}

        # Clear queue
        while not wifi_queue.empty():
            try:
                wifi_queue.get_nowait()
            except:
                break

        # Build airodump-ng command
        csv_path = '/tmp/intercept_wifi'

        # Remove old files
        for f in [f'/tmp/intercept_wifi-01.csv', f'/tmp/intercept_wifi-01.cap']:
            try:
                os.remove(f)
            except:
                pass

        cmd = [
            'airodump-ng',
            '-w', csv_path,
            '--output-format', 'csv,pcap',
            '--band', band,
            interface
        ]

        if channel:
            cmd.extend(['-c', str(channel)])

        print(f"[WiFi] Running: {' '.join(cmd)}")

        try:
            wifi_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

            # Wait briefly to check if process fails immediately
            import time
            time.sleep(0.5)

            if wifi_process.poll() is not None:
                # Process already exited - capture error
                stderr_output = wifi_process.stderr.read().decode('utf-8', errors='replace').strip()
                stdout_output = wifi_process.stdout.read().decode('utf-8', errors='replace').strip()
                exit_code = wifi_process.returncode
                wifi_process = None

                error_msg = stderr_output or stdout_output or f'Process exited with code {exit_code}'

                # Strip ANSI escape codes
                import re
                error_msg = re.sub(r'\x1b\[[0-9;]*m', '', error_msg)

                # Common error explanations
                if 'No such device' in error_msg or 'No such interface' in error_msg:
                    error_msg = f'Interface "{interface}" not found. Make sure monitor mode is enabled.'
                elif 'Operation not permitted' in error_msg:
                    error_msg = 'Permission denied. Try running with sudo.'
                elif 'monitor mode' in error_msg.lower():
                    error_msg = f'Interface "{interface}" is not in monitor mode. Enable monitor mode first.'
                elif 'Failed initialising' in error_msg:
                    error_msg = f'Failed to initialize "{interface}". The adapter may have been disconnected or monitor mode is not active. Try disabling and re-enabling monitor mode.'

                return jsonify({'status': 'error', 'message': error_msg})

            # Start parsing thread
            thread = threading.Thread(target=stream_airodump_output, args=(wifi_process, csv_path))
            thread.daemon = True
            thread.start()

            wifi_queue.put({'type': 'info', 'text': f'Started scanning on {interface}'})

            return jsonify({'status': 'started', 'interface': interface})

        except FileNotFoundError:
            return jsonify({'status': 'error', 'message': 'airodump-ng not found. Install aircrack-ng suite (brew install aircrack-ng).'})
        except Exception as e:
            return jsonify({'status': 'error', 'message': str(e)})


@app.route('/wifi/scan/stop', methods=['POST'])
def stop_wifi_scan():
    """Stop WiFi scanning."""
    global wifi_process

    with wifi_lock:
        if wifi_process:
            wifi_process.terminate()
            try:
                wifi_process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                wifi_process.kill()
            wifi_process = None
            return jsonify({'status': 'stopped'})
        return jsonify({'status': 'not_running'})


@app.route('/wifi/deauth', methods=['POST'])
def send_deauth():
    """Send deauthentication packets to force handshake capture."""
    data = request.json
    target_bssid = data.get('bssid')
    target_client = data.get('client', 'FF:FF:FF:FF:FF:FF')  # Broadcast by default
    count = data.get('count', 5)
    interface = data.get('interface') or wifi_monitor_interface

    if not target_bssid:
        return jsonify({'status': 'error', 'message': 'Target BSSID required'})

    # Validate MAC addresses to prevent command injection
    if not is_valid_mac(target_bssid):
        return jsonify({'status': 'error', 'message': 'Invalid BSSID format'})

    if not is_valid_mac(target_client):
        return jsonify({'status': 'error', 'message': 'Invalid client MAC format'})

    # Validate count to prevent abuse
    try:
        count = int(count)
        if count < 1 or count > 100:
            count = 5
    except (ValueError, TypeError):
        count = 5

    if not interface:
        return jsonify({'status': 'error', 'message': 'No monitor interface'})

    if not check_tool('aireplay-ng'):
        return jsonify({'status': 'error', 'message': 'aireplay-ng not found'})

    try:
        # aireplay-ng --deauth <count> -a <AP BSSID> -c <client> <interface>
        cmd = [
            'aireplay-ng',
            '--deauth', str(count),
            '-a', target_bssid,
            '-c', target_client,
            interface
        ]

        wifi_queue.put({'type': 'info', 'text': f'Sending {count} deauth packets to {target_bssid}'})

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

        if result.returncode == 0:
            return jsonify({'status': 'success', 'message': f'Sent {count} deauth packets'})
        else:
            return jsonify({'status': 'error', 'message': result.stderr})

    except subprocess.TimeoutExpired:
        return jsonify({'status': 'success', 'message': 'Deauth sent (timed out waiting for completion)'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})


@app.route('/wifi/handshake/capture', methods=['POST'])
def capture_handshake():
    """Start targeted handshake capture."""
    global wifi_process

    data = request.json
    target_bssid = data.get('bssid')
    channel = data.get('channel')
    interface = data.get('interface') or wifi_monitor_interface

    if not target_bssid or not channel:
        return jsonify({'status': 'error', 'message': 'BSSID and channel required'})

    # Validate inputs to prevent command injection
    if not is_valid_mac(target_bssid):
        return jsonify({'status': 'error', 'message': 'Invalid BSSID format'})

    if not is_valid_channel(channel):
        return jsonify({'status': 'error', 'message': 'Invalid channel'})

    with wifi_lock:
        if wifi_process:
            return jsonify({'status': 'error', 'message': 'Scan already running. Stop it first.'})

        # Safe to use in path after validation
        capture_path = f'/tmp/intercept_handshake_{target_bssid.replace(":", "")}'

        cmd = [
            'airodump-ng',
            '-c', str(channel),
            '--bssid', target_bssid,
            '-w', capture_path,
            '--output-format', 'pcap',
            interface
        ]

        try:
            wifi_process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            wifi_queue.put({'type': 'info', 'text': f'Capturing handshakes for {target_bssid} on channel {channel}'})
            return jsonify({'status': 'started', 'capture_file': capture_path + '-01.cap'})
        except Exception as e:
            return jsonify({'status': 'error', 'message': str(e)})


@app.route('/wifi/handshake/status', methods=['POST'])
def check_handshake_status():
    """Check if a handshake has been captured in the specified file."""
    import os

    data = request.json
    capture_file = data.get('file', '')
    target_bssid = data.get('bssid', '')

    # Security: ensure the file path is in /tmp and looks like our capture files
    if not capture_file.startswith('/tmp/intercept_handshake_') or '..' in capture_file:
        return jsonify({'status': 'error', 'message': 'Invalid capture file path'})

    # Check if file exists
    if not os.path.exists(capture_file):
        # Check if capture is still running
        with wifi_lock:
            if wifi_process and wifi_process.poll() is None:
                return jsonify({
                    'status': 'running',
                    'file_exists': False,
                    'handshake_found': False
                })
            else:
                return jsonify({
                    'status': 'stopped',
                    'file_exists': False,
                    'handshake_found': False
                })

    # File exists - get size
    file_size = os.path.getsize(capture_file)

    # Use aircrack-ng to check if handshake is present
    # aircrack-ng -a 2 -b <BSSID> <capture_file> will show if EAPOL handshake exists
    handshake_found = False
    try:
        if target_bssid and is_valid_mac(target_bssid):
            result = subprocess.run(
                ['aircrack-ng', '-a', '2', '-b', target_bssid, capture_file],
                capture_output=True,
                text=True,
                timeout=10
            )
            # Check output for handshake indicators
            # aircrack-ng shows "1 handshake" if found, or "0 handshake" if not
            output = result.stdout + result.stderr
            if '1 handshake' in output or 'handshake' in output.lower() and 'wpa' in output.lower():
                # Also check it's not "0 handshake"
                if '0 handshake' not in output:
                    handshake_found = True
    except subprocess.TimeoutExpired:
        pass  # aircrack-ng timed out, assume no handshake yet
    except Exception as e:
        print(f"[WiFi] Error checking handshake: {e}", flush=True)

    return jsonify({
        'status': 'running' if wifi_process and wifi_process.poll() is None else 'stopped',
        'file_exists': True,
        'file_size': file_size,
        'file': capture_file,
        'handshake_found': handshake_found
    })


# PMKID Capture using hcxdumptool
pmkid_process = None
pmkid_lock = threading.Lock()

@app.route('/wifi/pmkid/capture', methods=['POST'])
def capture_pmkid():
    """Start PMKID capture using hcxdumptool."""
    global pmkid_process

    data = request.json
    target_bssid = data.get('bssid')
    channel = data.get('channel')
    interface = data.get('interface') or wifi_monitor_interface

    if not target_bssid:
        return jsonify({'status': 'error', 'message': 'BSSID required'})

    if not is_valid_mac(target_bssid):
        return jsonify({'status': 'error', 'message': 'Invalid BSSID format'})

    with pmkid_lock:
        if pmkid_process and pmkid_process.poll() is None:
            return jsonify({'status': 'error', 'message': 'PMKID capture already running'})

        capture_path = f'/tmp/intercept_pmkid_{target_bssid.replace(":", "")}.pcapng'

        # Create filter file for target BSSID
        filter_file = f'/tmp/pmkid_filter_{target_bssid.replace(":", "")}'
        with open(filter_file, 'w') as f:
            f.write(target_bssid.replace(':', '').lower())

        # hcxdumptool command
        cmd = [
            'hcxdumptool',
            '-i', interface,
            '-o', capture_path,
            '--filterlist_ap', filter_file,
            '--filtermode', '2',  # whitelist mode
            '--enable_status', '1'
        ]

        if channel:
            cmd.extend(['-c', str(channel)])

        try:
            pmkid_process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            return jsonify({'status': 'started', 'file': capture_path})
        except FileNotFoundError:
            return jsonify({'status': 'error', 'message': 'hcxdumptool not found. Install with: apt install hcxdumptool'})
        except Exception as e:
            return jsonify({'status': 'error', 'message': str(e)})


@app.route('/wifi/pmkid/status', methods=['POST'])
def check_pmkid_status():
    """Check if PMKID has been captured."""
    import os

    data = request.json
    capture_file = data.get('file', '')

    if not capture_file.startswith('/tmp/intercept_pmkid_') or '..' in capture_file:
        return jsonify({'status': 'error', 'message': 'Invalid capture file path'})

    if not os.path.exists(capture_file):
        return jsonify({'pmkid_found': False, 'file_exists': False})

    file_size = os.path.getsize(capture_file)

    # Use hcxpcapngtool to check for PMKID
    pmkid_found = False
    try:
        hash_file = capture_file.replace('.pcapng', '.22000')
        result = subprocess.run(
            ['hcxpcapngtool', '-o', hash_file, capture_file],
            capture_output=True,
            text=True,
            timeout=10
        )

        # Check if hash file was created and has content
        if os.path.exists(hash_file) and os.path.getsize(hash_file) > 0:
            pmkid_found = True
    except FileNotFoundError:
        # hcxpcapngtool not installed, check file size as fallback
        pmkid_found = file_size > 1000  # Rough heuristic
    except Exception:
        pass

    return jsonify({
        'pmkid_found': pmkid_found,
        'file_exists': True,
        'file_size': file_size,
        'file': capture_file
    })


@app.route('/wifi/pmkid/stop', methods=['POST'])
def stop_pmkid():
    """Stop PMKID capture."""
    global pmkid_process

    with pmkid_lock:
        if pmkid_process:
            pmkid_process.terminate()
            try:
                pmkid_process.wait(timeout=5)
            except:
                pmkid_process.kill()
            pmkid_process = None

    return jsonify({'status': 'stopped'})


@app.route('/wifi/networks')
def get_wifi_networks():
    """Get current list of discovered networks."""
    return jsonify({
        'networks': list(wifi_networks.values()),
        'clients': list(wifi_clients.values()),
        'handshakes': wifi_handshakes,
        'monitor_interface': wifi_monitor_interface
    })


@app.route('/wifi/stream')
def stream_wifi():
    """SSE stream for WiFi events."""
    def generate():
        import json
        while True:
            try:
                msg = wifi_queue.get(timeout=1)
                yield f"data: {json.dumps(msg)}\n\n"
            except queue.Empty:
                yield f"data: {json.dumps({'type': 'keepalive'})}\n\n"

    response = Response(generate(), mimetype='text/event-stream')
    response.headers['Cache-Control'] = 'no-cache'
    response.headers['X-Accel-Buffering'] = 'no'
    response.headers['Connection'] = 'keep-alive'
    return response


# ============== BLUETOOTH RECONNAISSANCE ROUTES ==============

def get_manufacturer(mac):
    """Look up manufacturer from MAC address OUI."""
    prefix = mac[:8].upper()
    result = OUI_DATABASE.get(prefix, 'Unknown')
    return result


def classify_bt_device(name, device_class, services, manufacturer=None):
    """Classify Bluetooth device type based on available info."""
    name_lower = (name or '').lower()
    mfr_lower = (manufacturer or '').lower()

    # Audio devices - extensive patterns
    audio_patterns = [
        'airpod', 'earbud', 'headphone', 'headset', 'speaker', 'audio', 'beats', 'bose',
        'jbl', 'sony wh', 'sony wf', 'sennheiser', 'jabra', 'soundcore', 'anker', 'buds',
        'earphone', 'pod', 'soundbar', 'subwoofer', 'amp', 'dac', 'hifi', 'stereo',
        'skullcandy', 'marshall', 'b&o', 'bang', 'olufsen', 'harman', 'akg', 'shure',
        'audio-technica', 'plantronics', 'poly', 'soundlink', 'soundsport', 'quietcomfort',
        'freebuds', 'galaxy buds', 'wf-', 'wh-', 'linkbuds', 'momentum', 'px7', 'px8',
        'liberty', 'life', 'enco', 'oppo enco', 'nothing ear', 'ear (', 'studio buds',
        'powerbeats', 'solo', 'flex', 'tour', 'tune', 'reflect', 'endurance', 'soundpeats'
    ]
    if any(x in name_lower for x in audio_patterns):
        return 'audio'

    # Wearables - watches, bands, fitness
    wearable_patterns = [
        'watch', 'band', 'fitbit', 'garmin', 'mi band', 'miband', 'amazfit', 'huawei band',
        'galaxy watch', 'gear', 'versa', 'sense', 'charge', 'inspire', 'vivosmart',
        'vivoactive', 'venu', 'forerunner', 'fenix', 'instinct', 'polar', 'suunto',
        'whoop', 'oura', 'ring', 'wristband', 'fitness', 'tracker', 'activity',
        'apple watch', 'iwatch', 'samsung watch', 'ticwatch', 'fossil', 'withings'
    ]
    if any(x in name_lower for x in wearable_patterns):
        return 'wearable'

    # Phones - mobile devices
    phone_patterns = [
        'iphone', 'galaxy', 'pixel', 'phone', 'android', 'oneplus', 'huawei', 'xiaomi',
        'redmi', 'poco', 'realme', 'oppo', 'vivo', 'motorola', 'moto', 'nokia', 'lg',
        'sony xperia', 'xperia', 'asus', 'rog phone', 'zenfone', 'nothing phone',
        'samsung sm-', 'sm-g', 'sm-a', 'sm-s', 'sm-n', 'sm-f'
    ]
    if any(x in name_lower for x in phone_patterns):
        return 'phone'

    # Trackers - location devices
    tracker_patterns = [
        'airtag', 'tile', 'smarttag', 'chipolo', 'find my', 'findmy', 'locator',
        'gps', 'pet tracker', 'key finder', 'nut', 'trackr', 'pebblebee', 'cube'
    ]
    if any(x in name_lower for x in tracker_patterns):
        return 'tracker'

    # Input devices - keyboards, mice, controllers
    input_patterns = [
        'keyboard', 'mouse', 'controller', 'gamepad', 'joystick', 'remote', 'trackpad',
        'magic mouse', 'magic keyboard', 'mx master', 'mx keys', 'logitech', 'razer',
        'dualshock', 'dualsense', 'xbox', 'switch pro', 'joycon', 'joy-con', '8bitdo',
        'steelseries', 'corsair', 'hyperx'
    ]
    if any(x in name_lower for x in input_patterns):
        return 'input'

    # Media devices - TVs, streaming
    media_patterns = [
        'tv', 'roku', 'chromecast', 'firestick', 'fire tv', 'appletv', 'apple tv',
        'nvidia shield', 'android tv', 'smart tv', 'lg tv', 'samsung tv', 'sony tv',
        'tcl', 'hisense', 'vizio', 'projector', 'beam', 'soundbase'
    ]
    if any(x in name_lower for x in media_patterns):
        return 'media'

    # Computers - laptops, desktops
    computer_patterns = [
        'macbook', 'imac', 'mac mini', 'mac pro', 'thinkpad', 'latitude', 'xps',
        'pavilion', 'envy', 'spectre', 'surface', 'chromebook', 'ideapad', 'legion',
        'predator', 'rog', 'alienware', 'desktop', 'laptop', 'notebook', 'pc'
    ]
    if any(x in name_lower for x in computer_patterns):
        return 'computer'

    # Use manufacturer to infer type
    if mfr_lower in ['bose', 'jbl', 'sony', 'sennheiser', 'jabra', 'beats', 'bang & olufsen', 'audio-technica', 'plantronics', 'skullcandy', 'anker']:
        return 'audio'
    if mfr_lower in ['fitbit', 'garmin']:
        return 'wearable'
    if mfr_lower == 'tile':
        return 'tracker'
    if mfr_lower == 'logitech':
        return 'input'

    # Check device class if available
    if device_class:
        major_class = (device_class >> 8) & 0x1F
        if major_class == 1:  # Computer
            return 'computer'
        elif major_class == 2:  # Phone
            return 'phone'
        elif major_class == 4:  # Audio/Video
            return 'audio'
        elif major_class == 5:  # Peripheral
            return 'input'
        elif major_class == 6:  # Imaging
            return 'imaging'
        elif major_class == 7:  # Wearable
            return 'wearable'

    return 'other'


def detect_tracker(mac, name, manufacturer_data=None):
    """Detect if device is a known tracker (AirTag, Tile, etc)."""
    mac_prefix = mac[:5].upper()

    # AirTag detection (Apple Find My)
    if any(mac_prefix.startswith(p) for p in AIRTAG_PREFIXES):
        if manufacturer_data and b'\\x4c\\x00' in manufacturer_data:
            return {'type': 'airtag', 'name': 'Apple AirTag', 'risk': 'high'}

    # Tile detection
    if any(mac_prefix.startswith(p) for p in TILE_PREFIXES):
        return {'type': 'tile', 'name': 'Tile Tracker', 'risk': 'medium'}

    # Samsung SmartTag
    if any(mac_prefix.startswith(p) for p in SAMSUNG_TRACKER):
        return {'type': 'smarttag', 'name': 'Samsung SmartTag', 'risk': 'medium'}

    # Name-based detection
    name_lower = (name or '').lower()
    if 'airtag' in name_lower:
        return {'type': 'airtag', 'name': 'Apple AirTag', 'risk': 'high'}
    if 'tile' in name_lower:
        return {'type': 'tile', 'name': 'Tile Tracker', 'risk': 'medium'}
    if 'smarttag' in name_lower:
        return {'type': 'smarttag', 'name': 'Samsung SmartTag', 'risk': 'medium'}
    if 'chipolo' in name_lower:
        return {'type': 'chipolo', 'name': 'Chipolo Tracker', 'risk': 'medium'}

    return None


def detect_bt_interfaces():
    """Detect available Bluetooth interfaces."""
    interfaces = []
    import platform

    if platform.system() == 'Linux':
        try:
            # Use hciconfig to list interfaces
            result = subprocess.run(['hciconfig'], capture_output=True, text=True, timeout=5)
            output = result.stdout

            # Parse hciconfig output - "UP RUNNING" appears on a separate line
            import re
            # Split by interface blocks
            blocks = re.split(r'(?=^hci\d+:)', output, flags=re.MULTILINE)
            for block in blocks:
                if block.strip():
                    # Get interface name from first line
                    first_line = block.split('\n')[0]
                    match = re.match(r'(hci\d+):', first_line)
                    if match:
                        iface_name = match.group(1)
                        # Check if UP appears anywhere in the block
                        is_up = 'UP RUNNING' in block or '\tUP ' in block
                        interfaces.append({
                            'name': iface_name,
                            'type': 'hci',
                            'status': 'up' if is_up else 'down'
                        })
        except FileNotFoundError:
            pass
        except Exception as e:
            print(f"[BT] Error detecting interfaces: {e}")

    elif platform.system() == 'Darwin':  # macOS
        # macOS uses different Bluetooth stack
        interfaces.append({
            'name': 'default',
            'type': 'macos',
            'status': 'available'
        })

    return interfaces


@app.route('/bt/reload-oui', methods=['POST'])
def reload_oui_database():
    """Reload OUI database from external file."""
    global OUI_DATABASE
    new_db = load_oui_database()
    if new_db:
        OUI_DATABASE = new_db
        return jsonify({'status': 'success', 'entries': len(OUI_DATABASE)})
    return jsonify({'status': 'error', 'message': 'Could not load oui_database.json'})


@app.route('/bt/interfaces')
def get_bt_interfaces():
    """Get available Bluetooth interfaces and tools."""
    interfaces = detect_bt_interfaces()
    tools = {
        'hcitool': check_tool('hcitool'),
        'bluetoothctl': check_tool('bluetoothctl'),
        'hciconfig': check_tool('hciconfig'),
        'l2ping': check_tool('l2ping'),
        'sdptool': check_tool('sdptool')
    }
    return jsonify({
        'interfaces': interfaces,
        'tools': tools,
        'current_interface': bt_interface
    })


def parse_hcitool_output(line):
    """Parse hcitool scan output line."""
    # Format: "AA:BB:CC:DD:EE:FF    Device Name"
    parts = line.strip().split('\t')
    if len(parts) >= 2:
        mac = parts[0].strip()
        name = parts[1].strip() if len(parts) > 1 else ''
        if ':' in mac and len(mac) == 17:
            return {'mac': mac, 'name': name}
    return None


def stream_bt_scan(process, scan_mode):
    """Stream Bluetooth scan output to queue."""
    global bt_process, bt_devices
    import time

    try:
        bt_queue.put({'type': 'status', 'text': 'started'})
        start_time = time.time()
        device_found = False

        # Set up non-blocking stderr reading
        try:
            import fcntl
            fd = process.stderr.fileno()
            fl = fcntl.fcntl(fd, fcntl.F_GETFL)
            fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
        except Exception:
            pass

        if scan_mode == 'hcitool':
            # hcitool lescan output
            for line in iter(process.stdout.readline, b''):
                line = line.decode('utf-8', errors='replace').strip()
                if not line or 'LE Scan' in line:
                    continue

                # Parse BLE device
                parts = line.split()
                if len(parts) >= 1 and ':' in parts[0]:
                    mac = parts[0]
                    name = ' '.join(parts[1:]) if len(parts) > 1 else ''

                    manufacturer = get_manufacturer(mac)
                    device = {
                        'mac': mac,
                        'name': name or '[Unknown]',
                        'manufacturer': manufacturer,
                        'type': classify_bt_device(name, None, None, manufacturer),
                        'rssi': None,
                        'last_seen': time.time()
                    }

                    # Check for tracker
                    tracker = detect_tracker(mac, name)
                    if tracker:
                        device['tracker'] = tracker

                    is_new = mac not in bt_devices
                    bt_devices[mac] = device

                    queue_data = {
                        **device,
                        'type': 'device',  # Must come after **device to not be overwritten
                        'device_type': device.get('type', 'other'),
                        'action': 'new' if is_new else 'update',
                    }
                    bt_queue.put(queue_data)

        elif scan_mode == 'bluetoothctl':
            # bluetoothctl scan output - read from pty
            import os
            import select
            import time
            import re

            master_fd = getattr(process, '_master_fd', None)
            if not master_fd:
                bt_queue.put({'type': 'error', 'text': 'bluetoothctl pty not available'})
                return

            buffer = ''
            while process.poll() is None:
                # Check if data available
                readable, _, _ = select.select([master_fd], [], [], 1.0)
                if readable:
                    try:
                        data = os.read(master_fd, 4096)
                        if not data:
                            break
                        buffer += data.decode('utf-8', errors='replace')

                        # Process complete lines
                        while '\n' in buffer:
                            line, buffer = buffer.split('\n', 1)
                            line = line.strip()

                            # Remove ANSI escape codes
                            line = re.sub(r'\x1b\[[0-9;]*m', '', line)
                            line = re.sub(r'\x1b\[\?.*?[a-zA-Z]', '', line)
                            line = re.sub(r'\x1b\[K', '', line)  # Clear line escape
                            line = re.sub(r'\r', '', line)  # Remove carriage returns

                            # Debug: print what we're receiving
                            if line and 'Device' in line:
                                print(f"[BT] bluetoothctl: {line}")

                            # Parse [NEW] Device or [CHG] Device lines
                            # Format: [NEW] Device AA:BB:CC:DD:EE:FF DeviceName
                            if 'Device' in line:
                                match = re.search(r'([0-9A-Fa-f]{2}:[0-9A-Fa-f]{2}:[0-9A-Fa-f]{2}:[0-9A-Fa-f]{2}:[0-9A-Fa-f]{2}:[0-9A-Fa-f]{2})\s*(.*)', line)
                                if match:
                                    mac = match.group(1).upper()
                                    name = match.group(2).strip()

                                    manufacturer = get_manufacturer(mac)
                                    device = {
                                        'mac': mac,
                                        'name': name or '[Unknown]',
                                        'manufacturer': manufacturer,
                                        'type': classify_bt_device(name, None, None, manufacturer),
                                        'rssi': None,
                                        'last_seen': time.time()
                                    }

                                    tracker = detect_tracker(mac, name)
                                    if tracker:
                                        device['tracker'] = tracker

                                    is_new = mac not in bt_devices
                                    bt_devices[mac] = device

                                    queue_data = {
                                        **device,
                                        'type': 'device',  # Must come after **device to not be overwritten
                                        'device_type': device.get('type', 'other'),
                                        'action': 'new' if is_new else 'update',
                                    }
                                    print(f"[BT] Queuing device: {mac} - {name}")
                                    bt_queue.put(queue_data)
                    except OSError:
                        break

            # Close master_fd
            try:
                os.close(master_fd)
            except:
                pass

    except Exception as e:
        bt_queue.put({'type': 'error', 'text': str(e)})
    finally:
        # Capture any remaining stderr
        try:
            remaining_stderr = process.stderr.read()
            if remaining_stderr:
                stderr_text = remaining_stderr.decode('utf-8', errors='replace').strip()
                if stderr_text:
                    bt_queue.put({'type': 'error', 'text': f'Bluetooth scan: {stderr_text}'})
        except Exception:
            pass

        # Check exit code
        process.wait()
        exit_code = process.returncode
        if exit_code != 0 and exit_code is not None:
            bt_queue.put({'type': 'error', 'text': f'Bluetooth scan exited with code {exit_code}'})

        bt_queue.put({'type': 'status', 'text': 'stopped'})
        with bt_lock:
            bt_process = None


@app.route('/bt/scan/start', methods=['POST'])
def start_bt_scan():
    """Start Bluetooth scanning."""
    global bt_process, bt_devices, bt_interface

    with bt_lock:
        # Check if process is actually still running (not just set)
        if bt_process:
            if bt_process.poll() is None:
                # Process is actually running
                return jsonify({'status': 'error', 'message': 'Scan already running'})
            else:
                # Process died, clear the state
                bt_process = None

        data = request.json
        scan_mode = data.get('mode', 'hcitool')
        interface = data.get('interface', 'hci0')
        duration = data.get('duration', 30)
        scan_ble = data.get('scan_ble', True)
        scan_classic = data.get('scan_classic', True)

        bt_interface = interface
        bt_devices = {}

        # Clear queue
        while not bt_queue.empty():
            try:
                bt_queue.get_nowait()
            except:
                break

        try:
            if scan_mode == 'hcitool':
                if scan_ble:
                    cmd = ['hcitool', '-i', interface, 'lescan', '--duplicates']
                else:
                    cmd = ['hcitool', '-i', interface, 'scan']

                bt_process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )

            elif scan_mode == 'bluetoothctl':
                # Use bluetoothctl for BLE scanning with pty for proper output
                import pty
                import os
                import time

                master_fd, slave_fd = pty.openpty()
                bt_process = subprocess.Popen(
                    ['bluetoothctl'],
                    stdin=slave_fd,
                    stdout=slave_fd,
                    stderr=slave_fd,
                    close_fds=True
                )
                os.close(slave_fd)

                # Store master_fd for reading
                bt_process._master_fd = master_fd

                # Wait for bluetoothctl to initialize
                time.sleep(0.5)

                # Power on and start LE scan (compatible with Bluetooth 5.x)
                os.write(master_fd, b'power on\n')
                time.sleep(0.3)
                os.write(master_fd, b'scan on\n')

            else:
                return jsonify({'status': 'error', 'message': f'Unknown scan mode: {scan_mode}'})

            # Wait briefly to check if process fails immediately
            import time
            time.sleep(0.5)

            if bt_process.poll() is not None:
                # Process already exited - capture error
                stderr_output = bt_process.stderr.read().decode('utf-8', errors='replace').strip()
                stdout_output = bt_process.stdout.read().decode('utf-8', errors='replace').strip()
                exit_code = bt_process.returncode
                bt_process = None

                error_msg = stderr_output or stdout_output or f'Process exited with code {exit_code}'

                # Common error explanations and auto-recovery
                if 'No such device' in error_msg or 'hci0' in error_msg.lower():
                    error_msg = f'Bluetooth interface "{interface}" not found or not available.'
                elif 'Operation not permitted' in error_msg or 'Permission denied' in error_msg:
                    error_msg = 'Permission denied. Try running with sudo or add user to bluetooth group.'
                elif 'busy' in error_msg.lower():
                    error_msg = f'Bluetooth interface "{interface}" is busy. Stop other Bluetooth operations first.'
                elif 'set scan parameters failed' in error_msg.lower() or 'input/output error' in error_msg.lower():
                    # Try to auto-reset the adapter
                    try:
                        subprocess.run(['hciconfig', interface, 'down'], capture_output=True, timeout=5)
                        subprocess.run(['hciconfig', interface, 'up'], capture_output=True, timeout=5)
                        error_msg = f'Adapter error - attempted auto-reset. Click "Reset Adapter" and try again.'
                    except:
                        error_msg = 'Bluetooth adapter I/O error. Click "Reset Adapter" to reset the adapter and try again.'

                return jsonify({'status': 'error', 'message': error_msg})

            # Start streaming thread
            thread = threading.Thread(target=stream_bt_scan, args=(bt_process, scan_mode))
            thread.daemon = True
            thread.start()

            bt_queue.put({'type': 'info', 'text': f'Started {scan_mode} scan on {interface}'})
            return jsonify({'status': 'started', 'mode': scan_mode, 'interface': interface})

        except FileNotFoundError as e:
            tool_name = e.filename or scan_mode
            return jsonify({'status': 'error', 'message': f'Tool "{tool_name}" not found. Install required Bluetooth tools.'})
        except Exception as e:
            return jsonify({'status': 'error', 'message': str(e)})


@app.route('/bt/scan/stop', methods=['POST'])
def stop_bt_scan():
    """Stop Bluetooth scanning."""
    global bt_process

    with bt_lock:
        if bt_process:
            bt_process.terminate()
            try:
                bt_process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                bt_process.kill()
            bt_process = None
            return jsonify({'status': 'stopped'})
        return jsonify({'status': 'not_running'})


@app.route('/bt/reset', methods=['POST'])
def reset_bt_adapter():
    """Reset Bluetooth adapter and clear scan state."""
    global bt_process

    data = request.json
    interface = data.get('interface', 'hci0')

    with bt_lock:
        # Force clear the process state
        if bt_process:
            try:
                bt_process.terminate()
                bt_process.wait(timeout=2)
            except:
                try:
                    bt_process.kill()
                except:
                    pass
            bt_process = None

    # Reset the adapter
    try:
        import time
        import os

        # Kill any processes that might be using the adapter
        subprocess.run(['pkill', '-f', 'hcitool'], capture_output=True, timeout=2)
        subprocess.run(['pkill', '-f', 'bluetoothctl'], capture_output=True, timeout=2)
        time.sleep(0.5)

        # Check if running as root
        is_root = os.geteuid() == 0

        # Try rfkill unblock first
        subprocess.run(['rfkill', 'unblock', 'bluetooth'], capture_output=True, timeout=5)

        # Reset the adapter with a delay between down and up
        if is_root:
            down_result = subprocess.run(['hciconfig', interface, 'down'], capture_output=True, text=True, timeout=5)
            time.sleep(1)
            up_result = subprocess.run(['hciconfig', interface, 'up'], capture_output=True, text=True, timeout=5)
        else:
            # Try with sudo
            down_result = subprocess.run(['sudo', '-n', 'hciconfig', interface, 'down'], capture_output=True, text=True, timeout=5)
            time.sleep(1)
            up_result = subprocess.run(['sudo', '-n', 'hciconfig', interface, 'up'], capture_output=True, text=True, timeout=5)

        time.sleep(0.5)

        # Check if adapter is up
        result = subprocess.run(['hciconfig', interface], capture_output=True, text=True, timeout=5)
        is_up = 'UP RUNNING' in result.stdout

        # If still not up, try bluetoothctl
        if not is_up:
            subprocess.run(['bluetoothctl', 'power', 'off'], capture_output=True, timeout=5)
            time.sleep(1)
            subprocess.run(['bluetoothctl', 'power', 'on'], capture_output=True, timeout=5)
            time.sleep(0.5)
            result = subprocess.run(['hciconfig', interface], capture_output=True, text=True, timeout=5)
            is_up = 'UP RUNNING' in result.stdout

        if is_up:
            bt_queue.put({'type': 'info', 'text': f'Bluetooth adapter {interface} reset successfully'})
        else:
            bt_queue.put({'type': 'error', 'text': f'Adapter {interface} may need manual reset. Try: sudo hciconfig {interface} up'})

        return jsonify({
            'status': 'success' if is_up else 'warning',
            'message': f'Adapter {interface} reset' if is_up else f'Reset attempted but adapter still down. Run: sudo hciconfig {interface} up',
            'is_up': is_up
        })

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})


@app.route('/bt/enum', methods=['POST'])
def enum_bt_services():
    """Enumerate services on a Bluetooth device."""
    data = request.json
    target_mac = data.get('mac')

    if not target_mac:
        return jsonify({'status': 'error', 'message': 'Target MAC required'})

    try:
        # Try sdptool for classic BT
        result = subprocess.run(
            ['sdptool', 'browse', target_mac],
            capture_output=True, text=True, timeout=30
        )

        services = []
        current_service = {}

        for line in result.stdout.split('\n'):
            line = line.strip()
            if line.startswith('Service Name:'):
                if current_service:
                    services.append(current_service)
                current_service = {'name': line.split(':', 1)[1].strip()}
            elif line.startswith('Service Description:'):
                current_service['description'] = line.split(':', 1)[1].strip()
            elif line.startswith('Service Provider:'):
                current_service['provider'] = line.split(':', 1)[1].strip()
            elif 'Protocol Descriptor' in line:
                current_service['protocol'] = line

        if current_service:
            services.append(current_service)

        bt_services[target_mac] = services

        return jsonify({
            'status': 'success',
            'mac': target_mac,
            'services': services
        })

    except subprocess.TimeoutExpired:
        return jsonify({'status': 'error', 'message': 'Connection timed out'})
    except FileNotFoundError:
        return jsonify({'status': 'error', 'message': 'sdptool not found'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})


@app.route('/bt/devices')
def get_bt_devices():
    """Get current list of discovered Bluetooth devices."""
    return jsonify({
        'devices': list(bt_devices.values()),
        'beacons': list(bt_beacons.values()),
        'interface': bt_interface
    })


@app.route('/bt/stream')
def stream_bt():
    """SSE stream for Bluetooth events."""
    print("[BT Stream] Client connected")
    def generate():
        import json
        print("[BT Stream] Generator started, waiting for queue...")
        while True:
            try:
                msg = bt_queue.get(timeout=1)
                print(f"[BT Stream] Got from queue: {msg.get('type')}")
                if msg.get('type') == 'device':
                    print(f"[BT Stream] Sending device: {msg.get('mac')}")
                yield f"data: {json.dumps(msg)}\n\n"
            except queue.Empty:
                yield f"data: {json.dumps({'type': 'keepalive'})}\n\n"

    response = Response(generate(), mimetype='text/event-stream')
    response.headers['Cache-Control'] = 'no-cache'
    response.headers['X-Accel-Buffering'] = 'no'
    response.headers['Connection'] = 'keep-alive'
    return response


# ============================================
# AIRCRAFT (ADS-B) ROUTES
# ============================================

@app.route('/adsb/tools')
def check_adsb_tools():
    """Check for ADS-B decoding tools."""
    return jsonify({
        'dump1090': shutil.which('dump1090') is not None or shutil.which('dump1090-mutability') is not None,
        'rtl_adsb': shutil.which('rtl_adsb') is not None
    })


adsb_using_service = False  # Track if we're using an existing service


def check_dump1090_service():
    """Check if dump1090 SBS port (30003) is available."""
    import socket
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex(('localhost', 30003))
        sock.close()
        print(f"[ADS-B] Service check: port 30003 connect result = {result}")
        if result == 0:
            return 'localhost:30003'
    except Exception as e:
        print(f"[ADS-B] Service check error: {e}")
    return None


@app.route('/adsb/start', methods=['POST'])
def start_adsb():
    """Start ADS-B tracking."""
    global adsb_process, adsb_using_service

    with adsb_lock:
        if adsb_process and adsb_process.poll() is None:
            return jsonify({'status': 'error', 'message': 'ADS-B already running'})
        if adsb_using_service:
            return jsonify({'status': 'error', 'message': 'ADS-B already running (using service)'})

    data = request.json or {}
    gain = data.get('gain', '40')
    device = data.get('device', '0')

    # Find dump1090
    dump1090_path = shutil.which('dump1090') or shutil.which('dump1090-mutability')

    if not dump1090_path:
        return jsonify({'status': 'error', 'message': 'dump1090 not found. Install dump1090-mutability.'})

    # Start dump1090 with --net to enable SBS output on port 30003
    cmd = [dump1090_path, '--net', '--gain', gain, '--device-index', str(device), '--quiet']
    print(f"[ADS-B] Starting dump1090 with network mode: {' '.join(cmd)}")

    try:
        adsb_process = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

        # Wait for dump1090 to start and open ports
        print("[ADS-B] Waiting for dump1090 to initialize...")
        time.sleep(3)

        # Check if process is still running
        if adsb_process.poll() is not None:
            return jsonify({'status': 'error', 'message': 'dump1090 failed to start. Check if RTL-SDR is connected.'})

        # Connect to SBS port for data
        print("[ADS-B] Connecting to SBS stream on localhost:30003...")
        adsb_using_service = True
        thread = threading.Thread(target=parse_sbs_stream, args=('localhost:30003',), daemon=True)
        thread.start()

        return jsonify({'status': 'started', 'mode': 'standalone'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})


def parse_sbs_stream(service_addr):
    """Parse SBS format data from dump1090 port 30003."""
    global adsb_aircraft, adsb_using_service
    import socket

    host, port = service_addr.split(':')
    port = int(port)

    print(f"[ADS-B] SBS stream parser started, connecting to {host}:{port}")

    while adsb_using_service:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((host, port))
            print(f"[ADS-B] Connected to SBS stream")

            buffer = ""
            msg_count = 0
            last_update = time.time()
            pending_updates = set()  # ICAOs with pending updates
            while adsb_using_service:
                try:
                    data = sock.recv(4096).decode('utf-8', errors='ignore')
                    if not data:
                        break
                    buffer += data

                    while '\n' in buffer:
                        line, buffer = buffer.split('\n', 1)
                        line = line.strip()
                        if not line:
                            continue

                        # Parse SBS format: MSG,type,sess,aircraft,hex,flight,dateG,timeG,dateL,timeL,callsign,alt,speed,heading,lat,lon,...
                        parts = line.split(',')
                        if len(parts) < 11 or parts[0] != 'MSG':
                            continue

                        msg_type = parts[1]
                        icao = parts[4].upper()
                        if not icao:
                            continue

                        aircraft = adsb_aircraft.get(icao, {'icao': icao})

                        # MSG,1: callsign
                        if msg_type == '1' and len(parts) > 10:
                            callsign = parts[10].strip()
                            if callsign:
                                aircraft['callsign'] = callsign

                        # MSG,3: position (alt, lat, lon)
                        elif msg_type == '3':
                            if len(parts) > 15:
                                if parts[11]:
                                    try:
                                        aircraft['altitude'] = int(float(parts[11]))
                                    except:
                                        pass
                                if parts[14] and parts[15]:
                                    try:
                                        lat = float(parts[14])
                                        lon = float(parts[15])
                                        aircraft['lat'] = lat
                                        aircraft['lon'] = lon
                                        print(f"[ADS-B] Position SET: {icao} at {lat:.4f}, {lon:.4f}")
                                    except Exception as e:
                                        print(f"[ADS-B] Position parse error: {e}")

                        # MSG,4: velocity (speed, heading)
                        elif msg_type == '4' and len(parts) > 13:
                            if parts[12]:
                                try:
                                    aircraft['speed'] = int(float(parts[12]))
                                except:
                                    pass
                            if parts[13]:
                                try:
                                    aircraft['heading'] = int(float(parts[13]))
                                except:
                                    pass

                        # MSG,5: callsign and altitude
                        elif msg_type == '5' and len(parts) > 11:
                            if parts[10]:
                                callsign = parts[10].strip()
                                if callsign:
                                    aircraft['callsign'] = callsign
                            if parts[11]:
                                try:
                                    aircraft['altitude'] = int(float(parts[11]))
                                except:
                                    pass

                        # MSG,6: squawk
                        elif msg_type == '6' and len(parts) > 17:
                            if parts[17]:
                                aircraft['squawk'] = parts[17]

                        adsb_aircraft[icao] = aircraft
                        pending_updates.add(icao)
                        msg_count += 1

                        # Throttle updates to frontend - send every 1 second
                        now = time.time()
                        if now - last_update >= 1.0:
                            for update_icao in pending_updates:
                                if update_icao in adsb_aircraft:
                                    adsb_queue.put({
                                        'type': 'aircraft',
                                        **adsb_aircraft[update_icao]
                                    })
                            pending_updates.clear()
                            last_update = now
                            if msg_count % 500 == 0:
                                print(f"[ADS-B] SBS: Processed {msg_count} messages, tracking {len(adsb_aircraft)} aircraft")

                except socket.timeout:
                    continue

            sock.close()
        except Exception as e:
            print(f"[ADS-B] SBS connection error: {e}, reconnecting...")
            time.sleep(2)

    print("[ADS-B] SBS stream parser stopped")


@app.route('/adsb/stop', methods=['POST'])
def stop_adsb():
    """Stop ADS-B tracking."""
    global adsb_process, adsb_aircraft, adsb_using_service

    with adsb_lock:
        if adsb_process:
            adsb_process.terminate()
            try:
                adsb_process.wait(timeout=5)
            except:
                adsb_process.kill()
            adsb_process = None
        adsb_using_service = False

    adsb_aircraft = {}
    return jsonify({'status': 'stopped'})


@app.route('/adsb/stream')
def stream_adsb():
    """SSE stream for ADS-B aircraft."""
    def generate():
        while True:
            try:
                msg = adsb_queue.get(timeout=1)
                yield f"data: {json.dumps(msg)}\n\n"
            except queue.Empty:
                yield f"data: {json.dumps({'type': 'keepalive'})}\n\n"

    response = Response(generate(), mimetype='text/event-stream')
    response.headers['Cache-Control'] = 'no-cache'
    response.headers['X-Accel-Buffering'] = 'no'
    return response


def parse_adsb_output(process):
    """Parse ADS-B output and poll dump1090 JSON for decoded data."""
    global adsb_aircraft
    import re
    import urllib.request
    import json as json_lib

    icao_pattern = re.compile(r'\*([0-9A-Fa-f]{6,14});')

    # Start a thread to poll dump1090's JSON endpoint for decoded positions
    def poll_dump1090_json():
        """Poll dump1090's aircraft.json for decoded lat/lon data."""
        json_urls = [
            'http://localhost:30005/data/aircraft.json',
            'http://localhost:8080/data/aircraft.json',
            'http://localhost:30003/data/aircraft.json',
            'http://localhost:16601/data/aircraft.json',
            'http://localhost:8080/dump1090/data/aircraft.json'
        ]
        working_url = None
        print("[ADS-B] JSON polling thread started")

        while process and process.poll() is None:
            try:
                # Find working URL on first success
                urls_to_try = [working_url] if working_url else json_urls
                for url in urls_to_try:
                    try:
                        with urllib.request.urlopen(url, timeout=2) as response:
                            data = json_lib.loads(response.read().decode())
                            if not working_url:
                                print(f"[ADS-B] JSON: Connected to {url}")
                            working_url = url

                            aircraft_list = data.get('aircraft', [])
                            if aircraft_list:
                                print(f"[ADS-B] JSON: Found {len(aircraft_list)} aircraft")
                            for ac in aircraft_list:
                                icao = ac.get('hex', '').upper()
                                if not icao:
                                    continue

                                # Update aircraft with decoded position data
                                aircraft = adsb_aircraft.get(icao, {'icao': icao})
                                aircraft.update({
                                    'icao': icao,
                                    'callsign': ac.get('flight', '').strip() or aircraft.get('callsign'),
                                    'altitude': ac.get('altitude') or ac.get('alt_baro') or aircraft.get('altitude'),
                                    'speed': ac.get('speed') or ac.get('gs') or aircraft.get('speed'),
                                    'heading': ac.get('track') or aircraft.get('heading'),
                                    'lat': ac.get('lat') or aircraft.get('lat'),
                                    'lon': ac.get('lon') or aircraft.get('lon'),
                                    'squawk': ac.get('squawk') or aircraft.get('squawk'),
                                    'rssi': ac.get('rssi') or aircraft.get('rssi')
                                })

                                adsb_aircraft[icao] = aircraft
                                adsb_queue.put({
                                    'type': 'aircraft',
                                    **aircraft
                                })
                            break
                    except Exception as url_err:
                        continue  # Try next URL
            except Exception as e:
                print(f"[ADS-B] JSON error: {e}")
            time.sleep(1)

    # Start JSON polling thread
    json_thread = threading.Thread(target=poll_dump1090_json, daemon=True)
    json_thread.start()

    # Also parse raw output for immediate ICAO detection
    try:
        for line in process.stdout:
            line = line.strip()
            if not line:
                continue

            # Parse raw Mode S messages for quick ICAO detection
            match = icao_pattern.search(line)
            if match:
                raw = match.group(1)
                if len(raw) >= 6:
                    icao = raw[:6].upper()

                    # Create placeholder if not seen via JSON yet
                    if icao not in adsb_aircraft:
                        print(f"[ADS-B] Raw: New aircraft {icao}")
                        aircraft = {
                            'icao': icao,
                            'callsign': None,
                            'altitude': None,
                            'speed': None,
                            'heading': None,
                            'lat': None,
                            'lon': None
                        }
                        adsb_aircraft[icao] = aircraft
                        adsb_queue.put({
                            'type': 'aircraft',
                            **aircraft
                        })
    except Exception as e:
        print(f"[ADS-B] Parse error: {e}")


# ============================================
# SATELLITE ROUTES
# ============================================

@app.route('/satellite/dashboard')
def satellite_dashboard():
    """Popout hi-tech satellite tracking dashboard."""
    return render_template_string('''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SATELLITE COMMAND // INTERCEPT</title>
    <link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@400;500;700;900&family=Rajdhani:wght@300;400;500;600;700&family=JetBrains+Mono:wght@300;400;500&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        :root {
            --bg-dark: #0a0a0f;
            --bg-panel: #0d1117;
            --bg-card: #161b22;
            --border-glow: #00d4ff;
            --text-primary: #e6edf3;
            --text-secondary: #8b949e;
            --accent-cyan: #00d4ff;
            --accent-green: #00ff88;
            --accent-orange: #ff9500;
            --accent-red: #ff4444;
            --accent-purple: #a855f7;
            --grid-line: rgba(0, 212, 255, 0.1);
        }

        body {
            font-family: 'Rajdhani', sans-serif;
            background: var(--bg-dark);
            color: var(--text-primary);
            min-height: 100vh;
            overflow-x: hidden;
        }

        /* Animated grid background */
        .grid-bg {
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background-image:
                linear-gradient(var(--grid-line) 1px, transparent 1px),
                linear-gradient(90deg, var(--grid-line) 1px, transparent 1px);
            background-size: 50px 50px;
            animation: gridMove 20s linear infinite;
            pointer-events: none;
            z-index: 0;
        }

        @keyframes gridMove {
            0% { transform: translate(0, 0); }
            100% { transform: translate(50px, 50px); }
        }

        /* Scan line effect */
        .scanline {
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            height: 4px;
            background: linear-gradient(90deg, transparent, var(--accent-cyan), transparent);
            animation: scan 3s linear infinite;
            pointer-events: none;
            z-index: 1000;
            opacity: 0.5;
        }

        @keyframes scan {
            0% { top: -4px; }
            100% { top: 100vh; }
        }

        /* Header */
        .header {
            position: relative;
            z-index: 10;
            padding: 15px 30px;
            background: linear-gradient(180deg, rgba(0,212,255,0.1) 0%, transparent 100%);
            border-bottom: 1px solid rgba(0,212,255,0.3);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .logo {
            font-family: 'Orbitron', monospace;
            font-size: 28px;
            font-weight: 900;
            letter-spacing: 4px;
            color: var(--accent-cyan);
            text-shadow: 0 0 20px var(--accent-cyan), 0 0 40px var(--accent-cyan);
        }

        .logo span {
            color: var(--text-secondary);
            font-weight: 400;
            font-size: 16px;
            margin-left: 15px;
            letter-spacing: 2px;
        }

        .status-bar {
            display: flex;
            gap: 30px;
            font-family: 'JetBrains Mono', monospace;
            font-size: 12px;
        }

        .status-item {
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .status-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: var(--accent-green);
            box-shadow: 0 0 10px var(--accent-green);
            animation: pulse 2s ease-in-out infinite;
        }

        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }

        .datetime {
            font-family: 'Orbitron', monospace;
            font-size: 14px;
            color: var(--accent-cyan);
        }

        /* Satellite selector */
        .satellite-selector {
            display: flex;
            align-items: center;
            gap: 12px;
            background: rgba(0,0,0,0.3);
            padding: 8px 16px;
            border-radius: 6px;
            border: 1px solid rgba(0,212,255,0.3);
        }

        .satellite-selector label {
            font-family: 'Orbitron', monospace;
            font-size: 11px;
            font-weight: 500;
            letter-spacing: 2px;
            color: var(--text-secondary);
        }

        .satellite-selector select {
            background: rgba(0,212,255,0.1);
            border: 1px solid var(--accent-cyan);
            border-radius: 4px;
            padding: 8px 30px 8px 12px;
            color: var(--accent-cyan);
            font-family: 'Orbitron', monospace;
            font-size: 13px;
            font-weight: 600;
            letter-spacing: 1px;
            cursor: pointer;
            appearance: none;
            background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 12 12'%3E%3Cpath fill='%2300d4ff' d='M6 8L2 4h8z'/%3E%3C/svg%3E");
            background-repeat: no-repeat;
            background-position: right 10px center;
            min-width: 180px;
        }

        .satellite-selector select:hover {
            background-color: rgba(0,212,255,0.2);
            box-shadow: 0 0 15px rgba(0,212,255,0.3);
        }

        .satellite-selector select:focus {
            outline: none;
            box-shadow: 0 0 20px rgba(0,212,255,0.4);
        }

        .satellite-selector select option {
            background: var(--bg-panel);
            color: var(--text-primary);
            padding: 10px;
        }

        /* Main dashboard grid */
        .dashboard {
            position: relative;
            z-index: 10;
            display: grid;
            grid-template-columns: 1fr 1fr 380px;
            grid-template-rows: 1fr;
            gap: 20px;
            padding: 20px;
            height: calc(100vh - 80px);
            min-height: 600px;
        }

        /* Panels */
        .panel {
            background: var(--bg-panel);
            border: 1px solid rgba(0,212,255,0.2);
            border-radius: 8px;
            overflow: hidden;
            position: relative;
        }

        .panel::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 2px;
            background: linear-gradient(90deg, transparent, var(--accent-cyan), transparent);
        }

        .panel-header {
            padding: 12px 20px;
            background: rgba(0,212,255,0.05);
            border-bottom: 1px solid rgba(0,212,255,0.1);
            font-family: 'Orbitron', monospace;
            font-size: 12px;
            font-weight: 500;
            letter-spacing: 2px;
            text-transform: uppercase;
            color: var(--accent-cyan);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .panel-indicator {
            width: 6px;
            height: 6px;
            background: var(--accent-green);
            border-radius: 50%;
            animation: blink 1s ease-in-out infinite;
        }

        @keyframes blink {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.3; }
        }

        .panel-content {
            padding: 15px;
            height: calc(100% - 45px);
            overflow-y: auto;
        }

        /* Polar plot */
        .polar-container {
            grid-column: 1;
            grid-row: 1;
        }

        #polarPlot {
            width: 100%;
            height: 100%;
            min-height: 400px;
        }

        /* Ground track map */
        .map-container {
            grid-column: 2;
            grid-row: 1;
        }

        #groundMap {
            width: 100%;
            height: 100%;
            min-height: 400px;
            border-radius: 4px;
        }

        /* Right sidebar */
        .sidebar {
            grid-column: 3;
            grid-row: 1;
            display: flex;
            flex-direction: column;
            gap: 12px;
            overflow-y: auto;
            padding-right: 5px;
        }

        .sidebar .panel {
            flex-shrink: 0;
        }

        /* Countdown panel */
        .countdown-panel {
            background: linear-gradient(135deg, rgba(0,212,255,0.1) 0%, rgba(0,255,136,0.05) 100%);
        }

        .countdown-display {
            text-align: center;
            padding: 20px 10px;
        }

        .next-pass-label {
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 2px;
            color: var(--text-secondary);
            margin-bottom: 5px;
        }

        .satellite-name {
            font-family: 'Orbitron', monospace;
            font-size: 20px;
            font-weight: 700;
            color: var(--accent-cyan);
            text-shadow: 0 0 15px var(--accent-cyan);
            margin-bottom: 15px;
        }

        .countdown-grid {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 8px;
        }

        .countdown-block {
            background: rgba(0,0,0,0.3);
            border: 1px solid rgba(0,212,255,0.2);
            border-radius: 6px;
            padding: 10px 5px;
            text-align: center;
        }

        .countdown-value {
            font-family: 'Orbitron', monospace;
            font-size: 28px;
            font-weight: 700;
            color: var(--accent-cyan);
            text-shadow: 0 0 10px var(--accent-cyan);
            line-height: 1;
        }

        .countdown-value.active {
            color: var(--accent-green);
            text-shadow: 0 0 15px var(--accent-green);
            animation: countPulse 1s ease-in-out infinite;
        }

        @keyframes countPulse {
            0%, 100% { opacity: 1; transform: scale(1); }
            50% { opacity: 0.8; transform: scale(1.05); }
        }

        .countdown-label {
            font-size: 10px;
            text-transform: uppercase;
            letter-spacing: 1px;
            color: var(--text-secondary);
            margin-top: 5px;
        }

        /* Stats panel */
        .stats-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 10px;
        }

        .stat-box {
            background: rgba(0,0,0,0.3);
            border: 1px solid rgba(0,212,255,0.15);
            border-radius: 6px;
            padding: 12px;
            text-align: center;
        }

        .stat-value {
            font-family: 'Orbitron', monospace;
            font-size: 22px;
            font-weight: 600;
            color: var(--accent-cyan);
        }

        .stat-value.highlight {
            color: var(--accent-green);
        }

        .stat-label {
            font-size: 10px;
            text-transform: uppercase;
            letter-spacing: 1px;
            color: var(--text-secondary);
            margin-top: 4px;
        }

        /* Pass list */
        .pass-list {
            flex: 1;
            overflow-y: auto;
        }

        .pass-list-content {
            display: flex;
            flex-direction: column;
            gap: 8px;
            max-height: 250px;
            overflow-y: auto;
        }

        .pass-item {
            background: rgba(0,0,0,0.3);
            border: 1px solid rgba(0,212,255,0.15);
            border-radius: 6px;
            padding: 12px;
            cursor: pointer;
            transition: all 0.2s ease;
        }

        .pass-item:hover {
            border-color: var(--accent-cyan);
            background: rgba(0,212,255,0.05);
        }

        .pass-item.active {
            border-color: var(--accent-cyan);
            box-shadow: 0 0 15px rgba(0,212,255,0.2);
            background: rgba(0,212,255,0.1);
        }

        .pass-item-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 8px;
        }

        .pass-sat-name {
            font-family: 'Orbitron', monospace;
            font-size: 13px;
            font-weight: 600;
            color: var(--accent-cyan);
        }

        .pass-quality {
            font-size: 9px;
            padding: 2px 6px;
            border-radius: 3px;
            text-transform: uppercase;
            letter-spacing: 1px;
            font-weight: 600;
        }

        .pass-quality.excellent {
            background: rgba(0,255,136,0.2);
            color: var(--accent-green);
        }

        .pass-quality.good {
            background: rgba(0,212,255,0.2);
            color: var(--accent-cyan);
        }

        .pass-quality.fair {
            background: rgba(255,149,0,0.2);
            color: var(--accent-orange);
        }

        .pass-item-details {
            display: flex;
            justify-content: space-between;
            font-size: 11px;
            color: var(--text-secondary);
        }

        .pass-time {
            font-family: 'JetBrains Mono', monospace;
        }

        /* Telemetry panel */
        .telemetry-panel {
            background: linear-gradient(135deg, rgba(168,85,247,0.1) 0%, rgba(0,212,255,0.05) 100%);
        }

        .telemetry-rows {
            display: flex;
            flex-direction: column;
            gap: 8px;
        }

        .telemetry-row {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 8px 12px;
            background: rgba(0,0,0,0.2);
            border-radius: 4px;
            border-left: 2px solid var(--accent-cyan);
        }

        .telemetry-label {
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 1px;
            color: var(--text-secondary);
        }

        .telemetry-value {
            font-family: 'JetBrains Mono', monospace;
            font-size: 14px;
            color: var(--accent-cyan);
            white-space: nowrap;
        }

        /* Observer location */
        .observer-panel .panel-content {
            padding: 10px 15px;
        }

        .observer-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 10px;
        }

        .observer-input {
            display: flex;
            flex-direction: column;
            gap: 4px;
        }

        .observer-input label {
            font-size: 10px;
            text-transform: uppercase;
            letter-spacing: 1px;
            color: var(--text-secondary);
        }

        .observer-input input {
            background: rgba(0,0,0,0.3);
            border: 1px solid rgba(0,212,255,0.2);
            border-radius: 4px;
            padding: 8px 10px;
            color: var(--text-primary);
            font-family: 'JetBrains Mono', monospace;
            font-size: 12px;
        }

        .observer-input input:focus {
            outline: none;
            border-color: var(--accent-cyan);
            box-shadow: 0 0 10px rgba(0,212,255,0.2);
        }

        .observer-actions {
            display: flex;
            gap: 8px;
            margin-top: 10px;
        }

        .btn {
            flex: 1;
            padding: 10px;
            border: 1px solid var(--accent-cyan);
            background: rgba(0,212,255,0.1);
            color: var(--accent-cyan);
            font-family: 'Rajdhani', sans-serif;
            font-size: 12px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 1px;
            border-radius: 4px;
            cursor: pointer;
            transition: all 0.2s ease;
        }

        .btn:hover {
            background: var(--accent-cyan);
            color: var(--bg-dark);
            box-shadow: 0 0 20px rgba(0,212,255,0.3);
        }

        .btn.primary {
            background: var(--accent-cyan);
            color: var(--bg-dark);
        }

        .btn.primary:hover {
            box-shadow: 0 0 25px rgba(0,212,255,0.5);
        }

        /* Leaflet dark theme overrides */
        .leaflet-container {
            background: var(--bg-dark) !important;
        }

        .leaflet-control-zoom a {
            background: var(--bg-panel) !important;
            color: var(--accent-cyan) !important;
            border-color: rgba(0,212,255,0.3) !important;
        }

        .leaflet-control-attribution {
            background: rgba(0,0,0,0.7) !important;
            color: var(--text-secondary) !important;
            font-size: 9px !important;
        }

        /* Custom scrollbar */
        ::-webkit-scrollbar {
            width: 6px;
        }

        ::-webkit-scrollbar-track {
            background: var(--bg-dark);
        }

        ::-webkit-scrollbar-thumb {
            background: var(--accent-cyan);
            border-radius: 3px;
        }

        /* Responsive - only kick in on smaller screens */
        @media (max-width: 1200px) {
            .dashboard {
                grid-template-columns: 1fr 1fr;
                grid-template-rows: 1fr auto;
                height: auto;
                min-height: 100vh;
            }
            .polar-container {
                grid-column: 1;
                grid-row: 1;
            }
            .map-container {
                grid-column: 2;
                grid-row: 1;
            }
            .sidebar {
                grid-column: 1 / 3;
                grid-row: 2;
                flex-direction: row;
                flex-wrap: wrap;
                overflow-y: visible;
                max-height: none;
            }
            .sidebar .panel {
                flex: 1 1 280px;
                max-height: 280px;
            }
        }

        @media (max-width: 800px) {
            .dashboard {
                grid-template-columns: 1fr;
                grid-template-rows: auto;
            }
            .polar-container, .map-container {
                grid-column: 1;
                min-height: 300px;
            }
            .sidebar {
                grid-column: 1;
                flex-direction: column;
            }
            .sidebar .panel {
                flex: none;
                max-height: none;
                min-width: 100%;
            }
        }
    </style>
</head>
<body>
    <div class="grid-bg"></div>
    <div class="scanline"></div>

    <header class="header">
        <div class="logo">
            SATELLITE COMMAND
            <span>// INTERCEPT</span>
        </div>
        <div class="satellite-selector">
            <label for="satSelect">TARGET:</label>
            <select id="satSelect" onchange="onSatelliteChange()">
                <option value="25544">ISS (ZARYA)</option>
                <option value="25338">NOAA 15</option>
                <option value="28654">NOAA 18</option>
                <option value="33591">NOAA 19</option>
                <option value="40069">METEOR-M2</option>
                <option value="43013">NOAA 20</option>
                <option value="54234">METEOR-M2-3</option>
            </select>
        </div>
        <div class="status-bar">
            <div class="status-item">
                <div class="status-dot" id="trackingDot"></div>
                <span id="trackingStatus">TRACKING ACTIVE</span>
            </div>
            <div class="status-item datetime" id="utcTime">--:--:-- UTC</div>
        </div>
    </header>

    <main class="dashboard">
        <!-- Polar Plot -->
        <div class="panel polar-container">
            <div class="panel-header">
                <span>SKY VIEW // POLAR PLOT</span>
                <div class="panel-indicator"></div>
            </div>
            <div class="panel-content">
                <canvas id="polarPlot"></canvas>
            </div>
        </div>

        <!-- Ground Track Map -->
        <div class="panel map-container">
            <div class="panel-header">
                <span>GROUND TRACK // WORLD VIEW</span>
                <div class="panel-indicator"></div>
            </div>
            <div class="panel-content">
                <div id="groundMap"></div>
            </div>
        </div>

        <!-- Sidebar -->
        <div class="sidebar">
            <!-- Countdown -->
            <div class="panel countdown-panel">
                <div class="panel-header">
                    <span>NEXT PASS</span>
                    <div class="panel-indicator"></div>
                </div>
                <div class="countdown-display">
                    <div class="next-pass-label">Incoming Signal</div>
                    <div class="satellite-name" id="countdownSat">AWAITING DATA</div>
                    <div class="countdown-grid">
                        <div class="countdown-block">
                            <div class="countdown-value" id="countDays">--</div>
                            <div class="countdown-label">Days</div>
                        </div>
                        <div class="countdown-block">
                            <div class="countdown-value" id="countHours">--</div>
                            <div class="countdown-label">Hours</div>
                        </div>
                        <div class="countdown-block">
                            <div class="countdown-value" id="countMins">--</div>
                            <div class="countdown-label">Mins</div>
                        </div>
                        <div class="countdown-block">
                            <div class="countdown-value" id="countSecs">--</div>
                            <div class="countdown-label">Secs</div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Stats -->
            <div class="panel">
                <div class="panel-header">
                    <span>TRACKING STATS</span>
                    <div class="panel-indicator"></div>
                </div>
                <div class="panel-content">
                    <div class="stats-grid">
                        <div class="stat-box">
                            <div class="stat-value" id="statTracked">0</div>
                            <div class="stat-label">Satellites</div>
                        </div>
                        <div class="stat-box">
                            <div class="stat-value highlight" id="statVisible">0</div>
                            <div class="stat-label">Visible Now</div>
                        </div>
                        <div class="stat-box">
                            <div class="stat-value" id="statPasses">0</div>
                            <div class="stat-label">Passes Today</div>
                        </div>
                        <div class="stat-box">
                            <div class="stat-value" id="statMaxEl">0°</div>
                            <div class="stat-label">Best Elevation</div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Pass List -->
            <div class="panel pass-list">
                <div class="panel-header">
                    <span>UPCOMING PASSES</span>
                    <div class="panel-indicator"></div>
                </div>
                <div class="panel-content">
                    <div class="pass-list-content" id="passList">
                        <div style="text-align:center;color:var(--text-secondary);padding:20px;">
                            Calculating passes...
                        </div>
                    </div>
                </div>
            </div>

            <!-- Telemetry -->
            <div class="panel telemetry-panel">
                <div class="panel-header">
                    <span>LIVE TELEMETRY</span>
                    <div class="panel-indicator"></div>
                </div>
                <div class="panel-content">
                    <div class="telemetry-rows">
                        <div class="telemetry-row">
                            <span class="telemetry-label">Latitude</span>
                            <span class="telemetry-value" id="telLat">---.----°</span>
                        </div>
                        <div class="telemetry-row">
                            <span class="telemetry-label">Longitude</span>
                            <span class="telemetry-value" id="telLon">---.----°</span>
                        </div>
                        <div class="telemetry-row">
                            <span class="telemetry-label">Altitude</span>
                            <span class="telemetry-value" id="telAlt">--- km</span>
                        </div>
                        <div class="telemetry-row">
                            <span class="telemetry-label">Elevation</span>
                            <span class="telemetry-value" id="telEl">--.-°</span>
                        </div>
                        <div class="telemetry-row">
                            <span class="telemetry-label">Azimuth</span>
                            <span class="telemetry-value" id="telAz">---.-°</span>
                        </div>
                        <div class="telemetry-row">
                            <span class="telemetry-label">Distance</span>
                            <span class="telemetry-value" id="telDist">---- km</span>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Observer -->
            <div class="panel observer-panel">
                <div class="panel-header">
                    <span>OBSERVER LOCATION</span>
                    <div class="panel-indicator"></div>
                </div>
                <div class="panel-content">
                    <div class="observer-grid">
                        <div class="observer-input">
                            <label>Latitude</label>
                            <input type="number" id="obsLat" value="51.5074" step="0.0001">
                        </div>
                        <div class="observer-input">
                            <label>Longitude</label>
                            <input type="number" id="obsLon" value="-0.1278" step="0.0001">
                        </div>
                    </div>
                    <div class="observer-actions">
                        <button class="btn" onclick="getLocation()">📍 GPS</button>
                        <button class="btn primary" onclick="calculatePasses()">CALCULATE</button>
                    </div>
                </div>
            </div>
        </div>
    </main>

    <script>
        // Dashboard state
        let passes = [];
        let selectedPass = null;
        let groundMap = null;
        let satMarker = null;
        let trackLine = null;
        let observerMarker = null;
        let orbitTrack = null;
        let selectedSatellite = 25544; // Default to ISS

        const satellites = {
            25544: { name: 'ISS (ZARYA)', color: '#00ffff' },
            25338: { name: 'NOAA 15', color: '#00ff00' },
            28654: { name: 'NOAA 18', color: '#ff6600' },
            33591: { name: 'NOAA 19', color: '#ff3366' },
            40069: { name: 'METEOR-M2', color: '#9370DB' },
            43013: { name: 'NOAA 20', color: '#00ffaa' },
            54234: { name: 'METEOR-M2-3', color: '#ff00ff' }
        };

        // Handle satellite selection change
        function onSatelliteChange() {
            const select = document.getElementById('satSelect');
            selectedSatellite = parseInt(select.value);
            const satName = satellites[selectedSatellite]?.name || 'Unknown';

            // Update tracking status
            document.getElementById('trackingStatus').textContent = 'ACQUIRING ' + satName;
            document.getElementById('trackingDot').style.background = 'var(--accent-orange)';

            // Recalculate passes for new satellite
            calculatePasses();

            // Update real-time position immediately
            updateRealTimePositions();
        }

        // Initialize dashboard
        document.addEventListener('DOMContentLoaded', () => {
            initGroundMap();
            updateClock();
            setInterval(updateClock, 1000);
            setInterval(updateCountdown, 1000);
            setInterval(updateRealTimePositions, 5000);

            // Try to get location, then calculate
            getLocation();
        });

        function updateClock() {
            const now = new Date();
            document.getElementById('utcTime').textContent =
                now.toISOString().substring(11, 19) + ' UTC';
        }

        function initGroundMap() {
            groundMap = L.map('groundMap', {
                center: [20, 0],
                zoom: 2,
                minZoom: 1,
                maxZoom: 10,
                worldCopyJump: true
            });

            L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
                attribution: '©OpenStreetMap, ©CartoDB'
            }).addTo(groundMap);
        }

        function getLocation() {
            if (navigator.geolocation) {
                navigator.geolocation.getCurrentPosition(pos => {
                    document.getElementById('obsLat').value = pos.coords.latitude.toFixed(4);
                    document.getElementById('obsLon').value = pos.coords.longitude.toFixed(4);
                    calculatePasses();
                }, () => {
                    calculatePasses();
                });
            } else {
                calculatePasses();
            }
        }

        async function calculatePasses() {
            const lat = parseFloat(document.getElementById('obsLat').value);
            const lon = parseFloat(document.getElementById('obsLon').value);
            const satName = satellites[selectedSatellite]?.name || 'Unknown';

            try {
                const response = await fetch('/satellite/predict', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        latitude: lat,
                        longitude: lon,
                        hours: 48,
                        minEl: 5,
                        satellites: [selectedSatellite]
                    })
                });

                const data = await response.json();
                if (data.status === 'success') {
                    passes = data.passes;
                    renderPassList();
                    updateStats();
                    if (passes.length > 0) {
                        selectPass(0);
                    }
                    updateObserverMarker(lat, lon);

                    // Update tracking status
                    document.getElementById('trackingStatus').textContent = 'TRACKING ' + satName;
                    document.getElementById('trackingDot').style.background = 'var(--accent-green)';
                } else {
                    document.getElementById('trackingStatus').textContent = 'ERROR: ' + (data.message || 'Unknown');
                    document.getElementById('trackingDot').style.background = 'var(--accent-red)';
                }
            } catch (err) {
                console.error('Pass calculation error:', err);
                document.getElementById('trackingStatus').textContent = 'CONNECTION ERROR';
                document.getElementById('trackingDot').style.background = 'var(--accent-red)';
            }
        }

        function renderPassList() {
            const container = document.getElementById('passList');
            if (passes.length === 0) {
                container.innerHTML = '<div style="text-align:center;color:var(--text-secondary);padding:20px;">No passes found</div>';
                return;
            }

            container.innerHTML = passes.slice(0, 10).map((pass, idx) => {
                const quality = pass.maxEl >= 60 ? 'excellent' : pass.maxEl >= 30 ? 'good' : 'fair';
                const qualityText = pass.maxEl >= 60 ? 'EXCELLENT' : pass.maxEl >= 30 ? 'GOOD' : 'FAIR';
                const time = pass.startTime.split(' ')[1] || pass.startTime; // Extract time portion

                return `
                    <div class="pass-item ${selectedPass === idx ? 'active' : ''}" onclick="selectPass(${idx})">
                        <div class="pass-item-header">
                            <span class="pass-sat-name">${pass.satellite}</span>
                            <span class="pass-quality ${quality}">${qualityText}</span>
                        </div>
                        <div class="pass-item-details">
                            <span class="pass-time">${time}</span>
                            <span>${pass.maxEl.toFixed(0)}° max · ${pass.duration} min</span>
                        </div>
                    </div>
                `;
            }).join('');
        }

        function selectPass(idx) {
            selectedPass = idx;
            renderPassList();

            const pass = passes[idx];
            if (!pass) return;

            drawPolarPlot(pass);
            updateGroundTrack(pass);
            updateTelemetry(pass);
        }

        function drawPolarPlot(pass) {
            const canvas = document.getElementById('polarPlot');
            const ctx = canvas.getContext('2d');
            const rect = canvas.parentElement.getBoundingClientRect();
            canvas.width = rect.width;
            canvas.height = rect.height - 20;

            const cx = canvas.width / 2;
            const cy = canvas.height / 2;
            const radius = Math.min(cx, cy) - 40;

            // Clear
            ctx.fillStyle = '#0a0a0f';
            ctx.fillRect(0, 0, canvas.width, canvas.height);

            // Draw elevation rings
            ctx.strokeStyle = 'rgba(0, 212, 255, 0.15)';
            ctx.lineWidth = 1;
            for (let el = 30; el <= 90; el += 30) {
                const r = radius * (1 - el / 90);
                ctx.beginPath();
                ctx.arc(cx, cy, r, 0, Math.PI * 2);
                ctx.stroke();

                // Label
                ctx.fillStyle = 'rgba(0, 212, 255, 0.4)';
                ctx.font = '10px JetBrains Mono';
                ctx.fillText(el + '°', cx + 5, cy - r + 12);
            }

            // Horizon
            ctx.strokeStyle = 'rgba(0, 212, 255, 0.3)';
            ctx.lineWidth = 2;
            ctx.beginPath();
            ctx.arc(cx, cy, radius, 0, Math.PI * 2);
            ctx.stroke();

            // Cardinal lines
            ctx.strokeStyle = 'rgba(0, 212, 255, 0.1)';
            ctx.lineWidth = 1;
            for (let az = 0; az < 360; az += 45) {
                const angle = (az - 90) * Math.PI / 180;
                ctx.beginPath();
                ctx.moveTo(cx, cy);
                ctx.lineTo(cx + radius * Math.cos(angle), cy + radius * Math.sin(angle));
                ctx.stroke();
            }

            // Cardinal labels
            ctx.font = 'bold 14px Orbitron';
            const labels = [
                { text: 'N', az: 0, color: '#ff4444' },
                { text: 'E', az: 90, color: '#00d4ff' },
                { text: 'S', az: 180, color: '#00d4ff' },
                { text: 'W', az: 270, color: '#00d4ff' }
            ];
            labels.forEach(l => {
                const angle = (l.az - 90) * Math.PI / 180;
                const x = cx + (radius + 20) * Math.cos(angle);
                const y = cy + (radius + 20) * Math.sin(angle);
                ctx.fillStyle = l.color;
                ctx.textAlign = 'center';
                ctx.textBaseline = 'middle';
                ctx.fillText(l.text, x, y);
            });

            // Draw pass trajectory
            if (pass && pass.trajectory) {
                ctx.strokeStyle = pass.color || '#00d4ff';
                ctx.lineWidth = 3;
                ctx.setLineDash([8, 4]);
                ctx.beginPath();

                let maxElPoint = null;
                let maxEl = 0;

                pass.trajectory.forEach((pt, i) => {
                    const r = radius * (1 - pt.el / 90);
                    const angle = (pt.az - 90) * Math.PI / 180;
                    const x = cx + r * Math.cos(angle);
                    const y = cy + r * Math.sin(angle);

                    if (i === 0) ctx.moveTo(x, y);
                    else ctx.lineTo(x, y);

                    if (pt.el > maxEl) {
                        maxEl = pt.el;
                        maxElPoint = { x, y };
                    }
                });
                ctx.stroke();
                ctx.setLineDash([]);

                // Max elevation marker
                if (maxElPoint) {
                    ctx.beginPath();
                    ctx.arc(maxElPoint.x, maxElPoint.y, 8, 0, Math.PI * 2);
                    ctx.fillStyle = pass.color || '#00d4ff';
                    ctx.fill();
                    ctx.strokeStyle = '#fff';
                    ctx.lineWidth = 2;
                    ctx.stroke();

                    // Glow effect
                    ctx.beginPath();
                    ctx.arc(maxElPoint.x, maxElPoint.y, 15, 0, Math.PI * 2);
                    ctx.strokeStyle = pass.color || '#00d4ff';
                    ctx.lineWidth = 1;
                    ctx.globalAlpha = 0.3;
                    ctx.stroke();
                    ctx.globalAlpha = 1;
                }
            }
        }

        function updateGroundTrack(pass) {
            if (!groundMap) return;

            // Clear existing
            if (trackLine) groundMap.removeLayer(trackLine);
            if (satMarker) groundMap.removeLayer(satMarker);

            if (pass && pass.groundTrack) {
                const coords = pass.groundTrack.map(pt => [pt.lat, pt.lon]);

                trackLine = L.polyline(coords, {
                    color: pass.color || '#00d4ff',
                    weight: 3,
                    opacity: 0.8,
                    dashArray: '10, 5'
                }).addTo(groundMap);

                // Current position marker
                if (pass.currentPos) {
                    const satIcon = L.divIcon({
                        className: 'sat-marker',
                        html: `<div style="
                            width: 16px; height: 16px;
                            background: ${pass.color || '#00d4ff'};
                            border-radius: 50%;
                            border: 2px solid #fff;
                            box-shadow: 0 0 20px ${pass.color || '#00d4ff'};
                        "></div>`,
                        iconSize: [16, 16],
                        iconAnchor: [8, 8]
                    });

                    satMarker = L.marker([pass.currentPos.lat, pass.currentPos.lon], { icon: satIcon })
                        .addTo(groundMap)
                        .bindPopup(`<b>${pass.name}</b><br>Alt: ${pass.currentPos.alt?.toFixed(0)} km`);
                }

                groundMap.fitBounds(trackLine.getBounds(), { padding: [30, 30] });
            }
        }

        function updateObserverMarker(lat, lon) {
            if (!groundMap) return;

            if (observerMarker) groundMap.removeLayer(observerMarker);

            const obsIcon = L.divIcon({
                className: 'obs-marker',
                html: `<div style="
                    width: 12px; height: 12px;
                    background: #ff9500;
                    border-radius: 50%;
                    border: 2px solid #fff;
                    box-shadow: 0 0 15px #ff9500;
                "></div>`,
                iconSize: [12, 12],
                iconAnchor: [6, 6]
            });

            observerMarker = L.marker([lat, lon], { icon: obsIcon })
                .addTo(groundMap)
                .bindPopup('Observer Location');
        }

        function updateStats() {
            document.getElementById('statTracked').textContent = Object.keys(satellites).length;
            document.getElementById('statPasses').textContent = passes.length;

            const maxEl = passes.reduce((max, p) => Math.max(max, p.maxEl || 0), 0);
            document.getElementById('statMaxEl').textContent = maxEl.toFixed(0) + '°';
        }

        function updateTelemetry(pass) {
            if (!pass || !pass.currentPos) {
                document.getElementById('telLat').textContent = '---.----°';
                document.getElementById('telLon').textContent = '---.----°';
                document.getElementById('telAlt').textContent = '--- km';
                document.getElementById('telEl').textContent = '--.-°';
                document.getElementById('telAz').textContent = '---.-°';
                document.getElementById('telDist').textContent = '---- km';
                return;
            }

            const pos = pass.currentPos;
            document.getElementById('telLat').textContent = (pos.lat || 0).toFixed(4) + '°';
            document.getElementById('telLon').textContent = (pos.lon || 0).toFixed(4) + '°';
            document.getElementById('telAlt').textContent = (pos.alt || 0).toFixed(0) + ' km';
            document.getElementById('telEl').textContent = (pos.el || 0).toFixed(1) + '°';
            document.getElementById('telAz').textContent = (pos.az || 0).toFixed(1) + '°';
            document.getElementById('telDist').textContent = (pos.dist || 0).toFixed(0) + ' km';
        }

        function updateCountdown() {
            if (!passes || passes.length === 0) {
                document.getElementById('countdownSat').textContent = 'NO PASSES FOUND';
                document.getElementById('countDays').textContent = '--';
                document.getElementById('countHours').textContent = '--';
                document.getElementById('countMins').textContent = '--';
                document.getElementById('countSecs').textContent = '--';
                return;
            }

            const now = new Date();
            let nextPass = null;

            for (const pass of passes) {
                // Parse the startTimeISO field
                const start = new Date(pass.startTimeISO);
                if (start > now) {
                    nextPass = pass;
                    break;
                }
            }

            if (!nextPass) {
                // All passes are in the past, show the first one anyway
                nextPass = passes[0];
            }

            document.getElementById('countdownSat').textContent = nextPass.satellite;

            const passTime = new Date(nextPass.startTimeISO);
            const diff = passTime - now;

            if (diff <= 0) {
                // Pass is happening now or passed
                document.getElementById('countDays').textContent = '00';
                document.getElementById('countHours').textContent = '00';
                document.getElementById('countMins').textContent = '00';
                document.getElementById('countSecs').textContent = '00';
                return;
            }

            const days = Math.floor(diff / (1000 * 60 * 60 * 24));
            const hours = Math.floor((diff % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
            const mins = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60));
            const secs = Math.floor((diff % (1000 * 60)) / 1000);

            document.getElementById('countDays').textContent = days.toString().padStart(2, '0');
            document.getElementById('countHours').textContent = hours.toString().padStart(2, '0');
            document.getElementById('countMins').textContent = mins.toString().padStart(2, '0');
            document.getElementById('countSecs').textContent = secs.toString().padStart(2, '0');

            // Animate if within 1 minute
            const elements = ['countDays', 'countHours', 'countMins', 'countSecs'].map(id => document.getElementById(id));
            if (diff < 60000) {
                elements.forEach(el => el.classList.add('active'));
            } else {
                elements.forEach(el => el.classList.remove('active'));
            }
        }

        async function updateRealTimePositions() {
            const lat = parseFloat(document.getElementById('obsLat').value);
            const lon = parseFloat(document.getElementById('obsLon').value);
            const satColor = satellites[selectedSatellite]?.color || '#00d4ff';

            try {
                const response = await fetch('/satellite/position', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        latitude: lat,
                        longitude: lon,
                        satellites: [selectedSatellite],
                        includeTrack: true
                    })
                });

                const data = await response.json();
                if (data.status === 'success' && data.positions.length > 0) {
                    const pos = data.positions[0];

                    // Update telemetry
                    document.getElementById('telLat').textContent = pos.lat.toFixed(4) + '°';
                    document.getElementById('telLon').textContent = pos.lon.toFixed(4) + '°';
                    document.getElementById('telAlt').textContent = pos.altitude.toFixed(0) + ' km';
                    document.getElementById('telEl').textContent = pos.elevation.toFixed(1) + '°';
                    document.getElementById('telAz').textContent = pos.azimuth.toFixed(1) + '°';
                    document.getElementById('telDist').textContent = pos.distance.toFixed(0) + ' km';

                    // Update visible count
                    document.getElementById('statVisible').textContent = pos.elevation > 0 ? '1' : '0';

                    // Update satellite marker on map
                    if (groundMap) {
                        if (satMarker) groundMap.removeLayer(satMarker);

                        const satIcon = L.divIcon({
                            className: 'sat-marker-live',
                            html: `<div style="
                                width: 20px; height: 20px;
                                background: ${satColor};
                                border-radius: 50%;
                                border: 3px solid #fff;
                                box-shadow: 0 0 20px ${satColor}, 0 0 40px ${satColor};
                            "></div>`,
                            iconSize: [20, 20],
                            iconAnchor: [10, 10]
                        });
                        satMarker = L.marker([pos.lat, pos.lon], { icon: satIcon }).addTo(groundMap);
                    }

                    // Update orbit track
                    if (pos.track && groundMap) {
                        if (orbitTrack) groundMap.removeLayer(orbitTrack);
                        orbitTrack = L.polyline(pos.track.map(p => [p.lat, p.lon]), {
                            color: satColor,
                            weight: 2,
                            opacity: 0.6,
                            dashArray: '5, 5'
                        }).addTo(groundMap);
                    }

                    // Update polar plot with current position
                    drawPolarPlotWithPosition(pos.azimuth, pos.elevation, satColor);
                }
            } catch (err) {
                console.error('Position update error:', err);
            }
        }

        function drawPolarPlotWithPosition(az, el, color) {
            const canvas = document.getElementById('polarPlot');
            const ctx = canvas.getContext('2d');
            const rect = canvas.parentElement.getBoundingClientRect();
            canvas.width = rect.width;
            canvas.height = rect.height - 20;

            const cx = canvas.width / 2;
            const cy = canvas.height / 2;
            const radius = Math.min(cx, cy) - 40;

            // Clear
            ctx.fillStyle = '#0a0a0f';
            ctx.fillRect(0, 0, canvas.width, canvas.height);

            // Draw elevation rings
            ctx.strokeStyle = 'rgba(0, 212, 255, 0.15)';
            ctx.lineWidth = 1;
            for (let elRing = 30; elRing <= 90; elRing += 30) {
                const r = radius * (1 - elRing / 90);
                ctx.beginPath();
                ctx.arc(cx, cy, r, 0, Math.PI * 2);
                ctx.stroke();

                ctx.fillStyle = 'rgba(0, 212, 255, 0.4)';
                ctx.font = '10px JetBrains Mono';
                ctx.fillText(elRing + '°', cx + 5, cy - r + 12);
            }

            // Horizon
            ctx.strokeStyle = 'rgba(0, 212, 255, 0.3)';
            ctx.lineWidth = 2;
            ctx.beginPath();
            ctx.arc(cx, cy, radius, 0, Math.PI * 2);
            ctx.stroke();

            // Cardinal lines
            ctx.strokeStyle = 'rgba(0, 212, 255, 0.1)';
            ctx.lineWidth = 1;
            for (let azLine = 0; azLine < 360; azLine += 45) {
                const angle = (azLine - 90) * Math.PI / 180;
                ctx.beginPath();
                ctx.moveTo(cx, cy);
                ctx.lineTo(cx + radius * Math.cos(angle), cy + radius * Math.sin(angle));
                ctx.stroke();
            }

            // Cardinal labels
            ctx.font = 'bold 14px Orbitron';
            const labels = [
                { text: 'N', az: 0, color: '#ff4444' },
                { text: 'E', az: 90, color: '#00d4ff' },
                { text: 'S', az: 180, color: '#00d4ff' },
                { text: 'W', az: 270, color: '#00d4ff' }
            ];
            labels.forEach(l => {
                const angle = (l.az - 90) * Math.PI / 180;
                const x = cx + (radius + 20) * Math.cos(angle);
                const y = cy + (radius + 20) * Math.sin(angle);
                ctx.fillStyle = l.color;
                ctx.textAlign = 'center';
                ctx.textBaseline = 'middle';
                ctx.fillText(l.text, x, y);
            });

            // Draw pass trajectory if available
            if (passes.length > 0 && selectedPass !== null && passes[selectedPass]?.trajectory) {
                const pass = passes[selectedPass];
                ctx.strokeStyle = color;
                ctx.lineWidth = 2;
                ctx.setLineDash([8, 4]);
                ctx.globalAlpha = 0.5;
                ctx.beginPath();

                pass.trajectory.forEach((pt, i) => {
                    const r = radius * (1 - pt.el / 90);
                    const angle = (pt.az - 90) * Math.PI / 180;
                    const x = cx + r * Math.cos(angle);
                    const y = cy + r * Math.sin(angle);

                    if (i === 0) ctx.moveTo(x, y);
                    else ctx.lineTo(x, y);
                });
                ctx.stroke();
                ctx.setLineDash([]);
                ctx.globalAlpha = 1;
            }

            // Draw current satellite position
            if (el > -5) { // Show even slightly below horizon
                const posEl = Math.max(0, el);
                const r = radius * (1 - posEl / 90);
                const angle = (az - 90) * Math.PI / 180;
                const x = cx + r * Math.cos(angle);
                const y = cy + r * Math.sin(angle);

                // Glow effect
                const gradient = ctx.createRadialGradient(x, y, 0, x, y, 25);
                gradient.addColorStop(0, color);
                gradient.addColorStop(1, 'transparent');
                ctx.fillStyle = gradient;
                ctx.globalAlpha = 0.4;
                ctx.beginPath();
                ctx.arc(x, y, 25, 0, Math.PI * 2);
                ctx.fill();
                ctx.globalAlpha = 1;

                // Main marker
                ctx.beginPath();
                ctx.arc(x, y, 10, 0, Math.PI * 2);
                ctx.fillStyle = color;
                ctx.fill();
                ctx.strokeStyle = '#fff';
                ctx.lineWidth = 3;
                ctx.stroke();

                // Satellite label
                ctx.font = 'bold 11px Orbitron';
                ctx.fillStyle = '#fff';
                ctx.textAlign = 'center';
                ctx.fillText(satellites[selectedSatellite]?.name || 'SAT', x, y - 20);

                // Elevation indicator
                ctx.font = '10px JetBrains Mono';
                ctx.fillStyle = el > 0 ? '#00ff88' : '#ff4444';
                ctx.fillText(el.toFixed(1) + '°', x, y + 25);
            } else {
                // Below horizon indicator
                ctx.font = '12px Rajdhani';
                ctx.fillStyle = '#ff4444';
                ctx.textAlign = 'center';
                ctx.fillText('BELOW HORIZON', cx, cy + radius + 35);
            }
        }
    </script>
</body>
</html>
''')


@app.route('/satellite/predict', methods=['POST'])
def predict_passes():
    """Calculate satellite passes using skyfield for accurate orbital prediction."""
    from datetime import datetime, timedelta

    try:
        from skyfield.api import load, wgs84, EarthSatellite
        from skyfield.almanac import find_discrete
    except ImportError:
        # Fallback if skyfield not installed
        return jsonify({
            'status': 'error',
            'message': 'skyfield library not installed. Run: pip install skyfield'
        })

    data = request.json
    lat = data.get('latitude', data.get('lat', 51.5074))
    lon = data.get('longitude', data.get('lon', -0.1278))
    hours = data.get('hours', 24)
    min_el = data.get('minEl', 10)

    # Map NORAD IDs to satellite names
    norad_to_name = {
        25544: 'ISS',
        25338: 'NOAA-15',
        28654: 'NOAA-18',
        33591: 'NOAA-19',
        43013: 'NOAA-20',
        40069: 'METEOR-M2',
        57166: 'METEOR-M2-3'
    }

    sat_input = data.get('satellites', ['ISS', 'NOAA-15', 'NOAA-18', 'NOAA-19'])
    # Convert NORAD IDs to names if needed
    satellites = []
    for sat in sat_input:
        if isinstance(sat, int) and sat in norad_to_name:
            satellites.append(norad_to_name[sat])
        else:
            satellites.append(sat)

    passes = []
    colors = {
        'ISS': '#00ffff',
        'NOAA-15': '#00ff00',
        'NOAA-18': '#ff6600',
        'NOAA-19': '#ff3366',
        'NOAA-20': '#00ffaa',
        'METEOR-M2': '#9370DB',
        'METEOR-M2-3': '#ff00ff'
    }
    name_to_norad = {v: k for k, v in norad_to_name.items()}

    ts = load.timescale()
    observer = wgs84.latlon(lat, lon)

    t0 = ts.now()
    t1 = ts.utc(t0.utc_datetime() + timedelta(hours=hours))

    for sat_name in satellites:
        if sat_name not in TLE_SATELLITES:
            continue

        tle_data = TLE_SATELLITES[sat_name]
        try:
            satellite = EarthSatellite(tle_data[1], tle_data[2], tle_data[0], ts)
        except Exception:
            continue

        # Find passes by checking when satellite is above minimum elevation
        def above_horizon(t):
            diff = satellite - observer
            topocentric = diff.at(t)
            alt, _, _ = topocentric.altaz()
            return alt.degrees > 0

        above_horizon.step_days = 1/720  # Check every 2 minutes

        try:
            times, events = find_discrete(t0, t1, above_horizon)
        except Exception:
            continue

        # Process rise/set pairs
        i = 0
        while i < len(times):
            # Find rise event (event = True)
            if i < len(events) and events[i]:
                rise_time = times[i]

                # Find corresponding set event
                set_time = None
                for j in range(i + 1, len(times)):
                    if not events[j]:
                        set_time = times[j]
                        i = j
                        break

                if set_time is None:
                    i += 1
                    continue

                # Generate trajectory points between rise and set
                trajectory = []
                max_elevation = 0
                num_points = 30

                duration_seconds = (set_time.utc_datetime() - rise_time.utc_datetime()).total_seconds()

                for k in range(num_points):
                    frac = k / (num_points - 1)
                    t_point = ts.utc(rise_time.utc_datetime() + timedelta(seconds=duration_seconds * frac))

                    diff = satellite - observer
                    topocentric = diff.at(t_point)
                    alt, az, _ = topocentric.altaz()

                    el = alt.degrees
                    azimuth = az.degrees

                    if el > max_elevation:
                        max_elevation = el

                    trajectory.append({'el': max(0, el), 'az': azimuth})

                # Only include pass if max elevation meets minimum requirement
                if max_elevation >= min_el:
                    duration_minutes = int(duration_seconds / 60)

                    # Generate ground track (sub-satellite points)
                    ground_track = []
                    for k in range(60):  # 60 points for smoother track
                        frac = k / 59
                        t_point = ts.utc(rise_time.utc_datetime() + timedelta(seconds=duration_seconds * frac))
                        geocentric = satellite.at(t_point)
                        subpoint = wgs84.subpoint(geocentric)
                        ground_track.append({
                            'lat': subpoint.latitude.degrees,
                            'lon': subpoint.longitude.degrees
                        })

                    # Get current position
                    current_geo = satellite.at(ts.now())
                    current_subpoint = wgs84.subpoint(current_geo)
                    current_topo = (satellite - observer).at(ts.now())
                    current_alt, current_az, _ = current_topo.altaz()

                    passes.append({
                        'satellite': sat_name,
                        'norad': name_to_norad.get(sat_name, 0),
                        'startTime': rise_time.utc_datetime().strftime('%Y-%m-%d %H:%M UTC'),
                        'startTimeISO': rise_time.utc_datetime().isoformat(),
                        'maxEl': round(max_elevation, 1),
                        'duration': duration_minutes,
                        'trajectory': trajectory,
                        'groundTrack': ground_track,
                        'currentPos': {
                            'lat': current_subpoint.latitude.degrees,
                            'lon': current_subpoint.longitude.degrees
                        },
                        'color': colors.get(sat_name, '#00ff00')
                    })

            i += 1

    # Sort by time
    passes.sort(key=lambda p: p['startTime'])

    return jsonify({
        'status': 'success',
        'passes': passes
    })


@app.route('/satellite/position', methods=['POST'])
def get_satellite_position():
    """Get real-time positions of satellites with full orbit ground track."""
    from datetime import datetime, timedelta

    try:
        from skyfield.api import load, wgs84, EarthSatellite
    except ImportError:
        return jsonify({'status': 'error', 'message': 'skyfield not installed'})

    data = request.json
    lat = data.get('latitude', data.get('lat', 51.5074))
    lon = data.get('longitude', data.get('lon', -0.1278))
    sat_input = data.get('satellites', [])
    include_track = data.get('includeTrack', True)

    # Map NORAD IDs to satellite names
    norad_to_name = {
        25544: 'ISS',
        25338: 'NOAA-15',
        28654: 'NOAA-18',
        33591: 'NOAA-19',
        43013: 'NOAA-20',
        40069: 'METEOR-M2',
        57166: 'METEOR-M2-3'
    }

    # Convert NORAD IDs to names if needed
    satellites = []
    for sat in sat_input:
        if isinstance(sat, int) and sat in norad_to_name:
            satellites.append(norad_to_name[sat])
        else:
            satellites.append(sat)

    ts = load.timescale()
    observer = wgs84.latlon(lat, lon)
    now = ts.now()
    now_dt = now.utc_datetime()

    positions = []

    for sat_name in satellites:
        if sat_name not in TLE_SATELLITES:
            continue

        tle_data = TLE_SATELLITES[sat_name]
        try:
            satellite = EarthSatellite(tle_data[1], tle_data[2], tle_data[0], ts)

            # Get current geocentric position
            geocentric = satellite.at(now)
            subpoint = wgs84.subpoint(geocentric)

            # Get topocentric position (from observer)
            diff = satellite - observer
            topocentric = diff.at(now)
            alt, az, distance = topocentric.altaz()

            pos_data = {
                'satellite': sat_name,
                'lat': subpoint.latitude.degrees,
                'lon': subpoint.longitude.degrees,
                'altitude': geocentric.distance().km - 6371,
                'elevation': alt.degrees,
                'azimuth': az.degrees,
                'distance': distance.km,
                'visible': alt.degrees > 0
            }

            # Generate full orbit ground track (±45 minutes = ~1 orbit for LEO)
            if include_track:
                orbit_track = []
                # Past 45 minutes to future 45 minutes in 1-minute intervals
                for minutes_offset in range(-45, 46, 1):
                    t_point = ts.utc(now_dt + timedelta(minutes=minutes_offset))
                    try:
                        geo = satellite.at(t_point)
                        sp = wgs84.subpoint(geo)
                        orbit_track.append({
                            'lat': sp.latitude.degrees,
                            'lon': sp.longitude.degrees,
                            'past': minutes_offset < 0
                        })
                    except:
                        continue

                pos_data['track'] = orbit_track

            positions.append(pos_data)
        except Exception:
            continue

    return jsonify({
        'status': 'success',
        'positions': positions,
        'timestamp': datetime.utcnow().isoformat()
    })


@app.route('/satellite/update-tle', methods=['POST'])
def update_tle():
    """Update TLE data from CelesTrak."""
    global TLE_SATELLITES

    try:
        import urllib.request

        # Map our satellite names to CelesTrak groups
        groups_to_fetch = {
            'stations': ['ISS'],
            'weather': ['NOAA-15', 'NOAA-18', 'NOAA-19', 'METEOR-M2']
        }

        # Name mappings from CelesTrak to our internal names
        name_mappings = {
            'ISS (ZARYA)': 'ISS',
            'NOAA 15': 'NOAA-15',
            'NOAA 18': 'NOAA-18',
            'NOAA 19': 'NOAA-19',
            'METEOR-M 2': 'METEOR-M2',
            'METEOR-M2 2': 'METEOR-M2'
        }

        updated = []

        for group, sats in groups_to_fetch.items():
            try:
                url = f'https://celestrak.org/NORAD/elements/gp.php?GROUP={group}&FORMAT=tle'
                req = urllib.request.Request(url, headers={'User-Agent': 'INTERCEPT/1.0'})

                with urllib.request.urlopen(req, timeout=10) as response:
                    tle_data = response.read().decode('utf-8')

                lines = [l.strip() for l in tle_data.strip().split('\n') if l.strip()]

                for i in range(0, len(lines) - 2, 3):
                    name = lines[i]
                    line1 = lines[i + 1]
                    line2 = lines[i + 2]

                    if line1.startswith('1 ') and line2.startswith('2 '):
                        # Check if this satellite is one we track
                        internal_name = name_mappings.get(name)
                        if internal_name and internal_name in sats:
                            TLE_SATELLITES[internal_name] = (name, line1, line2)
                            updated.append(internal_name)
            except Exception:
                continue

        return jsonify({
            'status': 'success',
            'message': f'Updated TLE for: {", ".join(updated) if updated else "none"}',
            'updated': updated
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })


@app.route('/satellite/celestrak/<category>')
def fetch_celestrak(category):
    """Fetch TLE data from CelesTrak for a specific category."""
    global TLE_SATELLITES
    import urllib.request

    # Map category names to Celestrak groups
    category_map = {
        'stations': 'stations',
        'visual': 'visual',
        'weather': 'weather',
        'noaa': 'noaa',
        'amateur': 'amateur',
        'starlink': 'starlink',
        'gps-ops': 'gps-ops',
        'iridium': 'iridium'
    }

    if category not in category_map:
        return jsonify({'status': 'error', 'message': 'Unknown category'})

    try:
        url = f'https://celestrak.org/NORAD/elements/gp.php?GROUP={category_map[category]}&FORMAT=tle'
        req = urllib.request.Request(url, headers={'User-Agent': 'INTERCEPT/1.0'})

        with urllib.request.urlopen(req, timeout=10) as response:
            tle_data = response.read().decode('utf-8')

        lines = [l.strip() for l in tle_data.strip().split('\n') if l.strip()]
        satellites = []

        # Parse TLE (3 lines per satellite)
        for i in range(0, len(lines) - 2, 3):
            name = lines[i]
            line1 = lines[i + 1]
            line2 = lines[i + 2]

            if line1.startswith('1 ') and line2.startswith('2 '):
                norad = line1[2:7].strip()
                sat_id = name.replace(' ', '-').replace('/', '-').upper()[:20]
                satellites.append({
                    'id': sat_id,
                    'name': name,
                    'norad': norad,
                    'tle': [name, line1, line2]
                })
                # Also add to TLE_SATELLITES for prediction
                TLE_SATELLITES[sat_id] = (name, line1, line2)

        # Limit to first 50 satellites to avoid overwhelming the UI
        return jsonify({
            'status': 'success',
            'satellites': satellites[:50],
            'total': len(satellites)
        })
    except urllib.error.URLError as e:
        return jsonify({'status': 'error', 'message': f'Network error: {str(e)}'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})


# ============================================
# IRIDIUM ROUTES
# ============================================

@app.route('/iridium/tools')
def check_iridium_tools():
    """Check for Iridium decoding tools."""
    # Check for iridium-extractor or gr-iridium
    has_tool = shutil.which('iridium-extractor') is not None or shutil.which('iridium-parser') is not None
    return jsonify({'available': has_tool})


@app.route('/iridium/start', methods=['POST'])
def start_iridium():
    """Start Iridium burst capture."""
    global satellite_process

    with satellite_lock:
        if satellite_process and satellite_process.poll() is None:
            return jsonify({'status': 'error', 'message': 'Iridium capture already running'})

    data = request.json
    freq = data.get('freq', '1626.0')
    gain = data.get('gain', '40')
    sample_rate = data.get('sampleRate', '2.048e6')
    device = data.get('device', '0')

    # Check for tools
    if not shutil.which('iridium-extractor') and not shutil.which('rtl_fm'):
        return jsonify({
            'status': 'error',
            'message': 'Iridium tools not found. Install gr-iridium or use rtl_fm for basic capture.'
        })

    try:
        # For demo, use rtl_fm to capture L-band (iridium-extractor would be better)
        # Real implementation would pipe to iridium-extractor
        cmd = [
            'rtl_fm',
            '-f', f'{float(freq)}M',
            '-g', str(gain),
            '-s', sample_rate,
            '-d', str(device),
            '-'
        ]

        satellite_process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        # Start monitoring thread
        thread = threading.Thread(target=monitor_iridium, args=(satellite_process,), daemon=True)
        thread.start()

        return jsonify({'status': 'started'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})


@app.route('/iridium/stop', methods=['POST'])
def stop_iridium():
    """Stop Iridium capture."""
    global satellite_process, iridium_bursts

    with satellite_lock:
        if satellite_process:
            satellite_process.terminate()
            try:
                satellite_process.wait(timeout=5)
            except:
                satellite_process.kill()
            satellite_process = None

    return jsonify({'status': 'stopped'})


@app.route('/iridium/stream')
def stream_iridium():
    """SSE stream for Iridium bursts."""
    def generate():
        while True:
            try:
                msg = satellite_queue.get(timeout=1)
                yield f"data: {json.dumps(msg)}\n\n"
            except queue.Empty:
                yield f"data: {json.dumps({'type': 'keepalive'})}\n\n"

    response = Response(generate(), mimetype='text/event-stream')
    response.headers['Cache-Control'] = 'no-cache'
    response.headers['X-Accel-Buffering'] = 'no'
    return response


def monitor_iridium(process):
    """Monitor Iridium capture and detect bursts."""
    import time
    from datetime import datetime

    # Simulated burst detection (real implementation would use proper Iridium decoding)
    # With gr-iridium, you'd parse the actual burst frames

    try:
        burst_count = 0
        while process.poll() is None:
            # Read some data
            data = process.stdout.read(1024)
            if data:
                # Simulate burst detection based on signal energy
                # Real implementation would decode Iridium frames
                if len(data) > 0 and burst_count < 100:
                    # Occasional simulated burst for demo
                    import random
                    if random.random() < 0.01:  # 1% chance per read
                        burst = {
                            'type': 'burst',
                            'time': datetime.now().strftime('%H:%M:%S.%f')[:-3],
                            'frequency': f"{1616 + random.random() * 10:.3f}",
                            'data': f"Frame data (simulated) - Burst #{burst_count + 1}"
                        }
                        satellite_queue.put(burst)
                        iridium_bursts.append(burst)
                        burst_count += 1

            time.sleep(0.1)
    except Exception as e:
        print(f"[Iridium] Monitor error: {e}")


def cleanup_stale_processes():
    """Kill any stale processes from previous runs (but not system services)."""
    # Note: dump1090 is NOT included here as users may run it as a system service
    processes_to_kill = ['rtl_adsb', 'rtl_433', 'multimon-ng', 'rtl_fm']
    for proc_name in processes_to_kill:
        try:
            subprocess.run(['pkill', '-9', proc_name], capture_output=True)
        except:
            pass


def main():
    print("=" * 50)
    print("  INTERCEPT // Signal Intelligence")
    print("  Pager / 433MHz / Aircraft / Satellite / WiFi / BT")
    print("=" * 50)
    print()

    # Clean up any stale processes from previous runs
    cleanup_stale_processes()

    print("Open http://localhost:5050 in your browser")
    print()
    print("Press Ctrl+C to stop")
    print()

    app.run(host='0.0.0.0', port=5050, debug=False, threaded=True)


if __name__ == '__main__':
    main()
