/**
 * Intercept - Core Application Logic
 * Global state, mode switching, and shared functionality
 */

// ============== GLOBAL STATE ==============

// Mode state flags
let eventSource = null;
let isRunning = false;
let isSensorRunning = false;
let isAdsbRunning = false;
let isWifiRunning = false;
let isBtRunning = false;
let currentMode = 'pager';

// Message counters
let msgCount = 0;
let pocsagCount = 0;
let flexCount = 0;
let sensorCount = 0;
let filteredCount = 0;

// Device list (populated from server via Jinja2)
let deviceList = [];

// Auto-scroll setting
let autoScroll = localStorage.getItem('autoScroll') !== 'false';

// Mute setting
let muted = localStorage.getItem('audioMuted') === 'true';

// Observer location (load from localStorage or default to London)
let observerLocation = (function() {
    const saved = localStorage.getItem('observerLocation');
    if (saved) {
        try {
            const parsed = JSON.parse(saved);
            if (parsed.lat && parsed.lon) return parsed;
        } catch (e) {}
    }
    return { lat: 51.5074, lon: -0.1278 };
})();

// Message storage for export
let allMessages = [];

// Track unique sensor devices
let uniqueDevices = new Set();

// SDR device usage tracking
let sdrDeviceUsage = {};

// ============== DISCLAIMER HANDLING ==============

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

// ============== HEADER CLOCK ==============

function updateHeaderClock() {
    const now = new Date();
    const utc = now.toISOString().substring(11, 19);
    document.getElementById('headerUtcTime').textContent = utc;
}

// ============== HEADER STATS SYNC ==============

function syncHeaderStats() {
    // Pager stats
    document.getElementById('headerMsgCount').textContent = msgCount;
    document.getElementById('headerPocsagCount').textContent = pocsagCount;
    document.getElementById('headerFlexCount').textContent = flexCount;

    // Sensor stats
    document.getElementById('headerSensorCount').textContent = document.getElementById('sensorCount')?.textContent || '0';
    document.getElementById('headerDeviceTypeCount').textContent = document.getElementById('deviceCount')?.textContent || '0';

    // WiFi stats
    document.getElementById('headerApCount').textContent = document.getElementById('apCount')?.textContent || '0';
    document.getElementById('headerClientCount').textContent = document.getElementById('clientCount')?.textContent || '0';
    document.getElementById('headerHandshakeCount').textContent = document.getElementById('handshakeCount')?.textContent || '0';
    document.getElementById('headerDroneCount').textContent = document.getElementById('droneCount')?.textContent || '0';

    // Bluetooth stats
    document.getElementById('headerBtDeviceCount').textContent = document.getElementById('btDeviceCount')?.textContent || '0';
    document.getElementById('headerBtBeaconCount').textContent = document.getElementById('btBeaconCount')?.textContent || '0';

    // Aircraft stats
    document.getElementById('headerAircraftCount').textContent = document.getElementById('aircraftCount')?.textContent || '0';
    document.getElementById('headerAdsbMsgCount').textContent = document.getElementById('adsbMsgCount')?.textContent || '0';
    document.getElementById('headerIcaoCount').textContent = document.getElementById('icaoCount')?.textContent || '0';

    // Satellite stats
    document.getElementById('headerPassCount').textContent = document.getElementById('passCount')?.textContent || '0';
}

// ============== MODE SWITCHING ==============

function switchMode(mode) {
    // Stop any running scans when switching modes
    if (isRunning && typeof stopDecoding === 'function') stopDecoding();
    if (isSensorRunning && typeof stopSensorDecoding === 'function') stopSensorDecoding();
    if (isWifiRunning && typeof stopWifiScan === 'function') stopWifiScan();
    if (isBtRunning && typeof stopBtScan === 'function') stopBtScan();
    if (isAdsbRunning && typeof stopAdsbScan === 'function') stopAdsbScan();

    currentMode = mode;

    // Remove active from all nav buttons, then add to the correct one
    document.querySelectorAll('.mode-nav-btn').forEach(btn => btn.classList.remove('active'));
    const modeMap = {
        'pager': 'pager', 'sensor': '433', 'aircraft': 'aircraft',
        'satellite': 'satellite', 'wifi': 'wifi', 'bluetooth': 'bluetooth',
        'listening': 'listening'
    };
    document.querySelectorAll('.mode-nav-btn').forEach(btn => {
        const label = btn.querySelector('.nav-label');
        if (label && label.textContent.toLowerCase().includes(modeMap[mode])) {
            btn.classList.add('active');
        }
    });

    // Toggle mode content visibility
    document.getElementById('pagerMode').classList.toggle('active', mode === 'pager');
    document.getElementById('sensorMode').classList.toggle('active', mode === 'sensor');
    document.getElementById('aircraftMode').classList.toggle('active', mode === 'aircraft');
    document.getElementById('satelliteMode').classList.toggle('active', mode === 'satellite');
    document.getElementById('wifiMode').classList.toggle('active', mode === 'wifi');
    document.getElementById('bluetoothMode').classList.toggle('active', mode === 'bluetooth');
    document.getElementById('listeningPostMode').classList.toggle('active', mode === 'listening');

    // Toggle stats visibility
    document.getElementById('pagerStats').style.display = mode === 'pager' ? 'flex' : 'none';
    document.getElementById('sensorStats').style.display = mode === 'sensor' ? 'flex' : 'none';
    document.getElementById('aircraftStats').style.display = mode === 'aircraft' ? 'flex' : 'none';
    document.getElementById('satelliteStats').style.display = mode === 'satellite' ? 'flex' : 'none';
    document.getElementById('wifiStats').style.display = mode === 'wifi' ? 'flex' : 'none';
    document.getElementById('btStats').style.display = mode === 'bluetooth' ? 'flex' : 'none';

    // Hide signal meter - individual panels show signal strength where needed
    document.getElementById('signalMeter').style.display = 'none';

    // Update header stats groups
    document.getElementById('headerPagerStats').classList.toggle('active', mode === 'pager');
    document.getElementById('headerSensorStats').classList.toggle('active', mode === 'sensor');
    document.getElementById('headerAircraftStats').classList.toggle('active', mode === 'aircraft');
    document.getElementById('headerSatelliteStats').classList.toggle('active', mode === 'satellite');
    document.getElementById('headerWifiStats').classList.toggle('active', mode === 'wifi');
    document.getElementById('headerBtStats').classList.toggle('active', mode === 'bluetooth');

    // Show/hide dashboard buttons in nav bar
    document.getElementById('adsbDashboardBtn').style.display = mode === 'aircraft' ? 'inline-flex' : 'none';
    document.getElementById('satelliteDashboardBtn').style.display = mode === 'satellite' ? 'inline-flex' : 'none';

    // Update active mode indicator
    const modeNames = {
        'pager': 'PAGER',
        'sensor': '433MHZ',
        'aircraft': 'AIRCRAFT',
        'satellite': 'SATELLITE',
        'wifi': 'WIFI',
        'bluetooth': 'BLUETOOTH',
        'listening': 'LISTENING POST'
    };
    document.getElementById('activeModeIndicator').innerHTML = '<span class="pulse-dot"></span>' + modeNames[mode];

    // Toggle layout containers
    document.getElementById('wifiLayoutContainer').style.display = mode === 'wifi' ? 'flex' : 'none';
    document.getElementById('btLayoutContainer').style.display = mode === 'bluetooth' ? 'flex' : 'none';

    // Respect the "Show Radar Display" checkbox for aircraft mode
    const showRadar = document.getElementById('adsbEnableMap')?.checked;
    document.getElementById('aircraftVisuals').style.display = (mode === 'aircraft' && showRadar) ? 'grid' : 'none';
    document.getElementById('satelliteVisuals').style.display = mode === 'satellite' ? 'block' : 'none';
    document.getElementById('listeningPostVisuals').style.display = mode === 'listening' ? 'grid' : 'none';

    // Update output panel title based on mode
    const titles = {
        'pager': 'Pager Decoder',
        'sensor': '433MHz Sensor Monitor',
        'aircraft': 'ADS-B Aircraft Tracker',
        'satellite': 'Satellite Monitor',
        'wifi': 'WiFi Scanner',
        'bluetooth': 'Bluetooth Scanner',
        'listening': 'Listening Post'
    };
    document.getElementById('outputTitle').textContent = titles[mode] || 'Signal Monitor';

    // Show/hide Device Intelligence for modes that use it
    const reconBtn = document.getElementById('reconBtn');
    const intelBtn = document.querySelector('[onclick="exportDeviceDB()"]');
    if (mode === 'satellite' || mode === 'aircraft' || mode === 'listening') {
        document.getElementById('reconPanel').style.display = 'none';
        if (reconBtn) reconBtn.style.display = 'none';
        if (intelBtn) intelBtn.style.display = 'none';
    } else {
        if (reconBtn) reconBtn.style.display = 'inline-block';
        if (intelBtn) intelBtn.style.display = 'inline-block';
        if (typeof reconEnabled !== 'undefined' && reconEnabled) {
            document.getElementById('reconPanel').style.display = 'block';
        }
    }

    // Show RTL-SDR device section for modes that use it
    document.getElementById('rtlDeviceSection').style.display =
        (mode === 'pager' || mode === 'sensor' || mode === 'aircraft' || mode === 'listening') ? 'block' : 'none';

    // Toggle mode-specific tool status displays
    document.getElementById('toolStatusPager').style.display = (mode === 'pager') ? 'grid' : 'none';
    document.getElementById('toolStatusSensor').style.display = (mode === 'sensor') ? 'grid' : 'none';
    document.getElementById('toolStatusAircraft').style.display = (mode === 'aircraft') ? 'grid' : 'none';

    // Hide waterfall and output console for modes with their own visualizations
    document.querySelector('.waterfall-container').style.display =
        (mode === 'satellite' || mode === 'listening' || mode === 'aircraft' || mode === 'wifi' || mode === 'bluetooth') ? 'none' : 'block';
    document.getElementById('output').style.display =
        (mode === 'satellite' || mode === 'aircraft' || mode === 'wifi' || mode === 'bluetooth') ? 'none' : 'block';
    document.querySelector('.status-bar').style.display = (mode === 'satellite') ? 'none' : 'flex';

    // Load interfaces and initialize visualizations when switching modes
    if (mode === 'wifi') {
        if (typeof refreshWifiInterfaces === 'function') refreshWifiInterfaces();
        if (typeof initRadar === 'function') initRadar();
        if (typeof initWatchList === 'function') initWatchList();
    } else if (mode === 'bluetooth') {
        if (typeof refreshBtInterfaces === 'function') refreshBtInterfaces();
        if (typeof initBtRadar === 'function') initBtRadar();
    } else if (mode === 'aircraft') {
        if (typeof checkAdsbTools === 'function') checkAdsbTools();
        if (typeof initAircraftRadar === 'function') initAircraftRadar();
    } else if (mode === 'satellite') {
        if (typeof initPolarPlot === 'function') initPolarPlot();
        if (typeof initSatelliteList === 'function') initSatelliteList();
    } else if (mode === 'listening') {
        if (typeof checkScannerTools === 'function') checkScannerTools();
        if (typeof checkAudioTools === 'function') checkAudioTools();
        if (typeof populateScannerDeviceSelect === 'function') populateScannerDeviceSelect();
        if (typeof populateAudioDeviceSelect === 'function') populateAudioDeviceSelect();
    }
}

// ============== SECTION COLLAPSE ==============

function toggleSection(el) {
    el.closest('.section').classList.toggle('collapsed');
}

// ============== THEME MANAGEMENT ==============

function toggleTheme() {
    const html = document.documentElement;
    const currentTheme = html.getAttribute('data-theme');
    const newTheme = currentTheme === 'light' ? 'dark' : 'light';
    html.setAttribute('data-theme', newTheme);
    localStorage.setItem('theme', newTheme);

    // Update button text
    const btn = document.getElementById('themeToggle');
    if (btn) {
        btn.textContent = newTheme === 'light' ? '🌙' : '☀️';
    }
}

function loadTheme() {
    const savedTheme = localStorage.getItem('theme') || 'dark';
    document.documentElement.setAttribute('data-theme', savedTheme);
    const btn = document.getElementById('themeToggle');
    if (btn) {
        btn.textContent = savedTheme === 'light' ? '🌙' : '☀️';
    }
}

// ============== AUTO-SCROLL ==============

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

// ============== SDR DEVICE MANAGEMENT ==============

function getSelectedDevice() {
    return document.getElementById('deviceSelect').value;
}

function getSelectedSDRType() {
    return document.getElementById('sdrTypeSelect').value;
}

function reserveDevice(deviceIndex, modeId) {
    sdrDeviceUsage[modeId] = deviceIndex;
}

function releaseDevice(modeId) {
    delete sdrDeviceUsage[modeId];
}

function checkDeviceAvailability(requestingMode) {
    const selectedDevice = parseInt(getSelectedDevice());
    for (const [mode, device] of Object.entries(sdrDeviceUsage)) {
        if (mode !== requestingMode && device === selectedDevice) {
            alert(`Device ${selectedDevice} is currently in use by ${mode} mode. Please select a different device or stop the other scan first.`);
            return false;
        }
    }
    return true;
}

// ============== BIAS-T SETTINGS ==============

function saveBiasTSetting() {
    const enabled = document.getElementById('biasT')?.checked || false;
    localStorage.setItem('biasTEnabled', enabled);
}

function getBiasTEnabled() {
    return document.getElementById('biasT')?.checked || false;
}

function loadBiasTSetting() {
    const saved = localStorage.getItem('biasTEnabled');
    if (saved === 'true') {
        const checkbox = document.getElementById('biasT');
        if (checkbox) checkbox.checked = true;
    }
}

// ============== REMOTE SDR ==============

function toggleRemoteSDR() {
    const useRemote = document.getElementById('useRemoteSDR').checked;
    const configDiv = document.getElementById('remoteSDRConfig');
    const localControls = document.querySelectorAll('#sdrTypeSelect, #deviceSelect');

    if (useRemote) {
        configDiv.style.display = 'block';
        localControls.forEach(el => el.disabled = true);
    } else {
        configDiv.style.display = 'none';
        localControls.forEach(el => el.disabled = false);
    }
}

function getRemoteSDRConfig() {
    const useRemote = document.getElementById('useRemoteSDR')?.checked;
    if (!useRemote) return null;

    const host = document.getElementById('rtlTcpHost')?.value || 'localhost';
    const port = parseInt(document.getElementById('rtlTcpPort')?.value || '1234');

    if (!host || isNaN(port)) {
        alert('Please enter valid rtl_tcp host and port');
        return false;
    }

    return { host, port };
}

// ============== OUTPUT DISPLAY ==============

function showInfo(text) {
    const output = document.getElementById('output');
    if (!output) return;

    const placeholder = output.querySelector('.placeholder');
    if (placeholder) placeholder.remove();

    const infoEl = document.createElement('div');
    infoEl.className = 'info-msg';
    infoEl.style.cssText = 'padding: 12px 15px; margin-bottom: 8px; background: #0a0a0a; border: 1px solid #1a1a1a; border-left: 2px solid #00d4ff; font-family: "JetBrains Mono", monospace; font-size: 11px; color: #888; word-break: break-all;';
    infoEl.textContent = text;
    output.insertBefore(infoEl, output.firstChild);
}

function showError(text) {
    const output = document.getElementById('output');
    if (!output) return;

    const placeholder = output.querySelector('.placeholder');
    if (placeholder) placeholder.remove();

    const errorEl = document.createElement('div');
    errorEl.className = 'error-msg';
    errorEl.style.cssText = 'padding: 12px 15px; margin-bottom: 8px; background: #1a0a0a; border: 1px solid #2a1a1a; border-left: 2px solid #ff3366; font-family: "JetBrains Mono", monospace; font-size: 11px; color: #ff6688; word-break: break-all;';
    errorEl.textContent = '⚠ ' + text;
    output.insertBefore(errorEl, output.firstChild);
}

// ============== OBSERVER LOCATION ==============

function saveObserverLocation() {
    const lat = parseFloat(document.getElementById('adsbObsLat')?.value || document.getElementById('obsLat')?.value);
    const lon = parseFloat(document.getElementById('adsbObsLon')?.value || document.getElementById('obsLon')?.value);

    if (!isNaN(lat) && !isNaN(lon)) {
        observerLocation = { lat, lon };
        localStorage.setItem('observerLocation', JSON.stringify(observerLocation));

        // Sync both input sets
        const adsbLat = document.getElementById('adsbObsLat');
        const adsbLon = document.getElementById('adsbObsLon');
        const satLat = document.getElementById('obsLat');
        const satLon = document.getElementById('obsLon');

        if (adsbLat) adsbLat.value = lat.toFixed(4);
        if (adsbLon) adsbLon.value = lon.toFixed(4);
        if (satLat) satLat.value = lat.toFixed(4);
        if (satLon) satLon.value = lon.toFixed(4);
    }
}

function useGeolocation() {
    if ('geolocation' in navigator) {
        navigator.geolocation.getCurrentPosition(
            (position) => {
                const lat = position.coords.latitude;
                const lon = position.coords.longitude;

                observerLocation = { lat, lon };
                localStorage.setItem('observerLocation', JSON.stringify(observerLocation));

                // Update all input fields
                const adsbLat = document.getElementById('adsbObsLat');
                const adsbLon = document.getElementById('adsbObsLon');
                const satLat = document.getElementById('obsLat');
                const satLon = document.getElementById('obsLon');

                if (adsbLat) adsbLat.value = lat.toFixed(4);
                if (adsbLon) adsbLon.value = lon.toFixed(4);
                if (satLat) satLat.value = lat.toFixed(4);
                if (satLon) satLon.value = lon.toFixed(4);

                showInfo(`Location set to ${lat.toFixed(4)}, ${lon.toFixed(4)}`);
            },
            (error) => {
                showError('Geolocation failed: ' + error.message);
            }
        );
    } else {
        showError('Geolocation not supported by browser');
    }
}

// ============== EXPORT FUNCTIONS ==============

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
    downloadFile(csv.join('\n'), 'intercept_messages.csv', 'text/csv');
}

function exportJSON() {
    if (allMessages.length === 0) {
        alert('No messages to export');
        return;
    }
    downloadFile(JSON.stringify(allMessages, null, 2), 'intercept_messages.json', 'application/json');
}

// ============== INITIALIZATION ==============

function initApp() {
    // Check disclaimer
    checkDisclaimer();

    // Load theme
    loadTheme();

    // Start clock
    updateHeaderClock();
    setInterval(updateHeaderClock, 1000);

    // Start stats sync
    setInterval(syncHeaderStats, 500);

    // Load bias-T setting
    loadBiasTSetting();

    // Initialize observer location inputs
    const adsbLatInput = document.getElementById('adsbObsLat');
    const adsbLonInput = document.getElementById('adsbObsLon');
    const obsLatInput = document.getElementById('obsLat');
    const obsLonInput = document.getElementById('obsLon');
    if (adsbLatInput) adsbLatInput.value = observerLocation.lat.toFixed(4);
    if (adsbLonInput) adsbLonInput.value = observerLocation.lon.toFixed(4);
    if (obsLatInput) obsLatInput.value = observerLocation.lat.toFixed(4);
    if (obsLonInput) obsLonInput.value = observerLocation.lon.toFixed(4);

    // Update UI state
    updateAutoScrollButton();

    // Make sections collapsible
    document.querySelectorAll('.section h3').forEach(h3 => {
        h3.addEventListener('click', function() {
            this.parentElement.classList.toggle('collapsed');
        });
    });

    // Collapse all sections by default (except SDR Device which is first)
    document.querySelectorAll('.section').forEach((section, index) => {
        if (index > 0) {
            section.classList.add('collapsed');
        }
    });
}

// Run initialization when DOM is ready
document.addEventListener('DOMContentLoaded', initApp);
