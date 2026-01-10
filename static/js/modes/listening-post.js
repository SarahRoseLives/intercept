/**
 * Intercept - Listening Post Mode
 * Frequency scanner and manual audio receiver
 */

// ============== STATE ==============

let isScannerRunning = false;
let isScannerPaused = false;
let scannerEventSource = null;
let scannerSignalCount = 0;
let scannerLogEntries = [];
let scannerFreqsScanned = 0;
let scannerCycles = 0;
let scannerStartFreq = 118;
let scannerEndFreq = 137;
let scannerSignalActive = false;

// Audio state
let isAudioPlaying = false;
let audioToolsAvailable = { rtl_fm: false, ffmpeg: false };
let audioReconnectAttempts = 0;
const MAX_AUDIO_RECONNECT = 3;

// WebSocket audio state
let audioWebSocket = null;
let audioQueue = [];
let isWebSocketAudio = false;

// Visualizer state
let visualizerContext = null;
let visualizerAnalyser = null;
let visualizerSource = null;
let visualizerAnimationId = null;
let peakLevel = 0;
let peakDecay = 0.95;

// Track recent signal hits to prevent duplicates
let recentSignalHits = new Map();

// Direct listen state
let isDirectListening = false;
let currentModulation = 'am';

// ============== PRESETS ==============

const scannerPresets = {
    fm: { start: 88, end: 108, step: 200, mod: 'wfm' },
    air: { start: 118, end: 137, step: 25, mod: 'am' },
    marine: { start: 156, end: 163, step: 25, mod: 'fm' },
    amateur2m: { start: 144, end: 148, step: 12.5, mod: 'fm' },
    pager: { start: 152, end: 160, step: 25, mod: 'fm' },
    amateur70cm: { start: 420, end: 450, step: 25, mod: 'fm' }
};

const audioPresets = {
    fm: { freq: 98.1, mod: 'wfm' },
    airband: { freq: 121.5, mod: 'am' },     // Emergency/guard frequency
    marine: { freq: 156.8, mod: 'fm' },       // Channel 16 - distress
    amateur2m: { freq: 146.52, mod: 'fm' },   // 2m calling frequency
    amateur70cm: { freq: 446.0, mod: 'fm' }
};

// ============== SCANNER TOOLS CHECK ==============

function checkScannerTools() {
    fetch('/listening/tools')
        .then(r => r.json())
        .then(data => {
            const warnings = [];
            if (!data.rtl_fm) {
                warnings.push('rtl_fm not found - install rtl-sdr tools');
            }
            if (!data.ffmpeg) {
                warnings.push('ffmpeg not found - install: brew install ffmpeg (macOS) or apt install ffmpeg (Linux)');
            }

            const warningDiv = document.getElementById('scannerToolsWarning');
            const warningText = document.getElementById('scannerToolsWarningText');
            if (warningDiv && warnings.length > 0) {
                warningText.innerHTML = warnings.join('<br>');
                warningDiv.style.display = 'block';
                document.getElementById('scannerStartBtn').disabled = true;
                document.getElementById('scannerStartBtn').style.opacity = '0.5';
            } else if (warningDiv) {
                warningDiv.style.display = 'none';
                document.getElementById('scannerStartBtn').disabled = false;
                document.getElementById('scannerStartBtn').style.opacity = '1';
            }
        })
        .catch(() => {});
}

// ============== SCANNER HELPERS ==============

/**
 * Get the currently selected device from the global SDR selector
 */
function getSelectedDevice() {
    const select = document.getElementById('deviceSelect');
    return parseInt(select?.value || '0');
}

/**
 * Get the currently selected SDR type from the global selector
 */
function getSelectedSDRTypeForScanner() {
    const select = document.getElementById('sdrTypeSelect');
    return select?.value || 'rtlsdr';
}

// ============== SCANNER PRESETS ==============

function applyScannerPreset() {
    const preset = document.getElementById('scannerPreset').value;
    if (preset !== 'custom' && scannerPresets[preset]) {
        const p = scannerPresets[preset];
        document.getElementById('scannerStartFreq').value = p.start;
        document.getElementById('scannerEndFreq').value = p.end;
        document.getElementById('scannerStep').value = p.step;
        document.getElementById('scannerModulation').value = p.mod;
    }
}

// ============== SCANNER CONTROLS ==============

function toggleScanner() {
    if (isScannerRunning) {
        stopScanner();
    } else {
        startScanner();
    }
}

function startScanner() {
    // Use unified radio controls - read all current UI values
    const startFreq = parseFloat(document.getElementById('radioScanStart')?.value || 118);
    const endFreq = parseFloat(document.getElementById('radioScanEnd')?.value || 137);
    const stepSelect = document.getElementById('radioScanStep');
    const step = stepSelect ? parseFloat(stepSelect.value) : 25;
    const modulation = currentModulation || 'am';
    const squelch = parseInt(document.getElementById('radioSquelchValue')?.textContent) || 30;
    const gain = parseInt(document.getElementById('radioGainValue')?.textContent) || 40;
    const dwellSelect = document.getElementById('radioScanDwell');
    const dwell = dwellSelect ? parseInt(dwellSelect.value) : 10;
    const device = getSelectedDevice();

    if (startFreq >= endFreq) {
        if (typeof showNotification === 'function') {
            showNotification('Scanner Error', 'End frequency must be greater than start');
        }
        return;
    }

    // Check if device is available
    if (typeof checkDeviceAvailability === 'function' && !checkDeviceAvailability('scanner')) {
        return;
    }

    // Store scanner range for progress calculation
    scannerStartFreq = startFreq;
    scannerEndFreq = endFreq;
    scannerFreqsScanned = 0;
    scannerCycles = 0;

    // Update sidebar display
    updateScannerDisplay('STARTING...', 'var(--accent-orange)');

    // Show progress bars
    const progressEl = document.getElementById('scannerProgress');
    if (progressEl) {
        progressEl.style.display = 'block';
        document.getElementById('scannerRangeStart').textContent = startFreq.toFixed(1);
        document.getElementById('scannerRangeEnd').textContent = endFreq.toFixed(1);
    }

    const mainProgress = document.getElementById('mainScannerProgress');
    if (mainProgress) {
        mainProgress.style.display = 'block';
        document.getElementById('mainRangeStart').textContent = startFreq.toFixed(1) + ' MHz';
        document.getElementById('mainRangeEnd').textContent = endFreq.toFixed(1) + ' MHz';
    }

    fetch('/listening/scanner/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            start_freq: startFreq,
            end_freq: endFreq,
            step: step,
            modulation: modulation,
            squelch: squelch,
            gain: gain,
            dwell_time: dwell,
            device: device,
            bias_t: typeof getBiasTEnabled === 'function' ? getBiasTEnabled() : false
        })
    })
    .then(r => r.json())
    .then(data => {
        if (data.status === 'started') {
            if (typeof reserveDevice === 'function') reserveDevice(device, 'scanner');
            isScannerRunning = true;
            isScannerPaused = false;
            scannerSignalActive = false;

            // Update controls (with null checks)
            const startBtn = document.getElementById('scannerStartBtn');
            if (startBtn) {
                startBtn.textContent = 'Stop Scanner';
                startBtn.classList.add('active');
            }
            const pauseBtn = document.getElementById('scannerPauseBtn');
            if (pauseBtn) pauseBtn.disabled = false;

            // Update radio scan button to show STOP
            const radioScanBtn = document.getElementById('radioScanBtn');
            if (radioScanBtn) {
                radioScanBtn.innerHTML = '⏹ STOP';
                radioScanBtn.style.background = 'var(--accent-red)';
                radioScanBtn.style.borderColor = 'var(--accent-red)';
            }

            updateScannerDisplay('SCANNING', 'var(--accent-cyan)');
            const statusText = document.getElementById('scannerStatusText');
            if (statusText) statusText.textContent = 'Scanning...';

            // Show level meter
            const levelMeter = document.getElementById('scannerLevelMeter');
            if (levelMeter) levelMeter.style.display = 'block';

            connectScannerStream();
            addScannerLogEntry('Scanner started', `Range: ${startFreq}-${endFreq} MHz, Step: ${step} kHz`);
            if (typeof showNotification === 'function') {
                showNotification('Scanner Started', `Scanning ${startFreq} - ${endFreq} MHz`);
            }
        } else {
            updateScannerDisplay('ERROR', 'var(--accent-red)');
            if (typeof showNotification === 'function') {
                showNotification('Scanner Error', data.message || 'Failed to start');
            }
        }
    })
    .catch(err => {
        const statusText = document.getElementById('scannerStatusText');
        if (statusText) statusText.textContent = 'ERROR';
        updateScannerDisplay('ERROR', 'var(--accent-red)');
        if (typeof showNotification === 'function') {
            showNotification('Scanner Error', err.message);
        }
    });
}

function stopScanner() {
    fetch('/listening/scanner/stop', { method: 'POST' })
        .then(() => {
            if (typeof releaseDevice === 'function') releaseDevice('scanner');
            isScannerRunning = false;
            isScannerPaused = false;
            scannerSignalActive = false;

            // Update sidebar (with null checks)
            const startBtn = document.getElementById('scannerStartBtn');
            if (startBtn) {
                startBtn.textContent = 'Start Scanner';
                startBtn.classList.remove('active');
            }
            const pauseBtn = document.getElementById('scannerPauseBtn');
            if (pauseBtn) {
                pauseBtn.disabled = true;
                pauseBtn.textContent = '⏸ Pause';
            }

            // Update radio scan button
            const radioScanBtn = document.getElementById('radioScanBtn');
            if (radioScanBtn) {
                radioScanBtn.innerHTML = '📡 SCAN';
                radioScanBtn.style.background = '';
                radioScanBtn.style.borderColor = '';
            }

            updateScannerDisplay('STOPPED', 'var(--text-muted)');
            const currentFreq = document.getElementById('scannerCurrentFreq');
            if (currentFreq) currentFreq.textContent = '---.--- MHz';
            const modLabel = document.getElementById('scannerModLabel');
            if (modLabel) modLabel.textContent = '--';

            const progressEl = document.getElementById('scannerProgress');
            if (progressEl) progressEl.style.display = 'none';

            const signalPanel = document.getElementById('scannerSignalPanel');
            if (signalPanel) signalPanel.style.display = 'none';

            const levelMeter = document.getElementById('scannerLevelMeter');
            if (levelMeter) levelMeter.style.display = 'none';

            const statusText = document.getElementById('scannerStatusText');
            if (statusText) statusText.textContent = 'Ready';

            // Update main display
            const mainModeLabel = document.getElementById('mainScannerModeLabel');
            if (mainModeLabel) {
                mainModeLabel.textContent = 'SCANNER STOPPED';
                document.getElementById('mainScannerFreq').textContent = '---.---';
                document.getElementById('mainScannerFreq').style.color = 'var(--text-muted)';
                document.getElementById('mainScannerMod').textContent = '--';
            }

            const mainAnim = document.getElementById('mainScannerAnimation');
            if (mainAnim) mainAnim.style.display = 'none';

            const mainProgress = document.getElementById('mainScannerProgress');
            if (mainProgress) mainProgress.style.display = 'none';

            const mainSignalAlert = document.getElementById('mainSignalAlert');
            if (mainSignalAlert) mainSignalAlert.style.display = 'none';

            // Stop scanner audio
            const scannerAudio = document.getElementById('scannerAudioPlayer');
            if (scannerAudio) {
                scannerAudio.pause();
                scannerAudio.src = '';
            }

            if (scannerEventSource) {
                scannerEventSource.close();
                scannerEventSource = null;
            }
            addScannerLogEntry('Scanner stopped', '');
        })
        .catch(() => {});
}

function pauseScanner() {
    const endpoint = isScannerPaused ? '/listening/scanner/resume' : '/listening/scanner/pause';
    fetch(endpoint, { method: 'POST' })
        .then(r => r.json())
        .then(data => {
            isScannerPaused = !isScannerPaused;
            const pauseBtn = document.getElementById('scannerPauseBtn');
            if (pauseBtn) pauseBtn.textContent = isScannerPaused ? '▶ Resume' : '⏸ Pause';
            const statusText = document.getElementById('scannerStatusText');
            if (statusText) {
                statusText.textContent = isScannerPaused ? 'PAUSED' : 'SCANNING';
                statusText.style.color = isScannerPaused ? 'var(--accent-orange)' : 'var(--accent-green)';
            }

            const activityStatus = document.getElementById('scannerActivityStatus');
            if (activityStatus) {
                activityStatus.textContent = isScannerPaused ? 'PAUSED' : 'SCANNING';
                activityStatus.style.color = isScannerPaused ? 'var(--accent-orange)' : 'var(--accent-green)';
            }

            // Update main display
            const mainModeLabel = document.getElementById('mainScannerModeLabel');
            if (mainModeLabel) {
                mainModeLabel.textContent = isScannerPaused ? 'PAUSED' : 'SCANNING';
            }

            addScannerLogEntry(isScannerPaused ? 'Scanner paused' : 'Scanner resumed', '');
        })
        .catch(() => {});
}

function skipSignal() {
    if (!isScannerRunning) {
        if (typeof showNotification === 'function') {
            showNotification('Scanner', 'Scanner is not running');
        }
        return;
    }

    fetch('/listening/scanner/skip', { method: 'POST' })
        .then(r => r.json())
        .then(data => {
            if (data.status === 'skipped' && typeof showNotification === 'function') {
                showNotification('Signal Skipped', `Continuing scan from ${data.frequency.toFixed(3)} MHz`);
            }
        })
        .catch(err => {
            if (typeof showNotification === 'function') {
                showNotification('Skip Error', err.message);
            }
        });
}

// ============== SCANNER STREAM ==============

function connectScannerStream() {
    if (scannerEventSource) {
        scannerEventSource.close();
    }

    scannerEventSource = new EventSource('/listening/scanner/stream');

    scannerEventSource.onmessage = function(e) {
        try {
            const data = JSON.parse(e.data);
            handleScannerEvent(data);
        } catch (err) {
            console.warn('Scanner parse error:', err);
        }
    };

    scannerEventSource.onerror = function() {
        if (isScannerRunning) {
            setTimeout(connectScannerStream, 2000);
        }
    };
}

function handleScannerEvent(data) {
    switch (data.type) {
        case 'freq_change':
        case 'scan_update':
            handleFrequencyUpdate(data);
            break;
        case 'signal_found':
            handleSignalFound(data);
            break;
        case 'signal_lost':
        case 'signal_skipped':
            handleSignalLost(data);
            break;
        case 'log':
            if (data.entry && data.entry.type === 'scan_cycle') {
                scannerCycles++;
                const cyclesEl = document.getElementById('mainScanCycles');
                if (cyclesEl) cyclesEl.textContent = scannerCycles;
            }
            break;
        case 'stopped':
            stopScanner();
            break;
    }
}

function handleFrequencyUpdate(data) {
    const freqStr = data.frequency.toFixed(3);

    const currentFreq = document.getElementById('scannerCurrentFreq');
    if (currentFreq) currentFreq.textContent = freqStr + ' MHz';

    const mainFreq = document.getElementById('mainScannerFreq');
    if (mainFreq) mainFreq.textContent = freqStr;

    // Update progress bar
    const progress = ((data.frequency - scannerStartFreq) / (scannerEndFreq - scannerStartFreq)) * 100;
    const progressBar = document.getElementById('scannerProgressBar');
    if (progressBar) progressBar.style.width = Math.max(0, Math.min(100, progress)) + '%';

    const mainProgressBar = document.getElementById('mainProgressBar');
    if (mainProgressBar) mainProgressBar.style.width = Math.max(0, Math.min(100, progress)) + '%';

    scannerFreqsScanned++;
    const freqsEl = document.getElementById('mainFreqsScanned');
    if (freqsEl) freqsEl.textContent = scannerFreqsScanned;

    // Update level meter if present
    if (data.level !== undefined) {
        const levelPercent = Math.min(100, (data.level / 5000) * 100);
        const levelBar = document.getElementById('scannerLevelBar');
        if (levelBar) {
            levelBar.style.width = levelPercent + '%';
            if (data.detected) {
                levelBar.style.background = 'var(--accent-green)';
            } else if (data.level > (data.threshold || 0) * 0.7) {
                levelBar.style.background = 'var(--accent-orange)';
            } else {
                levelBar.style.background = 'var(--accent-cyan)';
            }
        }
        const levelValue = document.getElementById('scannerLevelValue');
        if (levelValue) levelValue.textContent = data.level;
    }

    const statusText = document.getElementById('scannerStatusText');
    if (statusText) statusText.textContent = `${freqStr} MHz${data.level !== undefined ? ` (level: ${data.level})` : ''}`;
}

function handleSignalFound(data) {
    scannerSignalCount++;
    scannerSignalActive = true;
    const freqStr = data.frequency.toFixed(3);

    const signalCount = document.getElementById('scannerSignalCount');
    if (signalCount) signalCount.textContent = scannerSignalCount;
    const mainSignalCount = document.getElementById('mainSignalCount');
    if (mainSignalCount) mainSignalCount.textContent = scannerSignalCount;

    // Update sidebar
    updateScannerDisplay('SIGNAL FOUND', 'var(--accent-green)');
    const signalPanel = document.getElementById('scannerSignalPanel');
    if (signalPanel) signalPanel.style.display = 'block';
    const statusText = document.getElementById('scannerStatusText');
    if (statusText) statusText.textContent = 'Listening to signal...';

    // Update main display
    const mainModeLabel = document.getElementById('mainScannerModeLabel');
    if (mainModeLabel) mainModeLabel.textContent = 'SIGNAL DETECTED';

    const mainFreq = document.getElementById('mainScannerFreq');
    if (mainFreq) mainFreq.style.color = 'var(--accent-green)';

    const mainAnim = document.getElementById('mainScannerAnimation');
    if (mainAnim) mainAnim.style.display = 'none';

    const mainSignalAlert = document.getElementById('mainSignalAlert');
    if (mainSignalAlert) mainSignalAlert.style.display = 'block';

    // Start audio playback for the detected signal
    if (data.audio_streaming) {
        const scannerAudio = document.getElementById('scannerAudioPlayer');
        if (scannerAudio) {
            // Pass the signal frequency and modulation to getStreamUrl
            const streamUrl = getStreamUrl(data.frequency, data.modulation);
            console.log('[SCANNER] Starting audio for signal:', data.frequency, 'MHz');
            scannerAudio.src = streamUrl;
            scannerAudio.play().catch(e => console.warn('[SCANNER] Audio autoplay blocked:', e));
        }
    }

    // Add to sidebar recent signals
    if (typeof addSidebarRecentSignal === 'function') {
        addSidebarRecentSignal(data.frequency, data.modulation);
    }

    addScannerLogEntry('SIGNAL FOUND', `${freqStr} MHz (${data.modulation.toUpperCase()})`, 'signal');
    addSignalHit(data);

    if (typeof showNotification === 'function') {
        showNotification('Signal Found!', `${freqStr} MHz - Audio streaming`);
    }
}

function handleSignalLost(data) {
    scannerSignalActive = false;

    // Update sidebar
    updateScannerDisplay('SCANNING', 'var(--accent-cyan)');
    const signalPanel = document.getElementById('scannerSignalPanel');
    if (signalPanel) signalPanel.style.display = 'none';
    const statusText = document.getElementById('scannerStatusText');
    if (statusText) statusText.textContent = 'Scanning...';

    // Update main display
    const mainModeLabel = document.getElementById('mainScannerModeLabel');
    if (mainModeLabel) mainModeLabel.textContent = 'SCANNING';

    const mainFreq = document.getElementById('mainScannerFreq');
    if (mainFreq) mainFreq.style.color = 'var(--accent-cyan)';

    const mainAnim = document.getElementById('mainScannerAnimation');
    if (mainAnim) mainAnim.style.display = 'block';

    const mainSignalAlert = document.getElementById('mainSignalAlert');
    if (mainSignalAlert) mainSignalAlert.style.display = 'none';

    // Stop audio
    const scannerAudio = document.getElementById('scannerAudioPlayer');
    if (scannerAudio) {
        scannerAudio.pause();
        scannerAudio.src = '';
    }

    const logType = data.type === 'signal_skipped' ? 'info' : 'info';
    const logTitle = data.type === 'signal_skipped' ? 'Signal skipped' : 'Signal lost';
    addScannerLogEntry(logTitle, `${data.frequency.toFixed(3)} MHz`, logType);
}

function updateScannerDisplay(mode, color) {
    const modeLabel = document.getElementById('scannerModeLabel');
    if (modeLabel) {
        modeLabel.textContent = mode;
        modeLabel.style.color = color;
    }

    const currentFreq = document.getElementById('scannerCurrentFreq');
    if (currentFreq) currentFreq.style.color = color;

    const mainModeLabel = document.getElementById('mainScannerModeLabel');
    if (mainModeLabel) mainModeLabel.textContent = mode;

    const mainFreq = document.getElementById('mainScannerFreq');
    if (mainFreq) mainFreq.style.color = color;
}

// ============== SCANNER LOG ==============

function addScannerLogEntry(title, detail, type = 'info') {
    const now = new Date();
    const timestamp = now.toLocaleTimeString();
    const entry = { timestamp, title, detail, type };
    scannerLogEntries.unshift(entry);

    if (scannerLogEntries.length > 100) {
        scannerLogEntries.pop();
    }

    // Color based on type
    const getTypeColor = (t) => {
        switch(t) {
            case 'signal': return 'var(--accent-green)';
            case 'error': return 'var(--accent-red)';
            default: return 'var(--text-secondary)';
        }
    };

    // Update sidebar log
    const sidebarLog = document.getElementById('scannerLog');
    if (sidebarLog) {
        sidebarLog.innerHTML = scannerLogEntries.slice(0, 20).map(e =>
            `<div style="margin-bottom: 4px; color: ${getTypeColor(e.type)};">
                <span style="color: var(--text-muted);">[${e.timestamp}]</span>
                <strong>${e.title}</strong> ${e.detail}
            </div>`
        ).join('');
    }

    // Update main activity log
    const activityLog = document.getElementById('scannerActivityLog');
    if (activityLog) {
        const getBorderColor = (t) => {
            switch(t) {
                case 'signal': return 'var(--accent-green)';
                case 'error': return 'var(--accent-red)';
                default: return 'var(--border-color)';
            }
        };
        activityLog.innerHTML = scannerLogEntries.slice(0, 50).map(e =>
            `<div class="scanner-log-entry" style="margin-bottom: 6px; padding: 4px; border-left: 2px solid ${getBorderColor(e.type)};">
                <span style="color: var(--text-muted);">[${e.timestamp}]</span>
                <strong style="color: ${getTypeColor(e.type)};">${e.title}</strong>
                <span style="color: var(--text-secondary);">${e.detail}</span>
            </div>`
        ).join('');
    }
}

function addSignalHit(data) {
    const tbody = document.getElementById('scannerHitsBody');
    if (!tbody) return;

    const now = Date.now();
    const freqKey = data.frequency.toFixed(3);

    // Check for duplicate
    if (recentSignalHits.has(freqKey)) {
        const lastHit = recentSignalHits.get(freqKey);
        if (now - lastHit < 5000) return;
    }
    recentSignalHits.set(freqKey, now);

    // Clean up old entries
    for (const [freq, time] of recentSignalHits) {
        if (now - time > 30000) {
            recentSignalHits.delete(freq);
        }
    }

    const timestamp = new Date().toLocaleTimeString();

    if (tbody.innerHTML.includes('No signals detected')) {
        tbody.innerHTML = '';
    }

    const mod = data.modulation || 'fm';
    const row = document.createElement('tr');
    row.style.borderBottom = '1px solid var(--border-color)';
    row.innerHTML = `
        <td style="padding: 4px; color: var(--text-secondary); font-size: 9px;">${timestamp}</td>
        <td style="padding: 4px; color: var(--accent-green); font-weight: bold;">${data.frequency.toFixed(3)}</td>
        <td style="padding: 4px; color: var(--text-secondary);">${mod.toUpperCase()}</td>
        <td style="padding: 4px; text-align: center;">
            <button class="preset-btn" onclick="tuneToFrequency(${data.frequency}, '${mod}')" style="padding: 2px 6px; font-size: 9px; background: var(--accent-green); border: none; color: #000; cursor: pointer; border-radius: 3px;">Listen</button>
        </td>
    `;
    tbody.insertBefore(row, tbody.firstChild);

    while (tbody.children.length > 50) {
        tbody.removeChild(tbody.lastChild);
    }

    const hitCount = document.getElementById('scannerHitCount');
    if (hitCount) hitCount.textContent = `${tbody.children.length} signals found`;
}

function clearScannerLog() {
    scannerLogEntries = [];
    scannerSignalCount = 0;
    scannerFreqsScanned = 0;
    scannerCycles = 0;
    recentSignalHits.clear();

    const signalCount = document.getElementById('scannerSignalCount');
    if (signalCount) signalCount.textContent = '0';

    const mainSignalCount = document.getElementById('mainSignalCount');
    if (mainSignalCount) mainSignalCount.textContent = '0';

    const mainFreqsScanned = document.getElementById('mainFreqsScanned');
    if (mainFreqsScanned) mainFreqsScanned.textContent = '0';

    const mainScanCycles = document.getElementById('mainScanCycles');
    if (mainScanCycles) mainScanCycles.textContent = '0';

    const sidebarLog = document.getElementById('scannerLog');
    if (sidebarLog) sidebarLog.innerHTML = '<div style="color: var(--text-muted);">Scanner activity will appear here...</div>';

    const activityLog = document.getElementById('scannerActivityLog');
    if (activityLog) activityLog.innerHTML = '<div class="scanner-log-entry" style="color: var(--text-muted);">Waiting for scanner to start...</div>';

    const hitsBody = document.getElementById('scannerHitsBody');
    if (hitsBody) hitsBody.innerHTML = '<tr style="color: var(--text-muted);"><td colspan="4" style="padding: 15px; text-align: center; font-size: 10px;">No signals detected</td></tr>';

    const hitCount = document.getElementById('scannerHitCount');
    if (hitCount) hitCount.textContent = '0 signals found';
}

function exportScannerLog() {
    if (scannerLogEntries.length === 0) {
        if (typeof showNotification === 'function') {
            showNotification('Export', 'No log entries to export');
        }
        return;
    }

    const csv = 'Timestamp,Event,Details\n' + scannerLogEntries.map(e =>
        `"${e.timestamp}","${e.title}","${e.detail}"`
    ).join('\n');

    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `scanner_log_${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);

    if (typeof showNotification === 'function') {
        showNotification('Export', 'Log exported to CSV');
    }
}

// ============== AUDIO TOOLS CHECK ==============

function checkAudioTools() {
    fetch('/listening/tools')
        .then(r => r.json())
        .then(data => {
            audioToolsAvailable.rtl_fm = data.rtl_fm;
            audioToolsAvailable.ffmpeg = data.ffmpeg;

            // Only rtl_fm/rx_fm + ffmpeg are required for direct streaming
            const warnings = [];
            if (!data.rtl_fm && !data.rx_fm) {
                warnings.push('rtl_fm/rx_fm not found - install rtl-sdr or soapysdr-tools');
            }
            if (!data.ffmpeg) {
                warnings.push('ffmpeg not found - install: brew install ffmpeg (macOS) or apt install ffmpeg (Linux)');
            }

            const warningDiv = document.getElementById('audioToolsWarning');
            const warningText = document.getElementById('audioToolsWarningText');
            if (warningDiv) {
                if (warnings.length > 0) {
                    warningText.innerHTML = warnings.join('<br>');
                    warningDiv.style.display = 'block';
                    document.getElementById('audioStartBtn').disabled = true;
                    document.getElementById('audioStartBtn').style.opacity = '0.5';
                } else {
                    warningDiv.style.display = 'none';
                    document.getElementById('audioStartBtn').disabled = false;
                    document.getElementById('audioStartBtn').style.opacity = '1';
                }
            }
        })
        .catch(() => {});
}

// ============== AUDIO PRESETS ==============

function applyAudioPreset() {
    const preset = document.getElementById('audioPreset').value;
    const freqInput = document.getElementById('audioFrequency');
    const modSelect = document.getElementById('audioModulation');

    if (audioPresets[preset]) {
        freqInput.value = audioPresets[preset].freq;
        modSelect.value = audioPresets[preset].mod;
    }
}

// ============== AUDIO CONTROLS ==============

function toggleAudio() {
    if (isAudioPlaying) {
        stopAudio();
    } else {
        startAudio();
    }
}

function startAudio() {
    const frequency = parseFloat(document.getElementById('audioFrequency').value);
    const modulation = document.getElementById('audioModulation').value;
    const squelch = parseInt(document.getElementById('audioSquelch').value);
    const gain = parseInt(document.getElementById('audioGain').value);
    const device = getSelectedDevice();

    if (isNaN(frequency) || frequency <= 0) {
        if (typeof showNotification === 'function') {
            showNotification('Audio Error', 'Invalid frequency');
        }
        return;
    }

    // Check if device is in use
    if (typeof getDeviceInUseBy === 'function') {
        const usedBy = getDeviceInUseBy(device);
        if (usedBy && usedBy !== 'audio') {
            if (typeof showNotification === 'function') {
                showNotification('SDR In Use', `Device ${device} is being used by ${usedBy.toUpperCase()}.`);
            }
            return;
        }
    }

    document.getElementById('audioStatus').textContent = 'STARTING...';
    document.getElementById('audioStatus').style.color = 'var(--accent-orange)';

    // Use direct streaming - no Icecast needed
    if (typeof reserveDevice === 'function') reserveDevice(device, 'audio');
    isAudioPlaying = true;

    // Build direct stream URL with parameters
    const streamUrl = `/listening/audio/stream?freq=${frequency}&mod=${modulation}&squelch=${squelch}&gain=${gain}&t=${Date.now()}`;
    console.log('Connecting to direct stream:', streamUrl);

    // Start browser audio playback
    const audioPlayer = document.getElementById('audioPlayer');
    audioPlayer.src = streamUrl;
    audioPlayer.volume = document.getElementById('audioVolume').value / 100;

    initAudioVisualizer();

    audioPlayer.onplaying = () => {
        document.getElementById('audioStatus').textContent = 'STREAMING';
        document.getElementById('audioStatus').style.color = 'var(--accent-green)';
    };

    audioPlayer.onerror = (e) => {
        console.error('Audio player error:', e);
        document.getElementById('audioStatus').textContent = 'ERROR';
        document.getElementById('audioStatus').style.color = 'var(--accent-red)';
        if (typeof showNotification === 'function') {
            showNotification('Audio Error', 'Stream error - check SDR connection');
        }
    };

    audioPlayer.play().catch(e => {
        console.warn('Audio autoplay blocked:', e);
        if (typeof showNotification === 'function') {
            showNotification('Audio Ready', 'Click Play button again if audio does not start');
        }
    });

    document.getElementById('audioStartBtn').textContent = '⏹ Stop Audio';
    document.getElementById('audioStartBtn').classList.add('active');
    document.getElementById('audioTunedFreq').textContent = frequency.toFixed(2) + ' MHz (' + modulation.toUpperCase() + ')';
    document.getElementById('audioDeviceStatus').textContent = 'SDR ' + device;

    if (typeof showNotification === 'function') {
        showNotification('Audio Started', `Streaming ${frequency} MHz to browser`);
    }
}

async function stopAudio() {
    stopAudioVisualizer();

    const audioPlayer = document.getElementById('audioPlayer');
    if (audioPlayer) {
        audioPlayer.pause();
        audioPlayer.src = '';
    }

    try {
        await fetch('/listening/audio/stop', { method: 'POST' });
        if (typeof releaseDevice === 'function') releaseDevice('audio');
        isAudioPlaying = false;
        document.getElementById('audioStartBtn').textContent = '▶ Play Audio';
        document.getElementById('audioStartBtn').classList.remove('active');
        document.getElementById('audioStatus').textContent = 'STOPPED';
        document.getElementById('audioStatus').style.color = 'var(--text-muted)';
        document.getElementById('audioDeviceStatus').textContent = '--';
    } catch (e) {
        console.error('Error stopping audio:', e);
    }
}

function updateAudioVolume() {
    const audioPlayer = document.getElementById('audioPlayer');
    if (audioPlayer) {
        audioPlayer.volume = document.getElementById('audioVolume').value / 100;
    }
}

function audioFreqUp() {
    const input = document.getElementById('audioFrequency');
    const mod = document.getElementById('audioModulation').value;
    const step = (mod === 'wfm') ? 0.2 : 0.025;
    input.value = (parseFloat(input.value) + step).toFixed(2);
    if (isAudioPlaying) {
        tuneAudioFrequency(parseFloat(input.value));
    }
}

function audioFreqDown() {
    const input = document.getElementById('audioFrequency');
    const mod = document.getElementById('audioModulation').value;
    const step = (mod === 'wfm') ? 0.2 : 0.025;
    input.value = (parseFloat(input.value) - step).toFixed(2);
    if (isAudioPlaying) {
        tuneAudioFrequency(parseFloat(input.value));
    }
}

function tuneAudioFrequency(frequency) {
    fetch('/listening/audio/tune', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ frequency: frequency })
    })
    .then(r => r.json())
    .then(data => {
        if (data.status === 'tuned') {
            document.getElementById('audioTunedFreq').textContent = frequency.toFixed(2) + ' MHz';
        }
    })
    .catch(() => {
        stopAudio();
        setTimeout(startAudio, 300);
    });
}

async function tuneToFrequency(freq, mod) {
    try {
        // Stop scanner if running
        if (isScannerRunning) {
            stopScanner();
            await new Promise(resolve => setTimeout(resolve, 300));
        }

        // Update frequency input
        const freqInput = document.getElementById('radioScanStart');
        if (freqInput) {
            freqInput.value = freq.toFixed(1);
        }

        // Update modulation if provided
        if (mod) {
            setModulation(mod);
        }

        // Update tuning dial (silent to avoid duplicate events)
        const mainTuningDial = document.getElementById('mainTuningDial');
        if (mainTuningDial && mainTuningDial._dial) {
            mainTuningDial._dial.setValue(freq, true);
        }

        // Update frequency display
        const mainFreq = document.getElementById('mainScannerFreq');
        if (mainFreq) {
            mainFreq.textContent = freq.toFixed(3);
        }

        // Start listening immediately
        await startDirectListenImmediate();

        if (typeof showNotification === 'function') {
            showNotification('Tuned', `Now listening to ${freq.toFixed(3)} MHz (${(mod || currentModulation).toUpperCase()})`);
        }
    } catch (err) {
        console.error('Error tuning to frequency:', err);
        if (typeof showNotification === 'function') {
            showNotification('Tune Error', 'Failed to tune to frequency: ' + err.message);
        }
    }
}

// ============== AUDIO VISUALIZER ==============

function initAudioVisualizer() {
    const audioPlayer = document.getElementById('audioPlayer');
    if (!audioPlayer) return;

    if (!visualizerContext) {
        visualizerContext = new (window.AudioContext || window.webkitAudioContext)();
    }

    if (visualizerContext.state === 'suspended') {
        visualizerContext.resume();
    }

    if (!visualizerSource) {
        try {
            visualizerSource = visualizerContext.createMediaElementSource(audioPlayer);
            visualizerAnalyser = visualizerContext.createAnalyser();
            visualizerAnalyser.fftSize = 256;
            visualizerAnalyser.smoothingTimeConstant = 0.7;

            visualizerSource.connect(visualizerAnalyser);
            visualizerAnalyser.connect(visualizerContext.destination);
        } catch (e) {
            console.warn('Could not create audio source:', e);
            return;
        }
    }

    const container = document.getElementById('audioVisualizerContainer');
    if (container) container.style.display = 'block';

    drawAudioVisualizer();
}

function drawAudioVisualizer() {
    if (!visualizerAnalyser) return;

    const canvas = document.getElementById('audioSpectrumCanvas');
    if (!canvas) return;

    const ctx = canvas.getContext('2d');
    const bufferLength = visualizerAnalyser.frequencyBinCount;
    const dataArray = new Uint8Array(bufferLength);

    function draw() {
        visualizerAnimationId = requestAnimationFrame(draw);

        visualizerAnalyser.getByteFrequencyData(dataArray);

        let sum = 0;
        for (let i = 0; i < bufferLength; i++) {
            sum += dataArray[i];
        }
        const average = sum / bufferLength;
        const levelPercent = (average / 255) * 100;

        if (levelPercent > peakLevel) {
            peakLevel = levelPercent;
        } else {
            peakLevel *= peakDecay;
        }

        const meterFill = document.getElementById('audioSignalMeter');
        const meterPeak = document.getElementById('audioSignalPeak');
        const meterValue = document.getElementById('audioSignalValue');

        if (meterFill) meterFill.style.width = levelPercent + '%';
        if (meterPeak) meterPeak.style.left = Math.min(peakLevel, 100) + '%';

        const db = average > 0 ? Math.round(20 * Math.log10(average / 255)) : -60;
        if (meterValue) meterValue.textContent = db + ' dB';

        ctx.fillStyle = 'rgba(0, 0, 0, 0.3)';
        ctx.fillRect(0, 0, canvas.width, canvas.height);

        const barWidth = canvas.width / bufferLength * 2.5;
        let x = 0;

        for (let i = 0; i < bufferLength; i++) {
            const barHeight = (dataArray[i] / 255) * canvas.height;
            const hue = 200 - (i / bufferLength) * 60;
            const lightness = 40 + (dataArray[i] / 255) * 30;
            ctx.fillStyle = `hsl(${hue}, 80%, ${lightness}%)`;
            ctx.fillRect(x, canvas.height - barHeight, barWidth - 1, barHeight);
            x += barWidth;
        }

        ctx.fillStyle = 'rgba(255, 255, 255, 0.3)';
        ctx.font = '8px JetBrains Mono';
        ctx.fillText('0', 2, canvas.height - 2);
        ctx.fillText('4kHz', canvas.width / 4, canvas.height - 2);
        ctx.fillText('8kHz', canvas.width / 2, canvas.height - 2);
    }

    draw();
}

function stopAudioVisualizer() {
    if (visualizerAnimationId) {
        cancelAnimationFrame(visualizerAnimationId);
        visualizerAnimationId = null;
    }

    const meterFill = document.getElementById('audioSignalMeter');
    const meterPeak = document.getElementById('audioSignalPeak');
    const meterValue = document.getElementById('audioSignalValue');

    if (meterFill) meterFill.style.width = '0%';
    if (meterPeak) meterPeak.style.left = '0%';
    if (meterValue) meterValue.textContent = '-∞ dB';

    peakLevel = 0;

    const container = document.getElementById('audioVisualizerContainer');
    if (container) container.style.display = 'none';
}

// ============== RADIO KNOB CONTROLS ==============

/**
 * Update scanner config on the backend (for live updates while scanning)
 */
function updateScannerConfig(config) {
    if (!isScannerRunning) return;
    fetch('/listening/scanner/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(config)
    }).catch(() => {});
}

/**
 * Initialize radio knob controls and wire them to scanner parameters
 */
function initRadioKnobControls() {
    // Squelch knob
    const squelchKnob = document.getElementById('radioSquelchKnob');
    if (squelchKnob) {
        squelchKnob.addEventListener('knobchange', function(e) {
            const value = Math.round(e.detail.value);
            const valueDisplay = document.getElementById('radioSquelchValue');
            if (valueDisplay) valueDisplay.textContent = value;
            // Sync with scanner
            updateScannerConfig({ squelch: value });
            // Restart stream if direct listening (squelch requires restart)
            if (isDirectListening) {
                startDirectListen();
            }
        });
    }

    // Gain knob
    const gainKnob = document.getElementById('radioGainKnob');
    if (gainKnob) {
        gainKnob.addEventListener('knobchange', function(e) {
            const value = Math.round(e.detail.value);
            const valueDisplay = document.getElementById('radioGainValue');
            if (valueDisplay) valueDisplay.textContent = value;
            // Sync with scanner
            updateScannerConfig({ gain: value });
            // Restart stream if direct listening (gain requires restart)
            if (isDirectListening) {
                startDirectListen();
            }
        });
    }

    // Volume knob - controls scanner audio player volume
    const volumeKnob = document.getElementById('radioVolumeKnob');
    if (volumeKnob) {
        volumeKnob.addEventListener('knobchange', function(e) {
            const audioPlayer = document.getElementById('scannerAudioPlayer');
            if (audioPlayer) {
                audioPlayer.volume = e.detail.value / 100;
            }
            const manualPlayer = document.getElementById('audioPlayer');
            if (manualPlayer) {
                manualPlayer.volume = e.detail.value / 100;
            }
            // Update knob value display
            const valueDisplay = document.getElementById('radioVolumeValue');
            if (valueDisplay) valueDisplay.textContent = Math.round(e.detail.value);
        });
    }

    // Main Tuning dial - updates frequency display and inputs
    const mainTuningDial = document.getElementById('mainTuningDial');
    if (mainTuningDial) {
        mainTuningDial.addEventListener('knobchange', function(e) {
            const freq = e.detail.value;
            // Update main frequency display
            const mainFreq = document.getElementById('mainScannerFreq');
            if (mainFreq) {
                mainFreq.textContent = freq.toFixed(3);
            }
            // Update radio scan start input
            const startFreqInput = document.getElementById('radioScanStart');
            if (startFreqInput) {
                startFreqInput.value = freq.toFixed(1);
            }
            // Update sidebar frequency input
            const sidebarFreq = document.getElementById('audioFrequency');
            if (sidebarFreq) {
                sidebarFreq.value = freq.toFixed(3);
            }
            // If currently listening, retune to new frequency
            if (isDirectListening) {
                startDirectListen();
            }
        });
    }

    // Legacy tuning dial support
    const tuningDial = document.getElementById('tuningDial');
    if (tuningDial) {
        tuningDial.addEventListener('knobchange', function(e) {
            const mainFreq = document.getElementById('mainScannerFreq');
            if (mainFreq) mainFreq.textContent = e.detail.value.toFixed(3);
            const startFreqInput = document.getElementById('radioScanStart');
            if (startFreqInput) startFreqInput.value = e.detail.value.toFixed(1);
            // If currently listening, retune to new frequency
            if (isDirectListening) {
                startDirectListen();
            }
        });
    }

    // Sync radio scan range inputs with sidebar
    const radioScanStart = document.getElementById('radioScanStart');
    const radioScanEnd = document.getElementById('radioScanEnd');

    if (radioScanStart) {
        radioScanStart.addEventListener('change', function() {
            const sidebarStart = document.getElementById('scanStartFreq');
            if (sidebarStart) sidebarStart.value = this.value;
            // Restart stream if direct listening
            if (isDirectListening) {
                startDirectListen();
            }
        });
    }

    if (radioScanEnd) {
        radioScanEnd.addEventListener('change', function() {
            const sidebarEnd = document.getElementById('scanEndFreq');
            if (sidebarEnd) sidebarEnd.value = this.value;
        });
    }
}

/**
 * Set modulation mode (called from HTML onclick)
 */
function setModulation(mod) {
    // Update sidebar select
    const modSelect = document.getElementById('scanModulation');
    if (modSelect) modSelect.value = mod;

    // Update audio modulation select
    const audioMod = document.getElementById('audioModulation');
    if (audioMod) audioMod.value = mod;

    // Update button states in radio panel
    document.querySelectorAll('#modBtnBank .radio-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.mod === mod);
    });

    // Update main display badge
    const mainBadge = document.getElementById('mainScannerMod');
    if (mainBadge) mainBadge.textContent = mod.toUpperCase();
}

/**
 * Set band preset (called from HTML onclick)
 */
function setBand(band) {
    const preset = scannerPresets[band];
    if (!preset) return;

    // Update button states
    document.querySelectorAll('#bandBtnBank .radio-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.band === band);
    });

    // Update sidebar frequency inputs
    const sidebarStart = document.getElementById('scanStartFreq');
    const sidebarEnd = document.getElementById('scanEndFreq');
    if (sidebarStart) sidebarStart.value = preset.start;
    if (sidebarEnd) sidebarEnd.value = preset.end;

    // Update radio panel frequency inputs
    const radioStart = document.getElementById('radioScanStart');
    const radioEnd = document.getElementById('radioScanEnd');
    if (radioStart) radioStart.value = preset.start;
    if (radioEnd) radioEnd.value = preset.end;

    // Update tuning dial range and value (silent to avoid triggering restart)
    const tuningDial = document.getElementById('tuningDial');
    if (tuningDial && tuningDial._dial) {
        tuningDial._dial.min = preset.start;
        tuningDial._dial.max = preset.end;
        tuningDial._dial.setValue(preset.start, true);
    }

    // Update main frequency display
    const mainFreq = document.getElementById('mainScannerFreq');
    if (mainFreq) mainFreq.textContent = preset.start.toFixed(3);

    // Update modulation
    setModulation(preset.mod);

    // Update main range display if scanning
    const rangeStart = document.getElementById('mainRangeStart');
    const rangeEnd = document.getElementById('mainRangeEnd');
    if (rangeStart) rangeStart.textContent = preset.start;
    if (rangeEnd) rangeEnd.textContent = preset.end;

    // Store for scanner use
    scannerStartFreq = preset.start;
    scannerEndFreq = preset.end;
}

// ============== SYNTHESIZER VISUALIZATION ==============

let synthAnimationId = null;
let synthCanvas = null;
let synthCtx = null;
let synthBars = [];
const SYNTH_BAR_COUNT = 32;

function initSynthesizer() {
    synthCanvas = document.getElementById('synthesizerCanvas');
    if (!synthCanvas) return;

    // Set canvas size
    const rect = synthCanvas.parentElement.getBoundingClientRect();
    synthCanvas.width = rect.width - 20;
    synthCanvas.height = 60;

    synthCtx = synthCanvas.getContext('2d');

    // Initialize bar heights
    for (let i = 0; i < SYNTH_BAR_COUNT; i++) {
        synthBars[i] = { height: 0, targetHeight: 0, velocity: 0 };
    }

    drawSynthesizer();
}

function drawSynthesizer() {
    if (!synthCtx || !synthCanvas) return;

    const width = synthCanvas.width;
    const height = synthCanvas.height;
    const barWidth = (width / SYNTH_BAR_COUNT) - 2;

    // Clear canvas
    synthCtx.fillStyle = 'rgba(0, 0, 0, 0.3)';
    synthCtx.fillRect(0, 0, width, height);

    // Determine activity level based on state
    let activityLevel = 0;
    if (isScannerRunning && !isScannerPaused) {
        activityLevel = scannerSignalActive ? 0.9 : 0.4;
    } else if (isDirectListening) {
        activityLevel = 0.7;
    }

    // Update bar targets
    for (let i = 0; i < SYNTH_BAR_COUNT; i++) {
        if (activityLevel > 0) {
            // Create wave-like pattern with some randomness
            const wave = Math.sin((Date.now() / 200) + (i * 0.3)) * 0.3;
            const random = Math.random() * 0.4;
            const centerBoost = 1 - Math.abs((i - SYNTH_BAR_COUNT / 2) / (SYNTH_BAR_COUNT / 2)) * 0.5;
            synthBars[i].targetHeight = (wave + random + 0.3) * activityLevel * centerBoost * height;
        } else {
            // Idle state - minimal activity
            synthBars[i].targetHeight = (Math.sin((Date.now() / 500) + (i * 0.5)) * 0.1 + 0.1) * height * 0.3;
        }

        // Smooth animation
        const diff = synthBars[i].targetHeight - synthBars[i].height;
        synthBars[i].velocity += diff * 0.1;
        synthBars[i].velocity *= 0.8;
        synthBars[i].height += synthBars[i].velocity;
        synthBars[i].height = Math.max(2, Math.min(height - 4, synthBars[i].height));
    }

    // Draw bars
    for (let i = 0; i < SYNTH_BAR_COUNT; i++) {
        const x = i * (barWidth + 2) + 1;
        const barHeight = synthBars[i].height;
        const y = (height - barHeight) / 2;

        // Color gradient based on height and state
        let hue, saturation, lightness;
        if (scannerSignalActive) {
            hue = 120; // Green for signal
            saturation = 80;
            lightness = 40 + (barHeight / height) * 30;
        } else if (isScannerRunning || isDirectListening) {
            hue = 190 + (i / SYNTH_BAR_COUNT) * 30; // Cyan to blue
            saturation = 80;
            lightness = 35 + (barHeight / height) * 25;
        } else {
            hue = 200;
            saturation = 50;
            lightness = 25 + (barHeight / height) * 15;
        }

        const gradient = synthCtx.createLinearGradient(x, y, x, y + barHeight);
        gradient.addColorStop(0, `hsla(${hue}, ${saturation}%, ${lightness + 20}%, 0.9)`);
        gradient.addColorStop(0.5, `hsla(${hue}, ${saturation}%, ${lightness}%, 1)`);
        gradient.addColorStop(1, `hsla(${hue}, ${saturation}%, ${lightness + 20}%, 0.9)`);

        synthCtx.fillStyle = gradient;
        synthCtx.fillRect(x, y, barWidth, barHeight);

        // Add glow effect for active bars
        if (barHeight > height * 0.5 && activityLevel > 0.5) {
            synthCtx.shadowColor = `hsla(${hue}, ${saturation}%, 60%, 0.5)`;
            synthCtx.shadowBlur = 8;
            synthCtx.fillRect(x, y, barWidth, barHeight);
            synthCtx.shadowBlur = 0;
        }
    }

    // Draw center line
    synthCtx.strokeStyle = 'rgba(0, 212, 255, 0.2)';
    synthCtx.lineWidth = 1;
    synthCtx.beginPath();
    synthCtx.moveTo(0, height / 2);
    synthCtx.lineTo(width, height / 2);
    synthCtx.stroke();

    synthAnimationId = requestAnimationFrame(drawSynthesizer);
}

function stopSynthesizer() {
    if (synthAnimationId) {
        cancelAnimationFrame(synthAnimationId);
        synthAnimationId = null;
    }
}

// ============== INITIALIZATION ==============

/**
 * Get the audio stream URL with parameters
 * Streams directly from Flask - no Icecast needed
 */
function getStreamUrl(freq, mod) {
    const frequency = freq || parseFloat(document.getElementById('radioScanStart')?.value) || 118.0;
    const modulation = mod || currentModulation || 'am';
    const squelch = parseInt(document.getElementById('radioSquelchValue')?.textContent) || 30;
    const gain = parseInt(document.getElementById('radioGainValue')?.textContent) || 40;
    return `/listening/audio/stream?freq=${frequency}&mod=${modulation}&squelch=${squelch}&gain=${gain}&t=${Date.now()}`;
}

function initListeningPost() {
    checkScannerTools();
    checkAudioTools();

    // WebSocket audio disabled for now - using HTTP streaming
    // initWebSocketAudio();

    // Initialize synthesizer visualization
    initSynthesizer();

    // Initialize radio knobs if the component is available
    if (typeof initRadioKnobs === 'function') {
        initRadioKnobs();
    }

    // Connect radio knobs to scanner controls
    initRadioKnobControls();

    // Step dropdown - sync with scanner when changed
    const stepSelect = document.getElementById('radioScanStep');
    if (stepSelect) {
        stepSelect.addEventListener('change', function() {
            const step = parseFloat(this.value);
            console.log('[SCANNER] Step changed to:', step, 'kHz');
            updateScannerConfig({ step: step });
        });
    }

    // Dwell dropdown - sync with scanner when changed
    const dwellSelect = document.getElementById('radioScanDwell');
    if (dwellSelect) {
        dwellSelect.addEventListener('change', function() {
            const dwell = parseInt(this.value);
            console.log('[SCANNER] Dwell changed to:', dwell, 's');
            updateScannerConfig({ dwell_time: dwell });
        });
    }

    // Set up audio player error handling
    const audioPlayer = document.getElementById('audioPlayer');
    if (audioPlayer) {
        audioPlayer.addEventListener('error', function(e) {
            console.warn('Audio player error:', e);
            if (isAudioPlaying && audioReconnectAttempts < MAX_AUDIO_RECONNECT) {
                audioReconnectAttempts++;
                setTimeout(() => {
                    audioPlayer.src = getStreamUrl();
                    audioPlayer.play().catch(() => {});
                }, 500);
            }
        });

        audioPlayer.addEventListener('stalled', function() {
            if (isAudioPlaying) {
                audioPlayer.load();
                audioPlayer.play().catch(() => {});
            }
        });

        audioPlayer.addEventListener('playing', function() {
            audioReconnectAttempts = 0;
        });
    }
}

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', initListeningPost);

// ============== UNIFIED RADIO CONTROLS ==============

/**
 * Toggle direct listen mode (tune to start frequency and listen)
 */
function toggleDirectListen() {
    console.log('[LISTEN] toggleDirectListen called, isDirectListening:', isDirectListening);
    if (isDirectListening) {
        stopDirectListen();
    } else {
        // First press - start immediately, don't debounce
        startDirectListenImmediate();
    }
}

// Debounce for startDirectListen
let listenDebounceTimer = null;
// Flag to prevent overlapping restart attempts
let isRestarting = false;
// Flag indicating another restart is needed after current one finishes
let restartPending = false;
// Debounce for frequency tuning (user might be scrolling through)
// Needs to be long enough for SDR to fully release between restarts
const TUNE_DEBOUNCE_MS = 600;

/**
 * Start direct listening - debounced for frequency changes
 */
function startDirectListen() {
    if (listenDebounceTimer) {
        clearTimeout(listenDebounceTimer);
    }
    listenDebounceTimer = setTimeout(async () => {
        // If already restarting, mark that we need another restart when done
        if (isRestarting) {
            console.log('[LISTEN] Restart in progress, will retry after');
            restartPending = true;
            return;
        }

        await _startDirectListenInternal();

        // If another restart was requested during this one, do it now
        while (restartPending) {
            restartPending = false;
            console.log('[LISTEN] Processing pending restart');
            await _startDirectListenInternal();
        }
    }, TUNE_DEBOUNCE_MS);
}

/**
 * Start listening immediately (no debounce) - for button press
 */
async function startDirectListenImmediate() {
    if (listenDebounceTimer) {
        clearTimeout(listenDebounceTimer);
        listenDebounceTimer = null;
    }
    restartPending = false; // Clear any pending
    if (isRestarting) {
        console.log('[LISTEN] Waiting for current restart to finish...');
        // Wait for current restart to complete (max 5 seconds)
        let waitCount = 0;
        while (isRestarting && waitCount < 50) {
            await new Promise(r => setTimeout(r, 100));
            waitCount++;
        }
    }
    await _startDirectListenInternal();
}

// ============== WEBSOCKET AUDIO ==============

/**
 * Initialize WebSocket audio connection
 */
function initWebSocketAudio() {
    if (audioWebSocket && audioWebSocket.readyState === WebSocket.OPEN) {
        return audioWebSocket;
    }

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/ws/audio`;

    console.log('[WS-AUDIO] Connecting to:', wsUrl);
    audioWebSocket = new WebSocket(wsUrl);
    audioWebSocket.binaryType = 'arraybuffer';

    audioWebSocket.onopen = () => {
        console.log('[WS-AUDIO] Connected');
        isWebSocketAudio = true;
    };

    audioWebSocket.onclose = () => {
        console.log('[WS-AUDIO] Disconnected');
        isWebSocketAudio = false;
        audioWebSocket = null;
    };

    audioWebSocket.onerror = (e) => {
        console.error('[WS-AUDIO] Error:', e);
        isWebSocketAudio = false;
    };

    audioWebSocket.onmessage = (event) => {
        if (typeof event.data === 'string') {
            // JSON message (status updates)
            try {
                const msg = JSON.parse(event.data);
                console.log('[WS-AUDIO] Status:', msg);
                if (msg.status === 'error') {
                    addScannerLogEntry('Audio error: ' + msg.message, '', 'error');
                }
            } catch (e) {}
        } else {
            // Binary data (audio)
            handleWebSocketAudioData(event.data);
        }
    };

    return audioWebSocket;
}

/**
 * Handle incoming WebSocket audio data
 */
function handleWebSocketAudioData(data) {
    const audioPlayer = document.getElementById('scannerAudioPlayer');
    if (!audioPlayer) return;

    // Use MediaSource API to stream audio
    if (!audioPlayer.msSource) {
        setupMediaSource(audioPlayer);
    }

    if (audioPlayer.sourceBuffer && !audioPlayer.sourceBuffer.updating) {
        try {
            audioPlayer.sourceBuffer.appendBuffer(new Uint8Array(data));
        } catch (e) {
            // Buffer full or other error, skip this chunk
        }
    } else {
        // Queue data for later
        audioQueue.push(new Uint8Array(data));
        if (audioQueue.length > 50) audioQueue.shift(); // Prevent memory buildup
    }
}

/**
 * Setup MediaSource for streaming audio
 */
function setupMediaSource(audioPlayer) {
    if (!window.MediaSource) {
        console.warn('[WS-AUDIO] MediaSource not supported');
        return;
    }

    const mediaSource = new MediaSource();
    audioPlayer.src = URL.createObjectURL(mediaSource);
    audioPlayer.msSource = mediaSource;

    mediaSource.addEventListener('sourceopen', () => {
        try {
            const sourceBuffer = mediaSource.addSourceBuffer('audio/mpeg');
            audioPlayer.sourceBuffer = sourceBuffer;

            sourceBuffer.addEventListener('updateend', () => {
                // Process queued data
                if (audioQueue.length > 0 && !sourceBuffer.updating) {
                    try {
                        sourceBuffer.appendBuffer(audioQueue.shift());
                    } catch (e) {}
                }
            });
        } catch (e) {
            console.error('[WS-AUDIO] Failed to create source buffer:', e);
        }
    });
}

/**
 * Send command over WebSocket
 */
function sendWebSocketCommand(cmd, config = {}) {
    if (!audioWebSocket || audioWebSocket.readyState !== WebSocket.OPEN) {
        initWebSocketAudio();
        // Wait for connection and retry
        setTimeout(() => sendWebSocketCommand(cmd, config), 500);
        return;
    }

    audioWebSocket.send(JSON.stringify({ cmd, config }));
}

async function _startDirectListenInternal() {
    console.log('[LISTEN] _startDirectListenInternal called');

    // Prevent overlapping restarts
    if (isRestarting) {
        console.log('[LISTEN] Already restarting, skipping');
        return;
    }
    isRestarting = true;

    try {
        if (isScannerRunning) {
            stopScanner();
        }

        const freqInput = document.getElementById('radioScanStart');
        const freq = freqInput ? parseFloat(freqInput.value) : 118.0;
        const squelch = parseInt(document.getElementById('radioSquelchValue')?.textContent) || 30;
        const gain = parseInt(document.getElementById('radioGainValue')?.textContent) || 40;

        console.log('[LISTEN] Tuning to:', freq, 'MHz', currentModulation);

        const listenBtn = document.getElementById('radioListenBtn');
        if (listenBtn) {
            listenBtn.innerHTML = '⏳ TUNING...';
            listenBtn.style.background = 'var(--accent-orange)';
            listenBtn.style.borderColor = 'var(--accent-orange)';
        }

        const audioPlayer = document.getElementById('scannerAudioPlayer');
        if (!audioPlayer) {
            addScannerLogEntry('Audio player not found', '', 'error');
            updateDirectListenUI(false);
            return;
        }

        // Fully reset audio element to clean state
        audioPlayer.oncanplay = null; // Remove old handler
        try {
            audioPlayer.pause();
        } catch (e) {}
        audioPlayer.removeAttribute('src');
        audioPlayer.load(); // Reset the element

        // Start audio on backend (it handles stopping old stream)
        const response = await fetch('/listening/audio/start', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                frequency: freq,
                modulation: currentModulation,
                squelch: squelch,
                gain: gain
            })
        });

        const result = await response.json();
        console.log('[LISTEN] Backend:', result.status);

        if (result.status !== 'started') {
            console.error('[LISTEN] Failed:', result.message);
            addScannerLogEntry('Failed: ' + (result.message || 'Unknown error'), '', 'error');
            isDirectListening = false;
            updateDirectListenUI(false);
            return;
        }

        // Wait for stream to be ready (backend needs time after restart)
        await new Promise(r => setTimeout(r, 300));

        // Connect to new stream
        const streamUrl = `/listening/audio/stream?t=${Date.now()}`;
        console.log('[LISTEN] Connecting to stream:', streamUrl);
        audioPlayer.src = streamUrl;

        // Wait for audio to be ready then play
        audioPlayer.oncanplay = () => {
            console.log('[LISTEN] Audio can play');
            audioPlayer.play().catch(e => console.warn('[LISTEN] Autoplay blocked:', e));
        };

        // Also try to play immediately (some browsers need this)
        audioPlayer.play().catch(e => {
            console.log('[LISTEN] Initial play blocked, waiting for canplay');
        });

        isDirectListening = true;
        updateDirectListenUI(true, freq);
        addScannerLogEntry(`${freq.toFixed(3)} MHz (${currentModulation.toUpperCase()})`, '', 'signal');

    } catch (e) {
        console.error('[LISTEN] Error:', e);
        addScannerLogEntry('Error: ' + e.message, '', 'error');
        isDirectListening = false;
        updateDirectListenUI(false);
    } finally {
        isRestarting = false;
    }
}

/**
 * Stop direct listening
 */
function stopDirectListen() {
    console.log('[LISTEN] Stopping');

    // Clear all pending state
    if (listenDebounceTimer) {
        clearTimeout(listenDebounceTimer);
        listenDebounceTimer = null;
    }
    restartPending = false;

    const audioPlayer = document.getElementById('scannerAudioPlayer');
    if (audioPlayer) {
        audioPlayer.pause();
        // Clear MediaSource if using WebSocket
        if (audioPlayer.msSource) {
            try {
                audioPlayer.msSource.endOfStream();
            } catch (e) {}
            audioPlayer.msSource = null;
            audioPlayer.sourceBuffer = null;
        }
        audioPlayer.src = '';
    }
    audioQueue = [];

    // Stop via WebSocket if connected
    if (audioWebSocket && audioWebSocket.readyState === WebSocket.OPEN) {
        sendWebSocketCommand('stop');
    }

    // Also stop via HTTP (fallback)
    fetch('/listening/audio/stop', { method: 'POST' }).catch(() => {});

    isDirectListening = false;
    updateDirectListenUI(false);
    addScannerLogEntry('Listening stopped');
}

/**
 * Update UI for direct listen mode
 */
function updateDirectListenUI(isPlaying, freq) {
    const listenBtn = document.getElementById('radioListenBtn');
    const statusLabel = document.getElementById('mainScannerModeLabel');
    const freqDisplay = document.getElementById('mainScannerFreq');
    const quickStatus = document.getElementById('lpQuickStatus');
    const quickFreq = document.getElementById('lpQuickFreq');

    if (listenBtn) {
        if (isPlaying) {
            listenBtn.innerHTML = '⏹ STOP';
            listenBtn.style.background = 'var(--accent-red)';
            listenBtn.style.borderColor = 'var(--accent-red)';
        } else {
            listenBtn.innerHTML = '🎧 LISTEN';
            listenBtn.style.background = 'var(--accent-purple)';
            listenBtn.style.borderColor = 'var(--accent-purple)';
        }
    }

    if (statusLabel) {
        statusLabel.textContent = isPlaying ? 'LISTENING' : 'STOPPED';
        statusLabel.style.color = isPlaying ? 'var(--accent-green)' : 'var(--text-muted)';
    }

    if (freqDisplay && freq) {
        freqDisplay.textContent = freq.toFixed(3);
    }

    if (quickStatus) {
        quickStatus.textContent = isPlaying ? 'LISTENING' : 'IDLE';
        quickStatus.style.color = isPlaying ? 'var(--accent-green)' : 'var(--accent-cyan)';
    }

    if (quickFreq && freq) {
        quickFreq.textContent = freq.toFixed(3) + ' MHz';
    }
}

/**
 * Tune frequency by delta
 */
function tuneFreq(delta) {
    const freqInput = document.getElementById('radioScanStart');
    if (freqInput) {
        let newFreq = parseFloat(freqInput.value) + delta;
        newFreq = Math.max(24, Math.min(1800, newFreq));
        freqInput.value = newFreq.toFixed(1);

        // Update display
        const freqDisplay = document.getElementById('mainScannerFreq');
        if (freqDisplay) {
            freqDisplay.textContent = newFreq.toFixed(3);
        }

        // Update tuning dial position (silent to avoid duplicate restart)
        const mainTuningDial = document.getElementById('mainTuningDial');
        if (mainTuningDial && mainTuningDial._dial) {
            mainTuningDial._dial.setValue(newFreq, true);
        }

        const quickFreq = document.getElementById('lpQuickFreq');
        if (quickFreq) {
            quickFreq.textContent = newFreq.toFixed(3) + ' MHz';
        }

        // If currently listening, restart stream at new frequency
        if (isDirectListening) {
            startDirectListen();
        }
    }
}

/**
 * Quick tune to a preset frequency
 */
function quickTune(freq, mod) {
    // Update frequency inputs
    const startInput = document.getElementById('radioScanStart');
    if (startInput) {
        startInput.value = freq;
    }

    // Update modulation (don't trigger auto-restart here, we'll handle it below)
    if (mod) {
        currentModulation = mod;
        // Update modulation UI without triggering restart
        document.querySelectorAll('#modBtnBank .radio-btn').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.mod === mod);
        });
        const badge = document.getElementById('mainScannerMod');
        if (badge) {
            const modLabels = { am: 'AM', fm: 'NFM', wfm: 'WFM', usb: 'USB', lsb: 'LSB' };
            badge.textContent = modLabels[mod] || mod.toUpperCase();
        }
    }

    // Update display
    const freqDisplay = document.getElementById('mainScannerFreq');
    if (freqDisplay) {
        freqDisplay.textContent = freq.toFixed(3);
    }

    // Update tuning dial position (silent to avoid duplicate restart)
    const mainTuningDial = document.getElementById('mainTuningDial');
    if (mainTuningDial && mainTuningDial._dial) {
        mainTuningDial._dial.setValue(freq, true);
    }

    const quickFreq = document.getElementById('lpQuickFreq');
    if (quickFreq) {
        quickFreq.textContent = freq.toFixed(3) + ' MHz';
    }

    addScannerLogEntry(`Quick tuned to ${freq.toFixed(3)} MHz (${mod.toUpperCase()})`);

    // If currently listening, restart immediately (this is a deliberate preset selection)
    if (isDirectListening) {
        startDirectListenImmediate();
    }
}

/**
 * Enhanced setModulation to also update currentModulation
 * Uses immediate restart if currently listening
 */
const originalSetModulation = window.setModulation;
window.setModulation = function(mod) {
    console.log('[MODULATION] Setting modulation to:', mod, 'isListening:', isDirectListening);
    currentModulation = mod;

    // Update modulation button states
    document.querySelectorAll('#modBtnBank .radio-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.mod === mod);
    });

    // Update badge
    const badge = document.getElementById('mainScannerMod');
    if (badge) {
        const modLabels = { am: 'AM', fm: 'NFM', wfm: 'WFM', usb: 'USB', lsb: 'LSB' };
        badge.textContent = modLabels[mod] || mod.toUpperCase();
    }

    // Update scanner modulation select if exists
    const modSelect = document.getElementById('scannerModulation');
    if (modSelect) {
        modSelect.value = mod;
    }

    // Sync with scanner if running
    updateScannerConfig({ modulation: mod });

    // If currently listening, restart immediately (deliberate modulation change)
    if (isDirectListening) {
        console.log('[MODULATION] Restarting audio with new modulation:', mod);
        startDirectListenImmediate();
    } else {
        console.log('[MODULATION] Not listening, just updated UI');
    }
};

/**
 * Update sidebar quick status
 */
function updateQuickStatus() {
    const quickStatus = document.getElementById('lpQuickStatus');
    const quickFreq = document.getElementById('lpQuickFreq');
    const quickSignals = document.getElementById('lpQuickSignals');

    if (quickStatus) {
        if (isScannerRunning) {
            quickStatus.textContent = isScannerPaused ? 'PAUSED' : 'SCANNING';
            quickStatus.style.color = isScannerPaused ? 'var(--accent-orange)' : 'var(--accent-green)';
        } else if (isDirectListening) {
            quickStatus.textContent = 'LISTENING';
            quickStatus.style.color = 'var(--accent-green)';
        } else {
            quickStatus.textContent = 'IDLE';
            quickStatus.style.color = 'var(--accent-cyan)';
        }
    }

    if (quickSignals) {
        quickSignals.textContent = scannerSignalCount;
    }
}

// ============== SIDEBAR CONTROLS ==============

// Frequency bookmarks stored in localStorage
let frequencyBookmarks = [];

/**
 * Load bookmarks from localStorage
 */
function loadFrequencyBookmarks() {
    try {
        const saved = localStorage.getItem('lpBookmarks');
        if (saved) {
            frequencyBookmarks = JSON.parse(saved);
            renderBookmarks();
        }
    } catch (e) {
        console.warn('Failed to load bookmarks:', e);
    }
}

/**
 * Save bookmarks to localStorage
 */
function saveFrequencyBookmarks() {
    try {
        localStorage.setItem('lpBookmarks', JSON.stringify(frequencyBookmarks));
    } catch (e) {
        console.warn('Failed to save bookmarks:', e);
    }
}

/**
 * Add a frequency bookmark
 */
function addFrequencyBookmark() {
    const input = document.getElementById('bookmarkFreqInput');
    if (!input) return;

    const freq = parseFloat(input.value);
    if (isNaN(freq) || freq <= 0) {
        if (typeof showNotification === 'function') {
            showNotification('Invalid Frequency', 'Please enter a valid frequency');
        }
        return;
    }

    // Check for duplicates
    if (frequencyBookmarks.some(b => Math.abs(b.freq - freq) < 0.001)) {
        if (typeof showNotification === 'function') {
            showNotification('Duplicate', 'This frequency is already bookmarked');
        }
        return;
    }

    frequencyBookmarks.push({
        freq: freq,
        mod: currentModulation || 'am',
        added: new Date().toISOString()
    });

    saveFrequencyBookmarks();
    renderBookmarks();
    input.value = '';

    if (typeof showNotification === 'function') {
        showNotification('Bookmark Added', `${freq.toFixed(3)} MHz saved`);
    }
}

/**
 * Remove a bookmark by index
 */
function removeBookmark(index) {
    frequencyBookmarks.splice(index, 1);
    saveFrequencyBookmarks();
    renderBookmarks();
}

/**
 * Render bookmarks list
 */
function renderBookmarks() {
    const container = document.getElementById('bookmarksList');
    if (!container) return;

    if (frequencyBookmarks.length === 0) {
        container.innerHTML = '<div style="color: var(--text-muted); text-align: center; padding: 10px;">No bookmarks saved</div>';
        return;
    }

    container.innerHTML = frequencyBookmarks.map((b, i) => `
        <div style="display: flex; justify-content: space-between; align-items: center; padding: 4px 6px; background: rgba(0,0,0,0.2); border-radius: 3px; margin-bottom: 3px;">
            <span style="cursor: pointer; color: var(--accent-cyan);" onclick="quickTune(${b.freq}, '${b.mod}')">${b.freq.toFixed(3)} MHz</span>
            <span style="color: var(--text-muted); font-size: 9px;">${b.mod.toUpperCase()}</span>
            <button onclick="removeBookmark(${i})" style="background: none; border: none; color: var(--accent-red); cursor: pointer; font-size: 12px; padding: 0 4px;">×</button>
        </div>
    `).join('');
}


/**
 * Add a signal to the sidebar recent signals list
 */
function addSidebarRecentSignal(freq, mod) {
    const container = document.getElementById('sidebarRecentSignals');
    if (!container) return;

    // Clear placeholder if present
    if (container.innerHTML.includes('No signals yet')) {
        container.innerHTML = '';
    }

    const timestamp = new Date().toLocaleTimeString();
    const signalDiv = document.createElement('div');
    signalDiv.style.cssText = 'display: flex; justify-content: space-between; align-items: center; padding: 3px 6px; background: rgba(0,255,100,0.1); border-left: 2px solid var(--accent-green); margin-bottom: 2px; border-radius: 2px;';
    signalDiv.innerHTML = `
        <span style="cursor: pointer; color: var(--accent-green);" onclick="quickTune(${freq}, '${mod}')">${freq.toFixed(3)}</span>
        <span style="color: var(--text-muted); font-size: 8px;">${timestamp}</span>
    `;

    container.insertBefore(signalDiv, container.firstChild);

    // Keep only last 10 signals
    while (container.children.length > 10) {
        container.removeChild(container.lastChild);
    }
}

// Load bookmarks on init
document.addEventListener('DOMContentLoaded', loadFrequencyBookmarks);

// Export functions for HTML onclick handlers
window.toggleDirectListen = toggleDirectListen;
window.startDirectListen = startDirectListen;
window.stopDirectListen = stopDirectListen;
window.toggleScanner = toggleScanner;
window.startScanner = startScanner;
window.stopScanner = stopScanner;
window.pauseScanner = pauseScanner;
window.skipSignal = skipSignal;
// Note: setModulation is already exported with enhancements above
window.setBand = setBand;
window.tuneFreq = tuneFreq;
window.quickTune = quickTune;
window.addFrequencyBookmark = addFrequencyBookmark;
window.removeBookmark = removeBookmark;
window.tuneToFrequency = tuneToFrequency;
window.clearScannerLog = clearScannerLog;
window.exportScannerLog = exportScannerLog;

