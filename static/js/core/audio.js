/**
 * Intercept - Audio System
 * Web Audio API alerts, notifications, and sound effects
 */

// ============== AUDIO STATE ==============

let audioContext = null;
let audioMuted = localStorage.getItem('audioMuted') === 'true';
let notificationsEnabled = false;

// ============== AUDIO CONTEXT ==============

/**
 * Initialize the Web Audio API context
 * Must be called after user interaction due to browser autoplay policies
 */
function initAudio() {
    if (!audioContext) {
        audioContext = new (window.AudioContext || window.webkitAudioContext)();
    }
    return audioContext;
}

/**
 * Get or create the audio context
 * @returns {AudioContext}
 */
function getAudioContext() {
    if (!audioContext) {
        audioContext = new (window.AudioContext || window.webkitAudioContext)();
    }
    return audioContext;
}

// ============== ALERT SOUNDS ==============

/**
 * Play a basic alert beep
 * Used for message received notifications
 */
function playAlert() {
    if (audioMuted || !audioContext) return;

    try {
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
    } catch (e) {
        console.warn('Audio alert failed:', e);
    }
}

/**
 * Play alert sound by type
 * @param {string} type - 'emergency', 'military', 'warning', 'info'
 */
function playAlertSound(type) {
    if (audioMuted) return;

    try {
        const ctx = getAudioContext();
        const oscillator = ctx.createOscillator();
        const gainNode = ctx.createGain();

        oscillator.connect(gainNode);
        gainNode.connect(ctx.destination);

        switch (type) {
            case 'emergency':
                // Urgent two-tone alert for emergencies
                oscillator.frequency.setValueAtTime(880, ctx.currentTime);
                oscillator.frequency.setValueAtTime(660, ctx.currentTime + 0.15);
                oscillator.frequency.setValueAtTime(880, ctx.currentTime + 0.3);
                gainNode.gain.setValueAtTime(0.3, ctx.currentTime);
                gainNode.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.5);
                oscillator.start(ctx.currentTime);
                oscillator.stop(ctx.currentTime + 0.5);
                break;

            case 'military':
                // Single tone for military aircraft detection
                oscillator.frequency.setValueAtTime(523, ctx.currentTime);
                gainNode.gain.setValueAtTime(0.2, ctx.currentTime);
                gainNode.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.3);
                oscillator.start(ctx.currentTime);
                oscillator.stop(ctx.currentTime + 0.3);
                break;

            case 'warning':
                // Warning tone (descending)
                oscillator.frequency.setValueAtTime(660, ctx.currentTime);
                oscillator.frequency.exponentialRampToValueAtTime(440, ctx.currentTime + 0.3);
                gainNode.gain.setValueAtTime(0.25, ctx.currentTime);
                gainNode.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.3);
                oscillator.start(ctx.currentTime);
                oscillator.stop(ctx.currentTime + 0.3);
                break;

            case 'info':
            default:
                // Simple info tone
                oscillator.frequency.setValueAtTime(440, ctx.currentTime);
                gainNode.gain.setValueAtTime(0.15, ctx.currentTime);
                gainNode.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.15);
                oscillator.start(ctx.currentTime);
                oscillator.stop(ctx.currentTime + 0.15);
                break;
        }
    } catch (e) {
        console.warn('Audio alert failed:', e);
    }
}

/**
 * Play scanner signal detected sound
 * A distinctive ascending tone for radio scanner
 */
function playSignalDetectedSound() {
    if (audioMuted) return;

    try {
        const ctx = getAudioContext();
        const oscillator = ctx.createOscillator();
        const gainNode = ctx.createGain();

        oscillator.connect(gainNode);
        gainNode.connect(ctx.destination);

        // Ascending tone
        oscillator.frequency.setValueAtTime(400, ctx.currentTime);
        oscillator.frequency.exponentialRampToValueAtTime(800, ctx.currentTime + 0.15);
        oscillator.type = 'sine';

        gainNode.gain.setValueAtTime(0.2, ctx.currentTime);
        gainNode.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.2);

        oscillator.start(ctx.currentTime);
        oscillator.stop(ctx.currentTime + 0.2);
    } catch (e) {
        console.warn('Signal detected sound failed:', e);
    }
}

/**
 * Play a click sound for UI feedback
 */
function playClickSound() {
    if (audioMuted) return;

    try {
        const ctx = getAudioContext();
        const oscillator = ctx.createOscillator();
        const gainNode = ctx.createGain();

        oscillator.connect(gainNode);
        gainNode.connect(ctx.destination);

        oscillator.frequency.value = 1000;
        oscillator.type = 'square';

        gainNode.gain.setValueAtTime(0.1, ctx.currentTime);
        gainNode.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.05);

        oscillator.start(ctx.currentTime);
        oscillator.stop(ctx.currentTime + 0.05);
    } catch (e) {
        console.warn('Click sound failed:', e);
    }
}

// ============== MUTE CONTROL ==============

/**
 * Toggle mute state
 */
function toggleMute() {
    audioMuted = !audioMuted;
    localStorage.setItem('audioMuted', audioMuted);
    updateMuteButton();
}

/**
 * Set mute state
 * @param {boolean} muted - Whether audio should be muted
 */
function setMuted(muted) {
    audioMuted = muted;
    localStorage.setItem('audioMuted', audioMuted);
    updateMuteButton();
}

/**
 * Get current mute state
 * @returns {boolean}
 */
function isMuted() {
    return audioMuted;
}

/**
 * Update mute button UI
 */
function updateMuteButton() {
    const btn = document.getElementById('muteBtn');
    if (btn) {
        btn.innerHTML = audioMuted ? '🔇 UNMUTE' : '🔊 MUTE';
        btn.classList.toggle('muted', audioMuted);
    }
}

// ============== DESKTOP NOTIFICATIONS ==============

/**
 * Request notification permission from user
 */
function requestNotificationPermission() {
    if ('Notification' in window && Notification.permission === 'default') {
        Notification.requestPermission().then(permission => {
            notificationsEnabled = permission === 'granted';
            if (notificationsEnabled && typeof showInfo === 'function') {
                showInfo('🔔 Desktop notifications enabled');
            }
        });
    }
}

/**
 * Show a desktop notification
 * @param {string} title - Notification title
 * @param {string} body - Notification body
 */
function showNotification(title, body) {
    if (notificationsEnabled && document.hidden) {
        new Notification(title, {
            body: body,
            icon: '/favicon.ico',
            tag: 'intercept-' + Date.now()
        });
    }
}

// ============== INITIALIZATION ==============

/**
 * Initialize audio system
 * Should be called on first user interaction
 */
function initAudioSystem() {
    // Initialize audio context
    initAudio();

    // Update mute button state
    updateMuteButton();

    // Check notification permission
    if ('Notification' in window) {
        if (Notification.permission === 'granted') {
            notificationsEnabled = true;
        } else if (Notification.permission === 'default') {
            // Will request on first interaction
            document.addEventListener('click', function requestOnce() {
                requestNotificationPermission();
                document.removeEventListener('click', requestOnce);
            }, { once: true });
        }
    }
}

// Initialize on first user interaction (required for Web Audio API)
document.addEventListener('click', function initOnInteraction() {
    initAudio();
    document.removeEventListener('click', initOnInteraction);
}, { once: true });
