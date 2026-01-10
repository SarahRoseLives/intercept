/**
 * Intercept - Core Utility Functions
 * Pure utility functions with no DOM dependencies
 */

// ============== HTML ESCAPING ==============

/**
 * Escape HTML to prevent XSS
 * @param {string} text - Text to escape
 * @returns {string} Escaped HTML
 */
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

/**
 * Escape text for use in HTML attributes (especially onclick handlers)
 * @param {string} text - Text to escape
 * @returns {string} Escaped attribute value
 */
function escapeAttr(text) {
    if (text === null || text === undefined) return '';
    var s = String(text);
    s = s.replace(/&/g, '&amp;');
    s = s.replace(/'/g, '&#39;');
    s = s.replace(/"/g, '&quot;');
    s = s.replace(/</g, '&lt;');
    s = s.replace(/>/g, '&gt;');
    return s;
}

// ============== VALIDATION ==============

/**
 * Validate MAC address format (XX:XX:XX:XX:XX:XX)
 * @param {string} mac - MAC address to validate
 * @returns {boolean} True if valid
 */
function isValidMac(mac) {
    return /^([0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}$/.test(mac);
}

/**
 * Validate WiFi channel (1-200 covers all bands)
 * @param {string|number} ch - Channel number
 * @returns {boolean} True if valid
 */
function isValidChannel(ch) {
    const num = parseInt(ch, 10);
    return !isNaN(num) && num >= 1 && num <= 200;
}

// ============== TIME FORMATTING ==============

/**
 * Get relative time string from timestamp
 * @param {string} timestamp - Time string in HH:MM:SS format
 * @returns {string} Relative time like "5s ago", "2m ago"
 */
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

/**
 * Format UTC time string
 * @param {Date} date - Date object
 * @returns {string} UTC time in HH:MM:SS format
 */
function formatUtcTime(date) {
    return date.toISOString().substring(11, 19);
}

// ============== DISTANCE CALCULATIONS ==============

/**
 * Calculate distance between two points in nautical miles
 * Uses Haversine formula
 * @param {number} lat1 - Latitude of first point
 * @param {number} lon1 - Longitude of first point
 * @param {number} lat2 - Latitude of second point
 * @param {number} lon2 - Longitude of second point
 * @returns {number} Distance in nautical miles
 */
function calculateDistanceNm(lat1, lon1, lat2, lon2) {
    const R = 3440.065;  // Earth radius in nautical miles
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLon = (lon2 - lon1) * Math.PI / 180;
    const a = Math.sin(dLat/2) * Math.sin(dLat/2) +
              Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
              Math.sin(dLon/2) * Math.sin(dLon/2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    return R * c;
}

/**
 * Calculate distance between two points in kilometers
 * @param {number} lat1 - Latitude of first point
 * @param {number} lon1 - Longitude of first point
 * @param {number} lat2 - Latitude of second point
 * @param {number} lon2 - Longitude of second point
 * @returns {number} Distance in kilometers
 */
function calculateDistanceKm(lat1, lon1, lat2, lon2) {
    const R = 6371;  // Earth radius in kilometers
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLon = (lon2 - lon1) * Math.PI / 180;
    const a = Math.sin(dLat/2) * Math.sin(dLat/2) +
              Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
              Math.sin(dLon/2) * Math.sin(dLon/2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    return R * c;
}

// ============== FILE OPERATIONS ==============

/**
 * Download content as a file
 * @param {string} content - File content
 * @param {string} filename - Name for the downloaded file
 * @param {string} type - MIME type
 */
function downloadFile(content, filename, type) {
    const blob = new Blob([content], { type });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
}

// ============== FREQUENCY FORMATTING ==============

/**
 * Format frequency value with proper units
 * @param {number} freqMhz - Frequency in MHz
 * @param {number} decimals - Number of decimal places (default 3)
 * @returns {string} Formatted frequency string
 */
function formatFrequency(freqMhz, decimals = 3) {
    return freqMhz.toFixed(decimals) + ' MHz';
}

/**
 * Parse frequency string to MHz
 * @param {string} freqStr - Frequency string (e.g., "118.0", "118.0 MHz")
 * @returns {number} Frequency in MHz
 */
function parseFrequency(freqStr) {
    return parseFloat(freqStr.replace(/[^\d.-]/g, ''));
}

// ============== LOCAL STORAGE HELPERS ==============

/**
 * Get item from localStorage with JSON parsing
 * @param {string} key - Storage key
 * @param {*} defaultValue - Default value if key doesn't exist
 * @returns {*} Parsed value or default
 */
function getStorageItem(key, defaultValue = null) {
    const saved = localStorage.getItem(key);
    if (saved === null) return defaultValue;
    try {
        return JSON.parse(saved);
    } catch (e) {
        return saved;
    }
}

/**
 * Set item in localStorage with JSON stringification
 * @param {string} key - Storage key
 * @param {*} value - Value to store
 */
function setStorageItem(key, value) {
    if (typeof value === 'object') {
        localStorage.setItem(key, JSON.stringify(value));
    } else {
        localStorage.setItem(key, value);
    }
}

// ============== ARRAY/OBJECT UTILITIES ==============

/**
 * Debounce function execution
 * @param {Function} func - Function to debounce
 * @param {number} wait - Wait time in milliseconds
 * @returns {Function} Debounced function
 */
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

/**
 * Throttle function execution
 * @param {Function} func - Function to throttle
 * @param {number} limit - Time limit in milliseconds
 * @returns {Function} Throttled function
 */
function throttle(func, limit) {
    let inThrottle;
    return function executedFunction(...args) {
        if (!inThrottle) {
            func(...args);
            inThrottle = true;
            setTimeout(() => inThrottle = false, limit);
        }
    };
}

// ============== NUMBER FORMATTING ==============

/**
 * Format large numbers with K/M suffixes
 * @param {number} num - Number to format
 * @returns {string} Formatted string
 */
function formatNumber(num) {
    if (num >= 1000000) {
        return (num / 1000000).toFixed(1) + 'M';
    }
    if (num >= 1000) {
        return (num / 1000).toFixed(1) + 'K';
    }
    return num.toString();
}

/**
 * Clamp a number between min and max
 * @param {number} num - Number to clamp
 * @param {number} min - Minimum value
 * @param {number} max - Maximum value
 * @returns {number} Clamped value
 */
function clamp(num, min, max) {
    return Math.min(Math.max(num, min), max);
}

/**
 * Map a value from one range to another
 * @param {number} value - Value to map
 * @param {number} inMin - Input range minimum
 * @param {number} inMax - Input range maximum
 * @param {number} outMin - Output range minimum
 * @param {number} outMax - Output range maximum
 * @returns {number} Mapped value
 */
function mapRange(value, inMin, inMax, outMin, outMax) {
    return (value - inMin) * (outMax - outMin) / (inMax - inMin) + outMin;
}
