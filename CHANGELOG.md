# Changelog

All notable changes to iNTERCEPT will be documented in this file.

## [2.9.0] - 2026-01-10

### Added
- **Landing Page** - Animated welcome screen with logo reveal and "See the Invisible" tagline
- **New Branding** - Redesigned logo featuring 'i' with signal wave brackets
- **Logo Assets** - Full-size SVG logos in `/static/img/` for external use
- **Instagram Promo** - Animated HTML promo video template in `/promo/` directory
- **Listening Post Scanner** - Fully functional frequency scanning with signal detection
  - Scan button toggles between start/stop states
  - Signal hits logged with Listen button to tune directly
  - Proper 4-column display (Time, Frequency, Modulation, Action)

### Changed
- **Rebranding** - Application renamed from "INTERCEPT" to "iNTERCEPT"
- **Updated Tagline** - "Signal Intelligence & Counter Surveillance Platform"
- **Setup Script** - Now installs Python packages via apt first (more reliable on Debian/Ubuntu)
  - Uses `--system-site-packages` for venv to leverage apt packages
  - Added fallback logic when pip fails
- **Troubleshooting Docs** - Added sections for pip install issues and apt alternatives

### Fixed
- **Tuning Dial Audio** - Fixed audio stopping when using tuning knob
  - Added restart prevention flags to avoid overlapping restarts
  - Increased debounce time for smoother operation
  - Added silent mode for programmatic value changes
- **Scanner Signal Hits** - Fixed table column count and colspan
- **Favicon** - Updated to new 'i' logo design

---

## [2.0.0] - 2026-01-06

### Added
- **Listening Post Mode** - New frequency scanner with automatic signal detection
  - Scans frequency ranges and stops on detected signals
  - Real-time audio monitoring with ffmpeg integration
  - Skip button to continue scanning after signal detection
  - Configurable dwell time, squelch, and step size
  - Preset frequency bands (FM broadcast, Air band, Marine, etc.)
  - Activity log of detected signals
- **Aircraft Dashboard Improvements**
  - Dependency warning when rtl_fm or ffmpeg not installed
  - Auto-restart audio when switching frequencies
  - Fixed toolbar overflow with custom frequency input
- **Device Correlation** - Match WiFi and Bluetooth devices by manufacturer
- **Settings System** - SQLite-based persistent settings storage
- **Comprehensive Test Suite** - Added tests for routes, validation, correlation, database

### Changed
- **Documentation Overhaul**
  - Simplified README with clear macOS and Debian installation steps
  - Added Docker installation option
  - Complete tool reference table in HARDWARE.md
  - Removed redundant/confusing content
- **Setup Script Rewrite**
  - Full macOS support with Homebrew auto-installation
  - Improved Debian/Ubuntu package detection
  - Added ffmpeg to tool checks
  - Better error messages with platform-specific install commands
- **Dockerfile Updated**
  - Added ffmpeg for Listening Post audio encoding
  - Added dump1090 with fallback for different package names

### Fixed
- SoapySDR device detection for RTL-SDR and HackRF
- Aircraft dashboard toolbar layout when using custom frequency input
- Frequency switching now properly stops/restarts audio

### Technical
- Added `utils/constants.py` for centralized configuration values
- Added `utils/database.py` for SQLite settings storage
- Added `utils/correlation.py` for device correlation logic
- Added `routes/listening_post.py` for scanner endpoints
- Added `routes/settings.py` for settings API
- Added `routes/correlation.py` for correlation API

---

## [1.2.0] - 2026-12-29

### Added
- Airspy SDR support
- GPS coordinate persistence
- SoapySDR device detection improvements

### Fixed
- RTL-SDR and HackRF detection via SoapySDR

---

## [1.1.0] - 2026-12-18

### Added
- Satellite tracking with TLE data
- Full-screen dashboard for aircraft radar
- Full-screen dashboard for satellite tracking

---

## [1.0.0] - 2026-12-15

### Initial Release
- Pager decoding (POCSAG/FLEX)
- 433MHz sensor decoding
- ADS-B aircraft tracking
- WiFi reconnaissance
- Bluetooth scanning
- Multi-SDR support (RTL-SDR, LimeSDR, HackRF)

