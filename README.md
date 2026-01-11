# INTERCEPT

<p align="center">
  <img src="https://img.shields.io/badge/python-3.9+-blue.svg" alt="Python 3.9+">
  <img src="https://img.shields.io/badge/license-MIT-green.svg" alt="MIT License">
  <img src="https://img.shields.io/badge/platform-macOS%20%7C%20Linux-lightgrey.svg" alt="Platform">
</p>

<p align="center">
  <strong>Signal Intelligence Platform</strong><br>
  A web-based interface for software-defined radio tools.
</p>

<p align="center">
  <img src="static/images/screenshots/logo-banner.png" alt="Screenshot">
</p>

---

## Features

- **Pager Decoding** - POCSAG/FLEX via rtl_fm + multimon-ng
- **433MHz Sensors** - Weather stations, TPMS, IoT devices via rtl_433
- **Aircraft Tracking** - ADS-B via dump1090 with real-time map and radar
- **Listening Post** - Frequency scanner with audio monitoring
- **Satellite Tracking** - Pass prediction using TLE data
- **WiFi Scanning** - Monitor mode reconnaissance via aircrack-ng
- **Bluetooth Scanning** - Device discovery and tracker detection

---

## Installation / Debian / Ubuntu / MacOS

```

**1. Clone and run:**
```bash
git clone https://github.com/smittix/intercept.git
cd intercept
./setup.sh
sudo python3 intercept.py
```

### Docker (Alternative)

```bash
git clone https://github.com/smittix/intercept.git
cd intercept
docker compose up -d
```

> **Note:** Docker requires privileged mode for USB SDR access. See `docker-compose.yml` for configuration options.

### Open the Interface

After starting, open **http://localhost:5050** in your browser.

---

## Hardware Requirements

| Hardware | Purpose | Price |
|----------|---------|-------|
| **RTL-SDR** | Required for all SDR features | ~$25-35 |
| **WiFi adapter** | Must support promiscuous (monitor) mode | ~$20-40 |
| **Bluetooth adapter** | Device scanning (usually built-in) | - |
| **GPS** | Any Linux supported GPS Unit | ~10 |

Most features work with a basic RTL-SDR dongle (RTL2832U + R820T2).

| :exclamation:  Not using an RTL-SDR Device?   |
|-----------------------------------------------
|Intercept supports any device that SoapySDR supports. You must however have the correct module for your device installed! For example if you have an SDRPlay device you'd need to install soapysdr-module-sdrplay.

| :exclamation:  GPS Usage   |
|-----------------------------------------------
|gpsd is needed for real time location. Intercept automatically checks to see if you're running gpsd in the background when any maps are rendered.

---

## Discord Server

<p align="center">
  <a href="https://discord.gg/z3g3NJMe">Join our Discord</a>
</p>

---

## Documentation

- [Usage Guide](docs/USAGE.md) - Detailed instructions for each mode
- [Hardware Guide](docs/HARDWARE.md) - SDR hardware and advanced setup
- [Troubleshooting](docs/TROUBLESHOOTING.md) - Common issues and solutions
- [Security](docs/SECURITY.md) - Network security and best practices

---

## Disclaimer

This project was developed using AI as a coding partner, combining human direction with AI-assisted implementation. The goal: make Software Defined Radio more accessible by providing a clean, unified interface for common SDR tools.

**This software is for educational and authorized testing purposes only.**

- Only use with proper authorization
- Intercepting communications without consent may be illegal
- You are responsible for compliance with applicable laws

---

## License

MIT License - see [LICENSE](LICENSE)

## Author

Created by **smittix** - [GitHub](https://github.com/smittix)

## Acknowledgments

[rtl-sdr](https://osmocom.org/projects/rtl-sdr/wiki) |
[multimon-ng](https://github.com/EliasOenal/multimon-ng) |
[rtl_433](https://github.com/merbanan/rtl_433) |
[dump1090](https://github.com/flightaware/dump1090) |
[aircrack-ng](https://www.aircrack-ng.org/) |
[Leaflet.js](https://leafletjs.com/) |
[Celestrak](https://celestrak.org/)





