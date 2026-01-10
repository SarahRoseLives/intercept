# Troubleshooting

Solutions for common issues.

## Python / Installation Issues

### "ModuleNotFoundError: No module named 'flask'"

Install Python dependencies first:
```bash
pip install -r requirements.txt

# Or with python3 explicitly
python3 -m pip install -r requirements.txt
```

### pip install fails for flask or skyfield

On newer Debian/Ubuntu systems, pip may fail with permission errors or dependency conflicts. **Use apt instead:**

```bash
# Install Python packages via apt (recommended for Debian/Ubuntu)
sudo apt install python3-flask python3-requests python3-serial python3-skyfield

# Then create venv with system packages
python3 -m venv --system-site-packages venv
source venv/bin/activate
sudo venv/bin/python intercept.py
```

### "error: externally-managed-environment" (pip blocked)

This is PEP 668 protection on Ubuntu 23.04+, Debian 12+, and similar systems. Solutions:

```bash
# Option 1: Use apt packages (recommended)
sudo apt install python3-flask python3-requests python3-serial python3-skyfield
python3 -m venv --system-site-packages venv
source venv/bin/activate

# Option 2: Use pipx for isolated install
pipx install flask

# Option 3: Force pip (not recommended)
pip install --break-system-packages flask
```

### "TypeError: 'type' object is not subscriptable"

This error occurs on Python 3.7 or 3.8. **INTERCEPT requires Python 3.9 or later.**

```bash
# Check your Python version
python3 --version

# Ubuntu/Debian - install newer Python
sudo apt update
sudo apt install python3.11 python3.11-venv python3-pip

# Run with newer Python
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
sudo venv/bin/python intercept.py
```

### Alternative: Use the setup script

The setup script handles all installation automatically, including apt packages:

```bash
chmod +x setup.sh
./setup.sh
```

### "pip: command not found"

```bash
# Ubuntu/Debian
sudo apt install python3-pip

# macOS
python3 -m ensurepip --upgrade
```

### Permission denied during pip install

```bash
# Install to user directory
pip install --user -r requirements.txt
```

## SDR Hardware Issues

### No SDR devices found

1. Ensure your SDR device is plugged in
2. Check detection:
   - RTL-SDR: `rtl_test`
   - LimeSDR/HackRF: `SoapySDRUtil --find`
3. On Linux, add udev rules (see below)
4. Blacklist conflicting drivers:
   ```bash
   echo "blacklist dvb_usb_rtl28xxu" | sudo tee /etc/modprobe.d/blacklist-rtl.conf
   sudo modprobe -r dvb_usb_rtl28xxu
   ```

### Linux udev rules for RTL-SDR

```bash
sudo bash -c 'cat > /etc/udev/rules.d/20-rtlsdr.rules << EOF
SUBSYSTEM=="usb", ATTRS{idVendor}=="0bda", ATTRS{idProduct}=="2838", MODE="0666"
SUBSYSTEM=="usb", ATTRS{idVendor}=="0bda", ATTRS{idProduct}=="2832", MODE="0666"
EOF'

sudo udevadm control --reload-rules
sudo udevadm trigger
```

Then unplug and replug your RTL-SDR.

### Device busy error

1. Click "Kill All Processes" in the UI
2. Unplug and replug the SDR device
3. Check for other applications: `lsof | grep rtl`

### LimeSDR/HackRF not detected
Ensure the correct SoapySDR module for your hardware is installed first

1. Verify SoapySDR is installed: `SoapySDRUtil --info`
2. Check driver is loaded: `SoapySDRUtil --find`
3. May need udev rules or run as root

### Using HackRF/Airspy/LimeSDR with ADS-B

For non-RTL-SDR devices, ADS-B requires `readsb` compiled with SoapySDR support (standard dump1090 won't work).

**Option 1: Run readsb separately and connect via Remote mode**

1. Start readsb with your device:
   ```bash
   # HackRF
   readsb --device-type soapysdr --device driver=hackrf --net --quiet

   # Airspy
   readsb --device-type soapysdr --device driver=airspy --net --quiet

   # LimeSDR
   readsb --device-type soapysdr --device driver=lime --net --quiet
   ```

2. In Intercept's ADS-B dashboard:
   - Check the **"Remote"** checkbox
   - Enter Host: `localhost` and Port: `30003`
   - Click **START**

3. Intercept will connect to readsb's SBS output on port 30003

**Option 2: Install readsb with SoapySDR support**

On Debian/Ubuntu:
```bash
# Install dependencies
sudo apt install build-essential debhelper librtlsdr-dev pkg-config \
    libncurses5-dev libbladerf-dev libhackrf-dev liblimesuite-dev libsoapysdr-dev

# Clone and build
git clone https://github.com/wiedehopf/readsb.git
cd readsb
dpkg-buildpackage -b --no-sign
sudo dpkg -i ../readsb_*.deb
```

### Using HackRF/Airspy with Listening Post

The Listening Post requires `rx_fm` from SoapySDR utilities for non-RTL-SDR devices.

```bash
# Install SoapySDR utilities (includes rx_fm)
sudo apt install soapysdr-tools

# Verify rx_fm is available
which rx_fm
```

If `rx_fm` is installed, select your device from the SDR dropdown in the Listening Post - HackRF, Airspy, LimeSDR, and SDRPlay are all supported.

### Setting up Icecast for Listening Post Audio

The Listening Post uses Icecast for low-latency audio streaming (2-10 second latency). Intercept will automatically start Icecast when you begin listening, but you must install and configure it first.

**Install Icecast:**
```bash
# Ubuntu/Debian
sudo apt install icecast2

# macOS
brew install icecast
```

**Configure Icecast:**

During installation on Debian/Ubuntu, you'll be prompted to configure. Otherwise, edit `/etc/icecast2/icecast.xml`:

```xml
<icecast>
    <authentication>
        <!-- Source password - used by ffmpeg to send audio -->
        <source-password>hackme</source-password>
        <!-- Admin password for web interface -->
        <admin-password>your-admin-password</admin-password>
    </authentication>
    <hostname>localhost</hostname>
    <listen-socket>
        <port>8000</port>
    </listen-socket>
</icecast>
```

**Start Icecast:**
```bash
# Ubuntu/Debian (as service)
sudo systemctl enable icecast2
sudo systemctl start icecast2

# Or run directly
icecast -c /etc/icecast2/icecast.xml

# macOS
brew services start icecast
# Or: icecast -c /usr/local/etc/icecast.xml
```

**Verify Icecast is running:**
- Open http://localhost:8000 in your browser
- You should see the Icecast status page

**Configure Intercept (optional):**

The default configuration expects Icecast on `127.0.0.1:8000` with source password `hackme` and mount point `/listen.mp3`. To change these, modify the scanner config in your API calls or update the defaults in `routes/listening_post.py`:

```python
scanner_config = {
    # ... other settings ...
    'icecast_host': '127.0.0.1',
    'icecast_port': 8000,
    'icecast_mount': '/listen.mp3',
    'icecast_source_password': 'hackme',
}
```

**Troubleshooting Icecast:**

- **"Connection refused" errors**: Ensure Icecast is running on the configured port
- **"Authentication failed"**: Check the source password matches between Icecast config and Intercept
- **No audio playing**: Check Icecast status page (http://localhost:8000) to verify the mount point is active
- **High latency**: Ensure nginx/reverse proxy isn't buffering - add `proxy_buffering off;` to nginx config

### Audio Streaming Issues - Detailed Debugging

If the Listening Post shows "Icecast mount not active" errors or audio doesn't play:

**1. Check the console output for errors**

Intercept now logs detailed error output. Look for lines starting with `[AUDIO]`:
```
[AUDIO] SDR errors: ...     # Problems with rtl_fm/rx_fm (SDR not connected, device busy)
[AUDIO] FFmpeg errors: ...  # Problems with ffmpeg (wrong password, codec issues)
```

**2. Verify SDR is connected and working**
```bash
# For RTL-SDR
rtl_test -t

# You should see: "Found 1 device(s)"
# If not, check USB connection and drivers
```

**3. Check Icecast password (macOS Homebrew)**

On macOS with Homebrew, the Icecast config is at `/opt/homebrew/etc/icecast.xml`. Check the source password:
```bash
grep source-password /opt/homebrew/etc/icecast.xml
```

If it's different from `hackme`, update it in the Listening Post Icecast config panel, or change the Icecast config and restart:
```bash
brew services restart icecast
```

**4. Verify ffmpeg has required codecs**
```bash
# Check MP3 encoder is available
ffmpeg -encoders 2>/dev/null | grep mp3

# Should show: libmp3lame
# If not, reinstall ffmpeg with all codecs:
# macOS: brew reinstall ffmpeg
# Linux: sudo apt install ffmpeg
```

**5. Test the pipeline manually**

Try running the audio pipeline directly to see errors:
```bash
# Test rtl_fm (should produce raw audio data)
rtl_fm -M am -f 118000000 -s 24000 -r 24000 -g 40 2>&1 | head -c 1000 | xxd | head

# Test ffmpeg to Icecast (replace PASSWORD with your source password)
rtl_fm -M am -f 118000000 -s 24000 -r 24000 -g 40 2>/dev/null | \
  ffmpeg -f s16le -ar 24000 -ac 1 -i pipe:0 -c:a libmp3lame -b:a 64k \
  -f mp3 -content_type audio/mpeg icecast://source:PASSWORD@127.0.0.1:8000/listen.mp3
```

**6. Common error messages and solutions**

| Error | Cause | Solution |
|-------|-------|----------|
| `No supported devices found` | SDR not connected | Plug in SDR, check USB |
| `Device or resource busy` | Another process using SDR | Click "Kill All Processes" |
| `401 Unauthorized` | Wrong Icecast password | Check password in Icecast config |
| `Connection refused` | Icecast not running | Start Icecast service |
| `Encoder libmp3lame not found` | ffmpeg missing codec | Reinstall ffmpeg with codecs |

## WiFi Issues

### Monitor mode fails

1. Ensure running as root/sudo
2. Check adapter supports monitor mode: `iw list | grep monitor`
3. Kill interfering processes: `airmon-ng check kill`

### Permission denied when scanning

Run INTERCEPT with sudo:
```bash
sudo python3 intercept.py
# Or with venv:
sudo venv/bin/python intercept.py
```

### Interface not found after enabling monitor mode

Some adapters rename when entering monitor mode (e.g., wlan0 → wlan0mon). The interface should auto-select, but if not, manually select the monitor interface from the dropdown.

## Bluetooth Issues

### No Bluetooth adapter found

```bash
# Check if adapter is detected
hciconfig

# Ubuntu/Debian - install BlueZ
sudo apt install bluez bluetooth
```

### Permission denied

Run with sudo or add your user to the bluetooth group:
```bash
sudo usermod -a -G bluetooth $USER
```

## Decoding Issues

### No messages appearing (Pager mode)

1. Verify frequency is correct for your area
2. Adjust gain (try 30-40 dB)
3. Check pager services are active in your area
4. Ensure antenna is connected

### Cannot install dump1090 in Debian (ADS-B mode)

On newer Debian versions, dump1090 may not be in repositories. The recommended action is to build from source or use the setup.sh script which will do it for you.

### No aircraft appearing (ADS-B mode)

1. Verify dump1090 is installed
2. Check antenna is connected (1090 MHz antenna recommended)
3. Ensure clear view of sky
4. Set correct observer location for range calculations or use gpsd

### Satellite passes not calculating

1. Ensure skyfield is installed: `apt install python3-skyfield`
2. Check TLE data is valid and recent
3. Verify observer location is set correctly

