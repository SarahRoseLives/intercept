#!/usr/bin/env bash
# INTERCEPT Setup Script (best-effort installs, hard-fail verification)

# ---- Force bash even if launched with sh ----
if [ -z "${BASH_VERSION:-}" ]; then
  echo "[x] This script must be run with bash (not sh)."
  echo "    Run: bash $0"
  exec bash "$0" "$@"
fi

set -Eeuo pipefail

# Ensure admin paths are searchable (many tools live here)
export PATH="/usr/local/sbin:/usr/sbin:/sbin:/opt/homebrew/sbin:/opt/homebrew/bin:$PATH"

# ----------------------------
# Pretty output
# ----------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

info()  { echo -e "${BLUE}[*]${NC} $*"; }
ok()    { echo -e "${GREEN}[✓]${NC} $*"; }
warn()  { echo -e "${YELLOW}[!]${NC} $*"; }
fail()  { echo -e "${RED}[x]${NC} $*"; }

# ----------------------------
# Progress tracking
# ----------------------------
CURRENT_STEP=0
TOTAL_STEPS=0

progress() {
  local msg="$1"
  ((CURRENT_STEP++)) || true
  local pct=$((CURRENT_STEP * 100 / TOTAL_STEPS))
  local filled=$((pct / 5))
  local empty=$((20 - filled))
  local bar=$(printf '█%.0s' $(seq 1 $filled 2>/dev/null) || true)
  bar+=$(printf '░%.0s' $(seq 1 $empty 2>/dev/null) || true)
  echo -e "${BLUE}[${CURRENT_STEP}/${TOTAL_STEPS}]${NC} ${bar} ${pct}% - ${msg}"
}

on_error() {
  local line="$1"
  local cmd="${2:-unknown}"
  fail "Setup failed at line ${line}: ${cmd}"
  exit 1
}
trap 'on_error $LINENO "$BASH_COMMAND"' ERR

# ----------------------------
# Banner
# ----------------------------
echo -e "${BLUE}"
echo "  ___ _   _ _____ _____ ____   ____ _____ ____ _____ "
echo " |_ _| \\ | |_   _| ____|  _ \\ / ___| ____|  _ \\_   _|"
echo "  | ||  \\| | | | |  _| | |_) | |   |  _| | |_) || |  "
echo "  | || |\\  | | | | |___|  _ <| |___| |___|  __/ | |  "
echo " |___|_| \\_| |_| |_____|_| \\_\\\\____|_____|_|    |_|  "
echo -e "${NC}"
echo "INTERCEPT - Setup Script"
echo "============================================"
echo

# ----------------------------
# Helpers
# ----------------------------
cmd_exists() {
  local c="$1"
  command -v "$c" >/dev/null 2>&1 && return 0
  [[ -x "/usr/sbin/$c" || -x "/sbin/$c" || -x "/usr/local/sbin/$c" || -x "/opt/homebrew/sbin/$c" ]] && return 0
  return 1
}

have_any() {
  local c
  for c in "$@"; do
    cmd_exists "$c" && return 0
  done
  return 1
}

need_sudo() {
  if [[ "$(id -u)" -eq 0 ]]; then
    SUDO=""
    ok "Running as root"
  else
    if cmd_exists sudo; then
      SUDO="sudo"
    else
      fail "sudo is not installed and you're not root."
      echo "Either run as root or install sudo first."
      exit 1
    fi
  fi
}

detect_os() {
  if [[ "${OSTYPE:-}" == "darwin"* ]]; then
    OS="macos"
  elif [[ -f /etc/debian_version ]]; then
    OS="debian"
  else
    OS="unknown"
  fi
  info "Detected OS: ${OS}"
  [[ "$OS" != "unknown" ]] || { fail "Unsupported OS (macOS + Debian/Ubuntu only)."; exit 1; }
}

# ----------------------------
# Required tool checks (with alternates)
# ----------------------------
missing_required=()

check_required() {
  local label="$1"; shift
  local desc="$1"; shift

  if have_any "$@"; then
    ok "${label} - ${desc}"
  else
    warn "${label} - ${desc} (missing, required)"
    missing_required+=("$label")
  fi
}

check_tools() {
  info "Checking required tools..."
  missing_required=()

  echo
  info "Core SDR:"
  check_required "rtl_fm"      "RTL-SDR FM demodulator" rtl_fm
  check_required "rtl_test"    "RTL-SDR device detection" rtl_test
  check_required "rtl_tcp"     "RTL-SDR TCP server" rtl_tcp
  check_required "multimon-ng" "Pager decoder" multimon-ng
  check_required "rtl_433"     "433MHz sensor decoder" rtl_433 rtl433
  check_required "rtlamr"      "Utility meter decoder" rtlamr
  check_required "dump1090"    "ADS-B decoder" dump1090
  check_required "acarsdec"    "ACARS decoder" acarsdec

  echo
  info "GPS:"
  check_required "gpsd" "GPS daemon" gpsd

  echo
  info "Audio:"
  check_required "ffmpeg" "Audio encoder/decoder" ffmpeg

  echo
  info "WiFi:"
  check_required "airmon-ng"     "Monitor mode helper" airmon-ng
  check_required "airodump-ng"   "WiFi scanner" airodump-ng
  check_required "aireplay-ng"   "Injection/deauth" aireplay-ng
  check_required "hcxdumptool"   "PMKID capture" hcxdumptool
  check_required "hcxpcapngtool" "PMKID/pcapng conversion" hcxpcapngtool

  echo
  info "Bluetooth:"
  check_required "bluetoothctl" "Bluetooth controller CLI" bluetoothctl
  check_required "hcitool"      "Bluetooth scan utility" hcitool
  check_required "hciconfig"    "Bluetooth adapter config" hciconfig

  echo
  info "SoapySDR:"
  check_required "SoapySDRUtil" "SoapySDR CLI utility" SoapySDRUtil
  echo
}

# ----------------------------
# Python venv + deps
# ----------------------------
check_python_version() {
  if ! cmd_exists python3; then
    fail "python3 not found."
    [[ "$OS" == "macos" ]] && echo "Install with: brew install python"
    [[ "$OS" == "debian" ]] && echo "Install with: sudo apt-get install python3"
    exit 1
  fi

  local ver
  ver="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
  info "Python version: ${ver}"

  python3 - <<'PY'
import sys
raise SystemExit(0 if sys.version_info >= (3,9) else 1)
PY
  ok "Python version OK (>= 3.9)"
}

install_python_deps() {
  progress "Setting up Python environment"
  check_python_version

  if [[ ! -f requirements.txt ]]; then
    warn "requirements.txt not found; skipping Python dependency install."
    return 0
  fi

  # On Debian/Ubuntu, try apt packages first as they're more reliable
  if [[ "$OS" == "debian" ]]; then
    info "Installing Python packages via apt (more reliable on Debian/Ubuntu)..."
    $SUDO apt-get install -y python3-flask python3-requests python3-serial >/dev/null 2>&1 || true

    # skyfield may not be available in all distros, try apt first then pip
    if ! $SUDO apt-get install -y python3-skyfield >/dev/null 2>&1; then
      warn "python3-skyfield not in apt, will try pip later"
    fi
    ok "Installed available Python packages via apt"
  fi

  if [[ ! -d venv ]]; then
    python3 -m venv --system-site-packages venv
    ok "Created venv/ (with system site-packages)"
  else
    ok "Using existing venv/"
  fi

  # shellcheck disable=SC1091
  source venv/bin/activate

  python -m pip install --upgrade pip setuptools wheel >/dev/null 2>&1 || true
  ok "Upgraded pip tooling"

  progress "Installing Python dependencies"
  # Try pip install, but don't fail if apt packages already satisfied deps
  if ! python -m pip install -r requirements.txt 2>/dev/null; then
    warn "Some pip packages failed - checking if apt packages cover them..."
    # Verify critical packages are available
    python -c "import flask; import requests; from flask_limiter import Limiter" 2>/dev/null || {
      fail "Critical Python packages (flask, requests, flask-limiter) not installed"
      echo "Try: pip install flask requests flask-limiter"
      exit 1
    }
    ok "Core Python dependencies available"
  else
    ok "Python dependencies installed"
  fi
  echo
}

# ----------------------------
# macOS install (Homebrew)
# ----------------------------
ensure_brew() {
  cmd_exists brew && return 0
  warn "Homebrew not found. Installing Homebrew..."
  /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

  if [[ -x /opt/homebrew/bin/brew ]]; then
    eval "$(/opt/homebrew/bin/brew shellenv)"
  elif [[ -x /usr/local/bin/brew ]]; then
    eval "$(/usr/local/bin/brew shellenv)"
  fi

  cmd_exists brew || { fail "Homebrew install failed. Install manually then re-run."; exit 1; }
}

brew_install() {
  local pkg="$1"
  if brew list --formula "$pkg" >/dev/null 2>&1; then
    ok "brew: ${pkg} already installed"
    return 0
  fi
  info "brew: installing ${pkg}..."
  if brew install "$pkg" 2>&1; then
    ok "brew: installed ${pkg}"
    return 0
  else
    return 1
  fi
}

install_multimon_ng_from_source_macos() {
  info "multimon-ng not available via Homebrew. Building from source..."

  # Ensure build dependencies are installed
  brew_install cmake
  brew_install libsndfile

  (
    tmp_dir="$(mktemp -d)"
    trap 'rm -rf "$tmp_dir"' EXIT

    info "Cloning multimon-ng..."
    git clone --depth 1 https://github.com/EliasOenal/multimon-ng.git "$tmp_dir/multimon-ng" >/dev/null 2>&1 \
      || { fail "Failed to clone multimon-ng"; exit 1; }

    cd "$tmp_dir/multimon-ng"
    info "Compiling multimon-ng..."
    mkdir -p build && cd build
    cmake .. >/dev/null 2>&1 || { fail "cmake failed for multimon-ng"; exit 1; }
    make >/dev/null 2>&1 || { fail "make failed for multimon-ng"; exit 1; }

    # Install to /usr/local/bin (no sudo needed on Homebrew systems typically)
    if [[ -w /usr/local/bin ]]; then
      install -m 0755 multimon-ng /usr/local/bin/multimon-ng
    else
      sudo install -m 0755 multimon-ng /usr/local/bin/multimon-ng
    fi
    ok "multimon-ng installed successfully from source"
  )
}

install_macos_packages() {
  TOTAL_STEPS=13
  CURRENT_STEP=0

  progress "Checking Homebrew"
  ensure_brew

  progress "Installing RTL-SDR libraries"
  brew_install librtlsdr

  progress "Installing multimon-ng"
  # multimon-ng is not in Homebrew core, so build from source
  if ! cmd_exists multimon-ng; then
    install_multimon_ng_from_source_macos
  else
    ok "multimon-ng already installed"
  fi

  progress "Installing direwolf (APRS decoder)"
  (brew_install direwolf) || warn "direwolf not available via Homebrew"

  progress "Installing ffmpeg"
  brew_install ffmpeg

  progress "Installing rtl_433"
  brew_install rtl_433

  progress "Installing rtlamr"
  # rtlamr needs to be installed via go or binary
  if ! cmd_exists rtlamr; then
    if [[ -f "/home/rose/Compiled/rtlamr/rtlamr" ]]; then
      info "Found rtlamr binary, linking to /usr/local/bin..."
      if [[ -w /usr/local/bin ]]; then
        ln -sf /home/rose/Compiled/rtlamr/rtlamr /usr/local/bin/rtlamr
      else
        sudo ln -sf /home/rose/Compiled/rtlamr/rtlamr /usr/local/bin/rtlamr
      fi
      ok "rtlamr linked successfully"
    else
      warn "rtlamr not found. Download from https://github.com/bemasher/rtlamr"
    fi
  else
    ok "rtlamr already installed"
  fi

  progress "Installing dump1090"
  (brew_install dump1090-mutability) || warn "dump1090 not available via Homebrew"

  progress "Installing acarsdec"
  (brew_install acarsdec) || warn "acarsdec not available via Homebrew"

  progress "Installing aircrack-ng"
  brew_install aircrack-ng

  progress "Installing hcxtools"
  brew_install hcxtools

  progress "Installing SoapySDR"
  brew_install soapysdr

  progress "Installing gpsd"
  brew_install gpsd

  warn "macOS note: hcitool/hciconfig are Linux (BlueZ) utilities and often unavailable on macOS."
  info "TSCM BLE scanning uses bleak library (installed via pip) for manufacturer data detection."
  echo
}

# ----------------------------
# Debian/Ubuntu install (APT)
# ----------------------------
apt_install() {
  local pkgs="$*"
  local output
  local ret=0
  output=$($SUDO apt-get install -y --no-install-recommends "$@" 2>&1) || ret=$?
  if [[ $ret -ne 0 ]]; then
    fail "Failed to install: $pkgs"
    echo "$output" | tail -10
    fail "Try running: sudo apt-get update && sudo apt-get install -y $pkgs"
    return 1
  fi
}

apt_try_install_any() {
  local p
  for p in "$@"; do
    if $SUDO apt-get install -y --no-install-recommends "$p" >/dev/null 2>&1; then
      ok "apt: installed ${p}"
      return 0
    fi
  done
  return 1
}

install_dump1090_from_source_debian() {
  info "dump1090 not available via APT. Building from source (required)..."

  apt_install build-essential git pkg-config \
    librtlsdr-dev libusb-1.0-0-dev \
    libncurses-dev tcl-dev python3-dev

  # Run in subshell to isolate EXIT trap
  (
    tmp_dir="$(mktemp -d)"
    trap 'rm -rf "$tmp_dir"' EXIT

    info "Cloning FlightAware dump1090..."
    git clone --depth 1 https://github.com/flightaware/dump1090.git "$tmp_dir/dump1090" >/dev/null 2>&1 \
      || { fail "Failed to clone FlightAware dump1090"; exit 1; }

    cd "$tmp_dir/dump1090"
    info "Compiling FlightAware dump1090..."
    if make BLADERF=no RTLSDR=yes >/dev/null 2>&1; then
      $SUDO install -m 0755 dump1090 /usr/local/bin/dump1090
      ok "dump1090 installed successfully (FlightAware)."
      exit 0
    fi

    warn "FlightAware build failed. Falling back to antirez/dump1090..."
    rm -rf "$tmp_dir/dump1090"
    git clone --depth 1 https://github.com/antirez/dump1090.git "$tmp_dir/dump1090" >/dev/null 2>&1 \
      || { fail "Failed to clone antirez dump1090"; exit 1; }

    cd "$tmp_dir/dump1090"
    info "Compiling antirez dump1090..."
    make >/dev/null 2>&1 || { fail "Failed to build dump1090 from source (required)."; exit 1; }

    $SUDO install -m 0755 dump1090 /usr/local/bin/dump1090
    ok "dump1090 installed successfully (antirez)."
  )
}

install_acarsdec_from_source_debian() {
  info "acarsdec not available via APT. Building from source..."

  apt_install build-essential git cmake \
    librtlsdr-dev libusb-1.0-0-dev libsndfile1-dev

  # Run in subshell to isolate EXIT trap
  (
    tmp_dir="$(mktemp -d)"
    trap 'rm -rf "$tmp_dir"' EXIT

    info "Cloning acarsdec..."
    git clone --depth 1 https://github.com/TLeconte/acarsdec.git "$tmp_dir/acarsdec" >/dev/null 2>&1 \
      || { warn "Failed to clone acarsdec"; exit 1; }

    cd "$tmp_dir/acarsdec"
    mkdir -p build && cd build

    info "Compiling acarsdec..."
    if cmake .. -Drtl=ON >/dev/null 2>&1 && make >/dev/null 2>&1; then
      $SUDO install -m 0755 acarsdec /usr/local/bin/acarsdec
      ok "acarsdec installed successfully."
    else
      warn "Failed to build acarsdec from source. ACARS decoding will not be available."
    fi
  )
}

install_rtlsdr_blog_drivers_debian() {
  # The RTL-SDR Blog drivers provide better support for:
  # - RTL-SDR Blog V4 (R828D tuner)
  # - RTL-SDR Blog V3 with bias-t improvements
  # - Better overall compatibility with all RTL-SDR devices
  # These drivers are backward compatible with standard RTL-SDR devices.

  info "Installing RTL-SDR Blog drivers (improved V4 support)..."

  # Install build dependencies
  apt_install build-essential git cmake libusb-1.0-0-dev pkg-config

  # Run in subshell to isolate EXIT trap
  (
    tmp_dir="$(mktemp -d)"
    trap 'rm -rf "$tmp_dir"' EXIT

    info "Cloning RTL-SDR Blog driver fork..."
    git clone https://github.com/rtlsdrblog/rtl-sdr-blog.git "$tmp_dir/rtl-sdr-blog" >/dev/null 2>&1 \
      || { warn "Failed to clone RTL-SDR Blog drivers"; exit 1; }

    cd "$tmp_dir/rtl-sdr-blog"
    mkdir -p build && cd build

    info "Compiling RTL-SDR Blog drivers..."
    if cmake .. -DINSTALL_UDEV_RULES=ON -DDETACH_KERNEL_DRIVER=ON >/dev/null 2>&1 && make >/dev/null 2>&1; then
      $SUDO make install >/dev/null 2>&1
      $SUDO ldconfig

      # Copy udev rules if they exist
      if [[ -f ../rtl-sdr.rules ]]; then
        $SUDO cp ../rtl-sdr.rules /etc/udev/rules.d/20-rtlsdr-blog.rules
        $SUDO udevadm control --reload-rules || true
        $SUDO udevadm trigger || true
      fi

      ok "RTL-SDR Blog drivers installed successfully."
      info "These drivers provide improved support for RTL-SDR Blog V4 and other devices."
      warn "Unplug and replug your RTL-SDR devices for the new drivers to take effect."
    else
      warn "Failed to build RTL-SDR Blog drivers. Using stock drivers."
      warn "If you have an RTL-SDR Blog V4, you may need to install drivers manually."
      warn "See: https://github.com/rtlsdrblog/rtl-sdr-blog"
    fi
  )
}

setup_udev_rules_debian() {
  [[ -d /etc/udev/rules.d ]] || { warn "udev not found; skipping RTL-SDR udev rules."; return 0; }

  local rules_file="/etc/udev/rules.d/20-rtlsdr.rules"
  [[ -f "$rules_file" ]] && { ok "RTL-SDR udev rules already present: $rules_file"; return 0; }

  info "Installing RTL-SDR udev rules..."
  $SUDO tee "$rules_file" >/dev/null <<'EOF'
SUBSYSTEM=="usb", ATTRS{idVendor}=="0bda", ATTRS{idProduct}=="2838", MODE="0666"
SUBSYSTEM=="usb", ATTRS{idVendor}=="0bda", ATTRS{idProduct}=="2832", MODE="0666"
EOF
  $SUDO udevadm control --reload-rules || true
  $SUDO udevadm trigger || true
  ok "udev rules installed. Unplug/replug your RTL-SDR if connected."
  echo
}

blacklist_kernel_drivers_debian() {
  local blacklist_file="/etc/modprobe.d/blacklist-rtlsdr.conf"

  if [[ -f "$blacklist_file" ]]; then
    ok "RTL-SDR kernel driver blacklist already present"
    return 0
  fi

  info "Blacklisting conflicting DVB kernel drivers..."
  $SUDO tee "$blacklist_file" >/dev/null <<'EOF'
# Blacklist DVB-T drivers to allow rtl-sdr to access RTL2832U devices
blacklist dvb_usb_rtl28xxu
blacklist rtl2832
blacklist rtl2830
blacklist r820t
EOF

  # Unload modules if currently loaded
  for mod in dvb_usb_rtl28xxu rtl2832 rtl2830 r820t; do
    if lsmod | grep -q "^$mod"; then
      $SUDO modprobe -r "$mod" 2>/dev/null || true
    fi
  done

  ok "Kernel drivers blacklisted. Unplug/replug your RTL-SDR if connected."
  echo
}

install_debian_packages() {
  need_sudo

  # Suppress needrestart prompts (Ubuntu Server 22.04+)
  export DEBIAN_FRONTEND=noninteractive
  export NEEDRESTART_MODE=a

  TOTAL_STEPS=18
  CURRENT_STEP=0

  progress "Updating APT package lists"
  $SUDO apt-get update -y >/dev/null

  progress "Installing RTL-SDR"
  # Handle package conflict between librtlsdr0 and librtlsdr2
  # The newer librtlsdr0 (2.0.2) conflicts with older librtlsdr2 (2.0.1)
  if dpkg -l | grep -q "librtlsdr2"; then
    info "Detected librtlsdr2 conflict - upgrading to librtlsdr0..."

    # Remove packages that depend on librtlsdr2, then remove librtlsdr2
    # These will be reinstalled with librtlsdr0 support
    $SUDO apt-get remove -y dump1090-mutability libgnuradio-osmosdr0.2.0t64 rtl-433 librtlsdr2 rtl-sdr 2>/dev/null || true
    $SUDO apt-get autoremove -y 2>/dev/null || true

    ok "Removed conflicting librtlsdr2 packages"
  fi

  # If rtl-sdr is in broken state, remove it completely first
  if dpkg -l | grep -q "^.[^i].*rtl-sdr" || ! dpkg -l rtl-sdr 2>/dev/null | grep -q "^ii"; then
    info "Removing broken rtl-sdr package..."
    $SUDO dpkg --remove --force-remove-reinstreq rtl-sdr 2>/dev/null || true
    $SUDO dpkg --purge --force-remove-reinstreq rtl-sdr 2>/dev/null || true
  fi

  # Force remove librtlsdr2 if it still exists
  if dpkg -l | grep -q "librtlsdr2"; then
    info "Force removing librtlsdr2..."
    $SUDO dpkg --remove --force-all librtlsdr2 2>/dev/null || true
    $SUDO dpkg --purge --force-all librtlsdr2 2>/dev/null || true
  fi

  # Clean up any partial installations
  $SUDO dpkg --configure -a 2>/dev/null || true
  $SUDO apt-get --fix-broken install -y 2>/dev/null || true

  apt_install rtl-sdr

  progress "Installing RTL-SDR Blog drivers (V4 support)"
  install_rtlsdr_blog_drivers_debian

  progress "Installing multimon-ng"
  apt_install multimon-ng

  progress "Installing direwolf (APRS decoder)"
  apt_install direwolf || true

  progress "Installing ffmpeg"
  apt_install ffmpeg

  progress "Installing rtl_433"
  apt_try_install_any rtl-433 rtl433 || warn "rtl-433 not available"

  progress "Installing rtlamr"
  # rtlamr needs to be installed via go or binary
  if ! cmd_exists rtlamr; then
    if [[ -f "/home/rose/Compiled/rtlamr/rtlamr" ]]; then
      info "Found rtlamr binary, installing to /usr/local/bin..."
      $SUDO install -m 0755 /home/rose/Compiled/rtlamr/rtlamr /usr/local/bin/rtlamr
      ok "rtlamr installed successfully"
    else
      warn "rtlamr not found. Download from https://github.com/bemasher/rtlamr"
    fi
  else
    ok "rtlamr already installed"
  fi

  progress "Installing aircrack-ng"
  apt_install aircrack-ng || true

  progress "Installing hcxdumptool"
  apt_install hcxdumptool || true

  progress "Installing hcxtools"
  apt_install hcxtools || true

  progress "Installing Bluetooth tools"
  apt_install bluez bluetooth || true

  progress "Installing SoapySDR"
  # Exclude xtrx-dkms - its kernel module fails to build on newer kernels (6.14+)
  # and causes apt to hang. Most users don't have XTRX hardware anyway.
  apt_install soapysdr-tools xtrx-dkms- || true

  progress "Installing gpsd"
  apt_install gpsd gpsd-clients || true

  progress "Installing Python packages"
  apt_install python3-venv python3-pip || true
  # Install Python packages via apt (more reliable than pip on modern Debian/Ubuntu)
  $SUDO apt-get install -y python3-flask python3-requests python3-serial >/dev/null 2>&1 || true
  $SUDO apt-get install -y python3-skyfield >/dev/null 2>&1 || true
  # bleak for BLE scanning with manufacturer data (TSCM mode)
  $SUDO apt-get install -y python3-bleak >/dev/null 2>&1 || true

  progress "Installing dump1090"
  if ! cmd_exists dump1090 && ! cmd_exists dump1090-mutability; then
    apt_try_install_any dump1090-fa dump1090-mutability dump1090 || true
  fi
  if ! cmd_exists dump1090; then
    if cmd_exists dump1090-mutability; then
      $SUDO ln -s $(which dump1090-mutability) /usr/local/sbin/dump1090
    fi
  fi
  cmd_exists dump1090 || install_dump1090_from_source_debian

  progress "Installing acarsdec"
  if ! cmd_exists acarsdec; then
    apt_install acarsdec || true
  fi
  cmd_exists acarsdec || install_acarsdec_from_source_debian

  progress "Configuring udev rules"
  setup_udev_rules_debian

  progress "Blacklisting conflicting kernel drivers"
  blacklist_kernel_drivers_debian
}

# ----------------------------
# Final summary / hard fail
# ----------------------------
final_summary_and_hard_fail() {
  check_tools

  echo "============================================"
  echo
  echo "To start INTERCEPT:"
  echo "  sudo -E venv/bin/python intercept.py"
  echo
  echo "Then open http://localhost:5050 in your browser"
  echo
  echo "============================================"

  if [[ "${#missing_required[@]}" -eq 0 ]]; then
    ok "All REQUIRED tools are installed."
  else
    fail "Missing REQUIRED tools:"
    for t in "${missing_required[@]}"; do echo "  - $t"; done
    echo
    if [[ "$OS" == "macos" ]]; then
      warn "macOS note: bluetoothctl/hcitool/hciconfig are Linux (BlueZ) tools and unavailable on macOS."
      warn "Bluetooth functionality will be limited. Other features should work."
    else
      fail "Exiting because required tools are missing."
      exit 1
    fi
  fi
}

# ----------------------------
# MAIN
# ----------------------------
main() {
  detect_os

  if [[ "$OS" == "macos" ]]; then
    install_macos_packages
  else
    install_debian_packages
  fi

  install_python_deps
  final_summary_and_hard_fail
}

main "$@"

