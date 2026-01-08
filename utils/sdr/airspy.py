"""
Airspy command builder implementation.

Uses SoapySDR-based tools for FM demodulation and signal capture.
Airspy R2/Mini supports 24 MHz to 1.8 GHz frequency range.
Airspy HF+ supports 9 kHz - 31 MHz and 60-260 MHz.
"""

from __future__ import annotations

from typing import Optional

from .base import CommandBuilder, SDRCapabilities, SDRDevice, SDRType


class AirspyCommandBuilder(CommandBuilder):
    """Airspy command builder using SoapySDR tools."""

    # Airspy R2/Mini capabilities (most common)
    # HF+ has different range but same interface
    CAPABILITIES = SDRCapabilities(
        sdr_type=SDRType.AIRSPY,
        freq_min_mhz=24.0,       # 24 MHz (HF+ goes lower)
        freq_max_mhz=1800.0,     # 1.8 GHz
        gain_min=0.0,
        gain_max=45.0,           # LNA (0-15) + Mixer (0-15) + VGA (0-15)
        sample_rates=[2500000, 3000000, 6000000, 10000000],
        supports_bias_t=True,
        supports_ppm=False,      # Airspy has TCXO, no PPM needed
        tx_capable=False
    )

    def _build_device_string(self, device: SDRDevice) -> str:
        """Build SoapySDR device string for Airspy."""
        driver = device.driver if device.driver in ('airspy', 'airspyhf') else 'airspy'
        if device.serial and device.serial != 'N/A':
            return f'driver={driver},serial={device.serial}'
        return f'driver={driver}'

    def _format_gain(self, gain: float) -> str:
        """
        Format gain string for Airspy.

        Airspy has three gain stages:
        - LNA: 0-15 dB
        - Mixer: 0-15 dB
        - VGA: 0-15 dB

        This distributes the requested gain across stages.
        """
        if gain <= 15:
            return f'LNA={int(gain)},MIX=0,VGA=0'
        elif gain <= 30:
            return f'LNA=15,MIX={int(gain - 15)},VGA=0'
        else:
            vga = min(15, int(gain - 30))
            return f'LNA=15,MIX=15,VGA={vga}'

    def build_fm_demod_command(
        self,
        device: SDRDevice,
        frequency_mhz: float,
        sample_rate: int = 22050,
        gain: Optional[float] = None,
        ppm: Optional[int] = None,
        modulation: str = "fm",
        squelch: Optional[int] = None,
        bias_t: bool = False
    ) -> list[str]:
        """
        Build SoapySDR rx_fm command for FM demodulation.

        For pager decoding with Airspy.
        """
        device_str = self._build_device_string(device)

        cmd = [
            'rx_fm',
            '-d', device_str,
            '-f', f'{frequency_mhz}M',
            '-M', modulation,
            '-s', str(sample_rate),
        ]

        if gain is not None and gain > 0:
            cmd.extend(['-g', self._format_gain(gain)])

        if squelch is not None and squelch > 0:
            cmd.extend(['-l', str(squelch)])

        if bias_t:
            cmd.extend(['-T'])

        # Output to stdout
        cmd.append('-')

        return cmd

    def build_adsb_command(
        self,
        device: SDRDevice,
        gain: Optional[float] = None
    ) -> list[str]:
        """
        Build dump1090/readsb command with SoapySDR support for ADS-B decoding.

        Uses readsb which has better SoapySDR support.
        """
        device_str = self._build_device_string(device)

        cmd = [
            'readsb',
            '--net',
            '--device-type', 'soapysdr',
            '--device', device_str,
            '--quiet'
        ]

        if gain is not None:
            cmd.extend(['--gain', str(int(gain))])

        return cmd

    def build_ism_command(
        self,
        device: SDRDevice,
        frequency_mhz: float = 433.92,
        gain: Optional[float] = None,
        ppm: Optional[int] = None
    ) -> list[str]:
        """
        Build rtl_433 command with SoapySDR support for ISM band decoding.

        rtl_433 has native SoapySDR support via -d flag.
        """
        device_str = self._build_device_string(device)

        cmd = [
            'rtl_433',
            '-d', device_str,
            '-f', f'{frequency_mhz}M',
            '-F', 'json'
        ]

        if gain is not None and gain > 0:
            cmd.extend(['-g', str(int(gain))])

        return cmd

    def get_capabilities(self) -> SDRCapabilities:
        """Return Airspy capabilities."""
        return self.CAPABILITIES

    @classmethod
    def get_sdr_type(cls) -> SDRType:
        """Return SDR type."""
        return SDRType.AIRSPY
