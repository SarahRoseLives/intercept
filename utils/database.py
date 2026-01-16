"""
SQLite database utilities for persistent settings storage.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger('intercept.database')

# Database file location
DB_DIR = Path(__file__).parent.parent / 'instance'
DB_PATH = DB_DIR / 'intercept.db'

# Thread-local storage for connections
_local = threading.local()


def get_db_path() -> Path:
    """Get the database file path, creating directory if needed."""
    DB_DIR.mkdir(parents=True, exist_ok=True)
    return DB_PATH


def get_connection() -> sqlite3.Connection:
    """Get a thread-local database connection."""
    if not hasattr(_local, 'connection') or _local.connection is None:
        db_path = get_db_path()
        _local.connection = sqlite3.connect(str(db_path), check_same_thread=False)
        _local.connection.row_factory = sqlite3.Row
        # Enable foreign keys
        _local.connection.execute('PRAGMA foreign_keys = ON')
    return _local.connection


@contextmanager
def get_db():
    """Context manager for database operations."""
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def init_db() -> None:
    """Initialize the database schema."""
    db_path = get_db_path()
    logger.info(f"Initializing database at {db_path}")

    with get_db() as conn:
        # Settings table for key-value storage
        conn.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                value_type TEXT DEFAULT 'string',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Signal history table for graphs
        conn.execute('''
            CREATE TABLE IF NOT EXISTS signal_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mode TEXT NOT NULL,
                device_id TEXT NOT NULL,
                signal_strength REAL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                metadata TEXT
            )
        ''')

        # Create index for faster queries
        conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_signal_history_mode_device
            ON signal_history(mode, device_id, timestamp)
        ''')

        # Device correlation table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS device_correlations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wifi_mac TEXT,
                bt_mac TEXT,
                confidence REAL,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                metadata TEXT,
                UNIQUE(wifi_mac, bt_mac)
            )
        ''')

        # =====================================================================
        # TSCM (Technical Surveillance Countermeasures) Tables
        # =====================================================================

        # TSCM Baselines - Environment snapshots for comparison
        conn.execute('''
            CREATE TABLE IF NOT EXISTS tscm_baselines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                location TEXT,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                wifi_networks TEXT,
                bt_devices TEXT,
                rf_frequencies TEXT,
                gps_coords TEXT,
                is_active BOOLEAN DEFAULT 0
            )
        ''')

        # TSCM Sweeps - Individual sweep sessions
        conn.execute('''
            CREATE TABLE IF NOT EXISTS tscm_sweeps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                baseline_id INTEGER,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                status TEXT DEFAULT 'running',
                sweep_type TEXT,
                wifi_enabled BOOLEAN DEFAULT 1,
                bt_enabled BOOLEAN DEFAULT 1,
                rf_enabled BOOLEAN DEFAULT 1,
                results TEXT,
                anomalies TEXT,
                threats_found INTEGER DEFAULT 0,
                FOREIGN KEY (baseline_id) REFERENCES tscm_baselines(id)
            )
        ''')

        # TSCM Threats - Detected threats/anomalies
        conn.execute('''
            CREATE TABLE IF NOT EXISTS tscm_threats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sweep_id INTEGER,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                threat_type TEXT NOT NULL,
                severity TEXT DEFAULT 'medium',
                source TEXT,
                identifier TEXT,
                name TEXT,
                signal_strength INTEGER,
                frequency REAL,
                details TEXT,
                acknowledged BOOLEAN DEFAULT 0,
                notes TEXT,
                gps_coords TEXT,
                FOREIGN KEY (sweep_id) REFERENCES tscm_sweeps(id)
            )
        ''')

        # TSCM Scheduled Sweeps
        conn.execute('''
            CREATE TABLE IF NOT EXISTS tscm_schedules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                baseline_id INTEGER,
                zone_name TEXT,
                cron_expression TEXT,
                sweep_type TEXT DEFAULT 'standard',
                enabled BOOLEAN DEFAULT 1,
                last_run TIMESTAMP,
                next_run TIMESTAMP,
                notify_on_threat BOOLEAN DEFAULT 1,
                notify_email TEXT,
                FOREIGN KEY (baseline_id) REFERENCES tscm_baselines(id)
            )
        ''')

        # TSCM indexes for performance
        conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_tscm_threats_sweep
            ON tscm_threats(sweep_id)
        ''')

        conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_tscm_threats_severity
            ON tscm_threats(severity, detected_at)
        ''')

        conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_tscm_sweeps_baseline
            ON tscm_sweeps(baseline_id)
        ''')

        logger.info("Database initialized successfully")


def close_db() -> None:
    """Close the thread-local database connection."""
    if hasattr(_local, 'connection') and _local.connection is not None:
        _local.connection.close()
        _local.connection = None


# =============================================================================
# Settings Functions
# =============================================================================

def get_setting(key: str, default: Any = None) -> Any:
    """
    Get a setting value by key.

    Args:
        key: Setting key
        default: Default value if not found

    Returns:
        Setting value (auto-converted from JSON for complex types)
    """
    with get_db() as conn:
        cursor = conn.execute(
            'SELECT value, value_type FROM settings WHERE key = ?',
            (key,)
        )
        row = cursor.fetchone()

        if row is None:
            return default

        value, value_type = row['value'], row['value_type']

        # Convert based on type
        if value_type == 'json':
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return default
        elif value_type == 'int':
            return int(value)
        elif value_type == 'float':
            return float(value)
        elif value_type == 'bool':
            return value.lower() in ('true', '1', 'yes')
        else:
            return value


def set_setting(key: str, value: Any) -> None:
    """
    Set a setting value.

    Args:
        key: Setting key
        value: Setting value (will be JSON-encoded for complex types)
    """
    # Determine value type and string representation
    if isinstance(value, bool):
        value_type = 'bool'
        str_value = 'true' if value else 'false'
    elif isinstance(value, int):
        value_type = 'int'
        str_value = str(value)
    elif isinstance(value, float):
        value_type = 'float'
        str_value = str(value)
    elif isinstance(value, (dict, list)):
        value_type = 'json'
        str_value = json.dumps(value)
    else:
        value_type = 'string'
        str_value = str(value)

    with get_db() as conn:
        conn.execute('''
            INSERT INTO settings (key, value, value_type, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                value_type = excluded.value_type,
                updated_at = CURRENT_TIMESTAMP
        ''', (key, str_value, value_type))


def delete_setting(key: str) -> bool:
    """
    Delete a setting.

    Args:
        key: Setting key

    Returns:
        True if setting was deleted, False if not found
    """
    with get_db() as conn:
        cursor = conn.execute('DELETE FROM settings WHERE key = ?', (key,))
        return cursor.rowcount > 0


def get_all_settings() -> dict[str, Any]:
    """Get all settings as a dictionary."""
    with get_db() as conn:
        cursor = conn.execute('SELECT key, value, value_type FROM settings')
        settings = {}

        for row in cursor:
            key, value, value_type = row['key'], row['value'], row['value_type']

            if value_type == 'json':
                try:
                    settings[key] = json.loads(value)
                except json.JSONDecodeError:
                    settings[key] = value
            elif value_type == 'int':
                settings[key] = int(value)
            elif value_type == 'float':
                settings[key] = float(value)
            elif value_type == 'bool':
                settings[key] = value.lower() in ('true', '1', 'yes')
            else:
                settings[key] = value

        return settings


# =============================================================================
# Signal History Functions
# =============================================================================

def add_signal_reading(
    mode: str,
    device_id: str,
    signal_strength: float,
    metadata: dict | None = None
) -> None:
    """Add a signal strength reading."""
    with get_db() as conn:
        conn.execute('''
            INSERT INTO signal_history (mode, device_id, signal_strength, metadata)
            VALUES (?, ?, ?, ?)
        ''', (mode, device_id, signal_strength, json.dumps(metadata) if metadata else None))


def get_signal_history(
    mode: str,
    device_id: str,
    limit: int = 100,
    since_minutes: int = 60
) -> list[dict]:
    """
    Get signal history for a device.

    Args:
        mode: Mode (wifi, bluetooth, adsb, etc.)
        device_id: Device identifier (MAC, ICAO, etc.)
        limit: Maximum number of readings
        since_minutes: Only get readings from last N minutes

    Returns:
        List of signal readings with timestamp
    """
    with get_db() as conn:
        cursor = conn.execute('''
            SELECT signal_strength, timestamp, metadata
            FROM signal_history
            WHERE mode = ? AND device_id = ?
              AND timestamp > datetime('now', ?)
            ORDER BY timestamp DESC
            LIMIT ?
        ''', (mode, device_id, f'-{since_minutes} minutes', limit))

        results = []
        for row in cursor:
            results.append({
                'signal': row['signal_strength'],
                'timestamp': row['timestamp'],
                'metadata': json.loads(row['metadata']) if row['metadata'] else None
            })

        return list(reversed(results))  # Return in chronological order


def cleanup_old_signal_history(max_age_hours: int = 24) -> int:
    """
    Remove old signal history entries.

    Args:
        max_age_hours: Maximum age in hours

    Returns:
        Number of deleted entries
    """
    with get_db() as conn:
        cursor = conn.execute('''
            DELETE FROM signal_history
            WHERE timestamp < datetime('now', ?)
        ''', (f'-{max_age_hours} hours',))
        return cursor.rowcount


# =============================================================================
# Device Correlation Functions
# =============================================================================

def add_correlation(
    wifi_mac: str,
    bt_mac: str,
    confidence: float,
    metadata: dict | None = None
) -> None:
    """Add or update a device correlation."""
    with get_db() as conn:
        conn.execute('''
            INSERT INTO device_correlations (wifi_mac, bt_mac, confidence, metadata, last_seen)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(wifi_mac, bt_mac) DO UPDATE SET
                confidence = excluded.confidence,
                last_seen = CURRENT_TIMESTAMP,
                metadata = excluded.metadata
        ''', (wifi_mac, bt_mac, confidence, json.dumps(metadata) if metadata else None))


def get_correlations(min_confidence: float = 0.5) -> list[dict]:
    """Get all device correlations above minimum confidence."""
    with get_db() as conn:
        cursor = conn.execute('''
            SELECT wifi_mac, bt_mac, confidence, first_seen, last_seen, metadata
            FROM device_correlations
            WHERE confidence >= ?
            ORDER BY confidence DESC
        ''', (min_confidence,))

        results = []
        for row in cursor:
            results.append({
                'wifi_mac': row['wifi_mac'],
                'bt_mac': row['bt_mac'],
                'confidence': row['confidence'],
                'first_seen': row['first_seen'],
                'last_seen': row['last_seen'],
                'metadata': json.loads(row['metadata']) if row['metadata'] else None
            })

        return results


# =============================================================================
# TSCM Functions
# =============================================================================

def create_tscm_baseline(
    name: str,
    location: str | None = None,
    description: str | None = None,
    wifi_networks: list | None = None,
    bt_devices: list | None = None,
    rf_frequencies: list | None = None,
    gps_coords: dict | None = None
) -> int:
    """
    Create a new TSCM baseline.

    Returns:
        The ID of the created baseline
    """
    with get_db() as conn:
        cursor = conn.execute('''
            INSERT INTO tscm_baselines
            (name, location, description, wifi_networks, bt_devices, rf_frequencies, gps_coords)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            name,
            location,
            description,
            json.dumps(wifi_networks) if wifi_networks else None,
            json.dumps(bt_devices) if bt_devices else None,
            json.dumps(rf_frequencies) if rf_frequencies else None,
            json.dumps(gps_coords) if gps_coords else None
        ))
        return cursor.lastrowid


def get_tscm_baseline(baseline_id: int) -> dict | None:
    """Get a specific TSCM baseline by ID."""
    with get_db() as conn:
        cursor = conn.execute('''
            SELECT * FROM tscm_baselines WHERE id = ?
        ''', (baseline_id,))
        row = cursor.fetchone()

        if row is None:
            return None

        return {
            'id': row['id'],
            'name': row['name'],
            'location': row['location'],
            'description': row['description'],
            'created_at': row['created_at'],
            'wifi_networks': json.loads(row['wifi_networks']) if row['wifi_networks'] else [],
            'bt_devices': json.loads(row['bt_devices']) if row['bt_devices'] else [],
            'rf_frequencies': json.loads(row['rf_frequencies']) if row['rf_frequencies'] else [],
            'gps_coords': json.loads(row['gps_coords']) if row['gps_coords'] else None,
            'is_active': bool(row['is_active'])
        }


def get_all_tscm_baselines() -> list[dict]:
    """Get all TSCM baselines."""
    with get_db() as conn:
        cursor = conn.execute('''
            SELECT id, name, location, description, created_at, is_active
            FROM tscm_baselines
            ORDER BY created_at DESC
        ''')

        return [dict(row) for row in cursor]


def get_active_tscm_baseline() -> dict | None:
    """Get the currently active TSCM baseline."""
    with get_db() as conn:
        cursor = conn.execute('''
            SELECT * FROM tscm_baselines WHERE is_active = 1 LIMIT 1
        ''')
        row = cursor.fetchone()

        if row is None:
            return None

        return get_tscm_baseline(row['id'])


def set_active_tscm_baseline(baseline_id: int) -> bool:
    """Set a baseline as active (deactivates others)."""
    with get_db() as conn:
        # Deactivate all
        conn.execute('UPDATE tscm_baselines SET is_active = 0')
        # Activate selected
        cursor = conn.execute(
            'UPDATE tscm_baselines SET is_active = 1 WHERE id = ?',
            (baseline_id,)
        )
        return cursor.rowcount > 0


def update_tscm_baseline(
    baseline_id: int,
    wifi_networks: list | None = None,
    bt_devices: list | None = None,
    rf_frequencies: list | None = None
) -> bool:
    """Update baseline device lists."""
    updates = []
    params = []

    if wifi_networks is not None:
        updates.append('wifi_networks = ?')
        params.append(json.dumps(wifi_networks))
    if bt_devices is not None:
        updates.append('bt_devices = ?')
        params.append(json.dumps(bt_devices))
    if rf_frequencies is not None:
        updates.append('rf_frequencies = ?')
        params.append(json.dumps(rf_frequencies))

    if not updates:
        return False

    params.append(baseline_id)

    with get_db() as conn:
        cursor = conn.execute(
            f'UPDATE tscm_baselines SET {", ".join(updates)} WHERE id = ?',
            params
        )
        return cursor.rowcount > 0


def delete_tscm_baseline(baseline_id: int) -> bool:
    """Delete a TSCM baseline."""
    with get_db() as conn:
        cursor = conn.execute(
            'DELETE FROM tscm_baselines WHERE id = ?',
            (baseline_id,)
        )
        return cursor.rowcount > 0


def create_tscm_sweep(
    sweep_type: str,
    baseline_id: int | None = None,
    wifi_enabled: bool = True,
    bt_enabled: bool = True,
    rf_enabled: bool = True
) -> int:
    """
    Create a new TSCM sweep session.

    Returns:
        The ID of the created sweep
    """
    with get_db() as conn:
        cursor = conn.execute('''
            INSERT INTO tscm_sweeps
            (baseline_id, sweep_type, wifi_enabled, bt_enabled, rf_enabled)
            VALUES (?, ?, ?, ?, ?)
        ''', (baseline_id, sweep_type, wifi_enabled, bt_enabled, rf_enabled))
        return cursor.lastrowid


def update_tscm_sweep(
    sweep_id: int,
    status: str | None = None,
    results: dict | None = None,
    anomalies: list | None = None,
    threats_found: int | None = None,
    completed: bool = False
) -> bool:
    """Update a TSCM sweep."""
    updates = []
    params = []

    if status is not None:
        updates.append('status = ?')
        params.append(status)
    if results is not None:
        updates.append('results = ?')
        params.append(json.dumps(results))
    if anomalies is not None:
        updates.append('anomalies = ?')
        params.append(json.dumps(anomalies))
    if threats_found is not None:
        updates.append('threats_found = ?')
        params.append(threats_found)
    if completed:
        updates.append('completed_at = CURRENT_TIMESTAMP')

    if not updates:
        return False

    params.append(sweep_id)

    with get_db() as conn:
        cursor = conn.execute(
            f'UPDATE tscm_sweeps SET {", ".join(updates)} WHERE id = ?',
            params
        )
        return cursor.rowcount > 0


def get_tscm_sweep(sweep_id: int) -> dict | None:
    """Get a specific TSCM sweep by ID."""
    with get_db() as conn:
        cursor = conn.execute('SELECT * FROM tscm_sweeps WHERE id = ?', (sweep_id,))
        row = cursor.fetchone()

        if row is None:
            return None

        return {
            'id': row['id'],
            'baseline_id': row['baseline_id'],
            'started_at': row['started_at'],
            'completed_at': row['completed_at'],
            'status': row['status'],
            'sweep_type': row['sweep_type'],
            'wifi_enabled': bool(row['wifi_enabled']),
            'bt_enabled': bool(row['bt_enabled']),
            'rf_enabled': bool(row['rf_enabled']),
            'results': json.loads(row['results']) if row['results'] else None,
            'anomalies': json.loads(row['anomalies']) if row['anomalies'] else [],
            'threats_found': row['threats_found']
        }


def add_tscm_threat(
    sweep_id: int,
    threat_type: str,
    severity: str,
    source: str,
    identifier: str,
    name: str | None = None,
    signal_strength: int | None = None,
    frequency: float | None = None,
    details: dict | None = None,
    gps_coords: dict | None = None
) -> int:
    """
    Add a detected threat to a TSCM sweep.

    Returns:
        The ID of the created threat
    """
    with get_db() as conn:
        cursor = conn.execute('''
            INSERT INTO tscm_threats
            (sweep_id, threat_type, severity, source, identifier, name,
             signal_strength, frequency, details, gps_coords)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            sweep_id, threat_type, severity, source, identifier, name,
            signal_strength, frequency,
            json.dumps(details) if details else None,
            json.dumps(gps_coords) if gps_coords else None
        ))
        return cursor.lastrowid


def get_tscm_threats(
    sweep_id: int | None = None,
    severity: str | None = None,
    acknowledged: bool | None = None,
    limit: int = 100
) -> list[dict]:
    """Get TSCM threats with optional filters."""
    conditions = []
    params = []

    if sweep_id is not None:
        conditions.append('sweep_id = ?')
        params.append(sweep_id)
    if severity is not None:
        conditions.append('severity = ?')
        params.append(severity)
    if acknowledged is not None:
        conditions.append('acknowledged = ?')
        params.append(1 if acknowledged else 0)

    where_clause = f'WHERE {" AND ".join(conditions)}' if conditions else ''
    params.append(limit)

    with get_db() as conn:
        cursor = conn.execute(f'''
            SELECT * FROM tscm_threats
            {where_clause}
            ORDER BY detected_at DESC
            LIMIT ?
        ''', params)

        results = []
        for row in cursor:
            results.append({
                'id': row['id'],
                'sweep_id': row['sweep_id'],
                'detected_at': row['detected_at'],
                'threat_type': row['threat_type'],
                'severity': row['severity'],
                'source': row['source'],
                'identifier': row['identifier'],
                'name': row['name'],
                'signal_strength': row['signal_strength'],
                'frequency': row['frequency'],
                'details': json.loads(row['details']) if row['details'] else None,
                'acknowledged': bool(row['acknowledged']),
                'notes': row['notes'],
                'gps_coords': json.loads(row['gps_coords']) if row['gps_coords'] else None
            })

        return results


def acknowledge_tscm_threat(threat_id: int, notes: str | None = None) -> bool:
    """Acknowledge a TSCM threat."""
    with get_db() as conn:
        if notes:
            cursor = conn.execute(
                'UPDATE tscm_threats SET acknowledged = 1, notes = ? WHERE id = ?',
                (notes, threat_id)
            )
        else:
            cursor = conn.execute(
                'UPDATE tscm_threats SET acknowledged = 1 WHERE id = ?',
                (threat_id,)
            )
        return cursor.rowcount > 0


def get_tscm_threat_summary() -> dict:
    """Get summary counts of threats by severity."""
    with get_db() as conn:
        cursor = conn.execute('''
            SELECT severity, COUNT(*) as count
            FROM tscm_threats
            WHERE acknowledged = 0
            GROUP BY severity
        ''')

        summary = {'critical': 0, 'high': 0, 'medium': 0, 'low': 0, 'total': 0}
        for row in cursor:
            summary[row['severity']] = row['count']
            summary['total'] += row['count']

        return summary
