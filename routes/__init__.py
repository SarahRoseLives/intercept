# Routes package - registers all blueprints with the Flask app

def register_blueprints(app):
    """Register all route blueprints with the Flask app."""
    from .pager import pager_bp
    from .sensor import sensor_bp
    from .wifi import wifi_bp
    from .bluetooth import bluetooth_bp
    from .adsb import adsb_bp
    from .acars import acars_bp
    from .aprs import aprs_bp
    from .satellite import satellite_bp
    from .gps import gps_bp
    from .settings import settings_bp
    from .correlation import correlation_bp
    from .listening_post import listening_post_bp
    from .tscm import tscm_bp, init_tscm_state

    app.register_blueprint(pager_bp)
    app.register_blueprint(sensor_bp)
    app.register_blueprint(wifi_bp)
    app.register_blueprint(bluetooth_bp)
    app.register_blueprint(adsb_bp)
    app.register_blueprint(acars_bp)
    app.register_blueprint(aprs_bp)
    app.register_blueprint(satellite_bp)
    app.register_blueprint(gps_bp)
    app.register_blueprint(settings_bp)
    app.register_blueprint(correlation_bp)
    app.register_blueprint(listening_post_bp)
    app.register_blueprint(tscm_bp)

    # Initialize TSCM state with queue and lock from app
    import app as app_module
    if hasattr(app_module, 'tscm_queue') and hasattr(app_module, 'tscm_lock'):
        init_tscm_state(app_module.tscm_queue, app_module.tscm_lock)
