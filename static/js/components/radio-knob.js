/**
 * Intercept - Radio Knob Component
 * Interactive rotary knob control with drag-to-rotate
 */

class RadioKnob {
    constructor(element, options = {}) {
        this.element = element;
        this.value = parseFloat(element.dataset.value) || 0;
        this.min = parseFloat(element.dataset.min) || 0;
        this.max = parseFloat(element.dataset.max) || 100;
        this.step = parseFloat(element.dataset.step) || 1;
        this.rotation = this.valueToRotation(this.value);
        this.isDragging = false;
        this.startY = 0;
        this.startRotation = 0;
        this.sensitivity = options.sensitivity || 1.5;
        this.onChange = options.onChange || null;

        this.bindEvents();
        this.updateVisual();
    }

    valueToRotation(value) {
        const range = this.max - this.min;
        const normalized = (value - this.min) / range;
        return normalized * 270 - 135; // -135 to +135 degrees
    }

    rotationToValue(rotation) {
        const normalized = (rotation + 135) / 270;
        let value = this.min + normalized * (this.max - this.min);

        // Snap to step
        value = Math.round(value / this.step) * this.step;
        return Math.max(this.min, Math.min(this.max, value));
    }

    bindEvents() {
        // Mouse events
        this.element.addEventListener('mousedown', (e) => this.startDrag(e));
        document.addEventListener('mousemove', (e) => this.drag(e));
        document.addEventListener('mouseup', () => this.endDrag());

        // Touch support
        this.element.addEventListener('touchstart', (e) => {
            e.preventDefault();
            this.startDrag(e.touches[0]);
        }, { passive: false });
        document.addEventListener('touchmove', (e) => {
            if (this.isDragging) {
                e.preventDefault();
                this.drag(e.touches[0]);
            }
        }, { passive: false });
        document.addEventListener('touchend', () => this.endDrag());

        // Scroll wheel support
        this.element.addEventListener('wheel', (e) => this.handleWheel(e), { passive: false });

        // Double-click to reset
        this.element.addEventListener('dblclick', () => this.reset());
    }

    startDrag(e) {
        this.isDragging = true;
        this.startY = e.clientY;
        this.startRotation = this.rotation;
        this.element.style.cursor = 'grabbing';
        this.element.classList.add('active');

        // Play click sound if available
        if (typeof playClickSound === 'function') {
            playClickSound();
        }
    }

    drag(e) {
        if (!this.isDragging) return;

        const deltaY = this.startY - e.clientY;
        let newRotation = this.startRotation + deltaY * this.sensitivity;

        // Clamp rotation
        newRotation = Math.max(-135, Math.min(135, newRotation));

        this.rotation = newRotation;
        this.value = this.rotationToValue(this.rotation);
        this.updateVisual();
        this.dispatchChange();
    }

    endDrag() {
        if (!this.isDragging) return;
        this.isDragging = false;
        this.element.style.cursor = 'grab';
        this.element.classList.remove('active');
    }

    handleWheel(e) {
        e.preventDefault();
        const delta = e.deltaY > 0 ? -this.step : this.step;
        const multiplier = e.shiftKey ? 5 : 1; // Faster with shift key
        this.setValue(this.value + delta * multiplier);

        // Play click sound if available
        if (typeof playClickSound === 'function') {
            playClickSound();
        }
    }

    setValue(value, silent = false) {
        this.value = Math.max(this.min, Math.min(this.max, value));
        this.rotation = this.valueToRotation(this.value);
        this.updateVisual();
        if (!silent) {
            this.dispatchChange();
        }
    }

    getValue() {
        return this.value;
    }

    reset() {
        const defaultValue = parseFloat(this.element.dataset.default) ||
            (this.min + this.max) / 2;
        this.setValue(defaultValue);
    }

    updateVisual() {
        this.element.style.transform = `rotate(${this.rotation}deg)`;

        // Update associated value display
        const valueDisplayId = this.element.id.replace('Knob', 'Value');
        const valueDisplay = document.getElementById(valueDisplayId);
        if (valueDisplay) {
            valueDisplay.textContent = Math.round(this.value);
        }

        // Update data attribute
        this.element.dataset.value = this.value;
    }

    dispatchChange() {
        // Custom callback
        if (this.onChange) {
            this.onChange(this.value, this);
        }

        // Custom event
        this.element.dispatchEvent(new CustomEvent('knobchange', {
            detail: { value: this.value, knob: this },
            bubbles: true
        }));
    }
}

/**
 * Tuning Dial - Larger rotary control for frequency tuning
 */
class TuningDial extends RadioKnob {
    constructor(element, options = {}) {
        super(element, {
            sensitivity: options.sensitivity || 0.8,
            ...options
        });

        this.fineStep = options.fineStep || 0.025;
        this.coarseStep = options.coarseStep || 0.2;
    }

    handleWheel(e) {
        e.preventDefault();
        const step = e.shiftKey ? this.fineStep : this.coarseStep;
        const delta = e.deltaY > 0 ? -step : step;
        this.setValue(this.value + delta);
    }

    // Override to not round to step for smooth tuning
    rotationToValue(rotation) {
        const normalized = (rotation + 135) / 270;
        let value = this.min + normalized * (this.max - this.min);
        return Math.max(this.min, Math.min(this.max, value));
    }

    updateVisual() {
        this.element.style.transform = `rotate(${this.rotation}deg)`;

        // Update associated value display with decimals
        const valueDisplayId = this.element.id.replace('Dial', 'Value');
        const valueDisplay = document.getElementById(valueDisplayId);
        if (valueDisplay) {
            valueDisplay.textContent = this.value.toFixed(3);
        }

        this.element.dataset.value = this.value;
    }
}

/**
 * Initialize all radio knobs on the page
 */
function initRadioKnobs() {
    // Initialize standard knobs
    document.querySelectorAll('.radio-knob').forEach(element => {
        if (!element._knob) {
            element._knob = new RadioKnob(element);
        }
    });

    // Initialize tuning dials
    document.querySelectorAll('.tuning-dial').forEach(element => {
        if (!element._dial) {
            element._dial = new TuningDial(element);
        }
    });
}

// Auto-initialize on DOM ready
document.addEventListener('DOMContentLoaded', initRadioKnobs);

// Export for use in modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { RadioKnob, TuningDial, initRadioKnobs };
}
