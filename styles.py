from kivy.core.text import LabelBase
from kivy.utils import get_color_from_hex

# Color scheme
COLORS = {
    'primary': '#2196F3',
    'secondary': '#FFC107',
    'success': '#4CAF50',
    'error': '#F44336',
    'background': '#FFFFFF',
    'text': '#000000',
}

# Button styles
BUTTON_STYLES = {
    'normal': {
        'background_color': get_color_from_hex(COLORS['primary']),
        'color': get_color_from_hex(COLORS['background']),
        'font_size': '16sp',
        'size_hint': (1, None),
        'height': '48dp',
    },
    'secondary': {
        'background_color': get_color_from_hex(COLORS['secondary']),
        'color': get_color_from_hex(COLORS['text']),
        'font_size': '14sp',
        'size_hint': (1, None),
        'height': '40dp',
    }
}

# Label styles
LABEL_STYLES = {
    'header': {
        'font_size': '20sp',
        'color': get_color_from_hex(COLORS['text']),
        'bold': True,
    },
    'normal': {
        'font_size': '16sp',
        'color': get_color_from_hex(COLORS['text']),
    }
}
