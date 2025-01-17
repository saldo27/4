from kivy.uix.popup import Popup
from kivy.uix.label import Label

class ErrorPopup(Popup):
    def __init__(self, message, **kwargs):
        super().__init__(**kwargs)
        self.title = 'Error'
        self.size_hint = (0.8, 0.4)
        self.content = Label(text=message)
        self.auto_dismiss = True
