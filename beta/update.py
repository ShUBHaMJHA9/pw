import json
import os

from mainLogic.utils.glv_var import debugger


class UpdateJSONFile:
    def __init__(self, file_path, debug=False):
        self.file_path = file_path
        self.data = None
        self.load()

        if debug:
            debugger.info(f"Debug Mode: Loaded data from {file_path}")
            debugger.warning(f"Debug Mode: Data: {self.data}")

    def load(self):
        if not os.path.exists(self.file_path):
            self._seed_from_defaults()
            return
        with open(self.file_path, 'r') as file:
            self.data = json.load(file)

    def _default_prefs_path(self):
        base_dir = os.path.dirname(self.file_path)
        candidates = []
        if os.name == "posix":
            candidates.append(os.path.join(base_dir, "defaults.linux.json"))
        candidates.append(os.path.join(base_dir, "defaults.json"))
        for path in candidates:
            if os.path.exists(path):
                return path
        return None

    def _seed_from_defaults(self):
        default_path = self._default_prefs_path()
        if default_path:
            with open(default_path, 'r') as file:
                self.data = json.load(file)
            debugger.info(f"Seeded preferences from {default_path}")
        else:
            self.data = {}
            debugger.error("No defaults.json found to seed preferences.")
        self.save()

    def save(self):
        with open(self.file_path, 'w+') as file:
            file.write(json.dumps(self.data, indent=4))

        # manually check if the file is saved correctly
        with open(self.file_path, 'r') as file:
            saved_data = json.load(file)
            if saved_data != self.data:
                debugger.error("Error: Data not saved correctly.")
            else:
                debugger.info("Data saved correctly.")

    def update(self, key, value, debug=False):

        if debug:
            print(f"Debug Mode: Updating {key} to {value}")

        self.data[key] = value
        self.save()
