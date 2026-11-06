import os
import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.normpath(os.path.join(BASE_DIR, "..", "configs"))
TEMPLATE_DIR = os.path.normpath(os.path.join(BASE_DIR, "..", "templates"))


def _resolve_path(path):
    if not path:
        return path
    if os.path.isabs(path):
        return path
    return os.path.normpath(os.path.join(BASE_DIR, path))


def metadata(*, event_name):
    if not event_name:
        return

    loader = MetadataLoader(CONFIG_DIR)
    meta = loader.load_yaml(event_name)

    template_name = meta.get("template").get("template_name")
    template_folder = _resolve_path(meta.get("template").get("template_folder", TEMPLATE_DIR))

    template = TemplateLoader(template_folder)
    return [meta, template.load_template(template_name)]


class MetadataLoader:
    def __init__(self, config_path):
        self.config_path = config_path

    def load_yaml(self, job_name):
        file_name = f"{job_name.lower()}.yaml"
        file_path = os.path.join(self.config_path, file_name)
        with open(file_path, "r") as file:
            return yaml.safe_load(file)


class TemplateLoader:
    def __init__(self, template_path):
        self.template_path = template_path

    def load_template(self, file_name):
        env = Environment(
            loader=FileSystemLoader(str(self.template_path)),
            undefined=StrictUndefined,
            autoescape=False,
        )
        return env.get_template(file_name).render()