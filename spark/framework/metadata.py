import os
import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(BASE_DIR, "..", "configs")  # resolves to .../spark/configs


def metadata(*, event_name):
    if not event_name:
        return

    loader = MetadataLoader(CONFIG_DIR)
    metadata = loader.load_yaml(event_name)

    template_name = metadata.get("template").get("template_name")
    folder_path = metadata.get("template").get("template_folder")

    template = TemplateLoader(folder_path)
    return [metadata, template.load_template(template_name)]


class MetadataLoader:
    def __init__(self, config_path):
        self.config_path = config_path

    def load_yaml(self, job_name):
        file_name = f"{job_name.lower()}.yaml"
        file_path = os.path.join(self.config_path, file_name)

        with open(file_path, "r") as file:
            config = yaml.safe_load(file)

        return config


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


if __name__ == "__main__":
    metadata(event_name="purchase_order")