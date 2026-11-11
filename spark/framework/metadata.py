from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined


SPARK_DIR = Path(__file__).resolve().parents[1]
CONFIG_DIR = SPARK_DIR / "configs"
TEMPLATE_DIR = SPARK_DIR / "templates"


class MetadataLoader:
    def __init__(self, config_path=CONFIG_DIR):
        self.config_path = Path(config_path)

    def load_yaml(self, job_name):
        if not job_name or Path(job_name).name != job_name:
            raise ValueError("job_name must be a config name, not a path")
        file_path = self.config_path / f"{job_name.lower()}.yaml"
        if not file_path.is_file():
            raise FileNotFoundError(f"Pipeline metadata not found: {file_path}")
        with file_path.open(encoding="utf-8") as file:
            metadata = yaml.safe_load(file)
        if not isinstance(metadata, dict):
            raise ValueError(f"Pipeline metadata must be a YAML mapping: {file_path}")
        return metadata


class TemplateLoader:
    def __init__(self, template_path=TEMPLATE_DIR):
        self.template_path = Path(template_path)

    def load_template(self, file_name, **context):
        if not file_name or Path(file_name).name != file_name:
            raise ValueError("template_name must be a file name, not a path")
        environment = Environment(
            loader=FileSystemLoader(str(self.template_path)),
            undefined=StrictUndefined,
            autoescape=False,
            trim_blocks=True,
            lstrip_blocks=True,
        )
        return environment.get_template(file_name).render(**context)


def resolve_spark_path(path):
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = SPARK_DIR / resolved
    return resolved.resolve()


def load_metadata(job_name):
    metadata = MetadataLoader().load_yaml(job_name)
    template_config = metadata.get("template") or {}
    template_name = template_config.get("template_name")
    if not template_name:
        raise ValueError(f"Missing template.template_name in {job_name}.yaml")

    template_folder = Path(template_config.get("template_folder", "templates"))
    if not template_folder.is_absolute() and ".." in template_folder.parts:
        template_path = (SPARK_DIR / "framework" / template_folder).resolve()
    else:
        template_path = resolve_spark_path(template_folder)
    template_context = {
        "metadata": metadata,
        **metadata,
    }
    sql = TemplateLoader(template_path).load_template(
        template_name,
        **template_context,
    )
    return metadata, sql


def metadata(*, event_name):
    if not event_name:
        raise ValueError("event_name is required")
    return load_metadata(event_name)
