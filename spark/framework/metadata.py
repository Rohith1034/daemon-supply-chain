import yaml
import os
from jinja2 import Environment, FileSystemLoader, StrictUndefined


def metadata(*, event_name):
    if (event_name == "" and
            (
                    event_name != "purchase_order"
                    or event_name != "order"
                    or event_name != "inventory"
                    or event_name != "purchase_order_items"
                    or event_name != "shipment"
                    or event_name != "supplier_shipment"
            )
    ):
        return

    loader = MetadataLoader(
        "../configs"
    )

    metadata = loader.load_yaml(
        event_name
    )

    # print(metadata)

    template_name = metadata.get("template").get("template_name")
    folder_path = metadata.get("template").get("template_folder")

    # print(template_name)
    # print(folder_path)

    template = TemplateLoader(folder_path)
    # print(template.load_template(template_name))

    return [metadata, template.load_template(template_name)]

class MetadataLoader:

    def __init__(self, config_path):
        self.config_path = config_path

    def load_yaml(self, job_name):
        file_name = f"{job_name.lower()}.yaml"

        file_path = os.path.join(
            self.config_path,
            file_name
        )
        # print(os.getcwd())
        # print(file_path)
        with open(file_path, "r") as file:
            config = yaml.safe_load(file)

        return config


class TemplateLoader:
    def __init__(self, template_path):
        self.template_path = template_path

    def load_template(self, file_name):
        sql_dir = self.template_path
        #print(sql_dir)
        env = Environment(
            loader=FileSystemLoader(str(sql_dir)),
            undefined=StrictUndefined,
            autoescape=False,
        )
        template = env.get_template(f"{file_name}")
        rendered_sql = template.render()
        return rendered_sql



metadata(event_name="purchase_order")