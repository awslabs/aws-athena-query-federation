import os
import glob
import subprocess

from pathlib import Path
from bs4 import BeautifulSoup

import common

# Dependencies:
# pip3 install lxml beautifulsoup4
# sudo apt-get install libxml2-utils

SDK_DIRS = {
    "athena-federation-sdk",
    "athena-federation-integ-test",
    "athena-federation-sdk-tools",
    "."
}


if __name__ == "__main__":
    # Make sure we are in the root directory
    root_dir = Path(__file__).parent.parent.parent
    os.chdir(root_dir)

    # Update the sdk project versions
    for sdk_dir in SDK_DIRS:
        with open(f"{sdk_dir}/pom.xml") as f:
            soup = BeautifulSoup(f, 'xml')
            # Update each project's version
            (existing_version, new_version) = common.update_project_version(soup)
        common.output_xml(soup, f"{sdk_dir}/pom.xml")

    # This incluldes the root project
    all_projects = glob.glob("athena*") + ["."]

    sdk_project_artifact_ids = common.get_projects_artifact_ids_map(SDK_DIRS)

    # Bump the sdk artifact dependency version across all projects
    for project in all_projects:
        with open(f"{project}/pom.xml") as f:
            soup = BeautifulSoup(f, 'xml')
            for sdk_project_artifact_id in sdk_project_artifact_ids.values():
                # This is ourselves, so don't bump again
                if sdk_project_artifact_id == sdk_project_artifact_ids.get(project):
                    continue
                common.update_dependency_version(soup, sdk_project_artifact_id)
        common.output_xml(soup, f"{project}/pom.xml")

    # athena-federation-sdk/athena-federation-sdk.yaml
    # Bump the versions in the yaml files
    yaml_files = glob.glob(f"athena-federation-sdk/*.yaml") + glob.glob(f"athena-federation-sdk/*.yml")
    common.update_yaml(yaml_files, existing_version, new_version)

    # Bump misc files
    MISC = ["athena-federation-integ-test/README.md", "tools/validate_connector.sh"]
    for m in MISC:
        subprocess.run(["sed", "-i", f"s/{existing_version}/{new_version}/", m])
