import os
import glob

from pathlib import Path
from bs4 import BeautifulSoup

import common

# Dependencies:
# pip3 install lxml beautifulsoup4
# sudo apt-get install libxml2-utils

EXCLUDE_DIRS={
    "athena-federation-sdk",
    "athena-federation-integ-test",
    "athena-federation-sdk-tools"
}


if __name__ == "__main__":
    # Make sure we are in the root directory
    root_dir = Path(__file__).parent.parent.parent
    os.chdir(root_dir)

    connector_dirs = list(filter(lambda x: x not in EXCLUDE_DIRS, glob.glob("athena*")))

    connector_artifact_ids = common.get_projects_artifact_ids_map(connector_dirs)

    # Bump the versions across all of the connectors including dependencies on other connectors
    for connector in connector_dirs:
        with open(f"{connector}/pom.xml") as f:
            soup = BeautifulSoup(f, 'xml')

            # First update the project's version
            (existing_version, new_version) = common.update_project_version(soup)

            # Then update any dependencies on other connectors to the new version as well
            for connector_artifact_id in connector_artifact_ids.values():
                # This is ourselves. We can't depend on ourselves so skip this.
                if connector_artifact_id == connector_artifact_ids[connector]:
                    continue
                common.update_dependency_version(soup, connector_artifact_id)

        # Output the xml
        common.output_xml(soup, f"{connector}/pom.xml")

        # Bump the versions in the yaml files
        yaml_files = glob.glob(f"{connector}/*.yaml") + glob.glob(f"{connector}/*.yml")
        common.update_yaml(yaml_files, existing_version, new_version)
