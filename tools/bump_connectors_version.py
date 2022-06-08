import os
import glob
import datetime
import subprocess

from pathlib import Path
from bs4 import BeautifulSoup


# Dependencies:
# pip3 install lxml beautifulsoup4
# sudo apt-get install libxml2-utils

EXCLUDE_DIRS={
    "athena-federation-sdk",
    "athena-federation-integ-test",
    "athena-federation-sdk-tools"
}


def get_new_version(existing_version):
    existing_split = existing_version.split(".")
    existing_version_without_minor = ".".join(existing_split[0:2])
    new_version_without_minor = datetime.datetime.now().strftime(f"%Y.%U")
    minor_version = 1
    # Bump the minor version if the versions match
    if existing_version_without_minor == new_version_without_minor:
        minor_version = int(existing_split[-1]) + 1
    return f"{new_version_without_minor}.{minor_version}"


if __name__ == "__main__":
    # Make sure we are in the root directory
    root_dir = Path(__file__).parent.parent
    os.chdir(root_dir)

    connector_dirs = list(filter(lambda x: x not in EXCLUDE_DIRS, glob.glob("athena*")))
    
    connector_artifact_ids = {}
    # Get the artifactIds of the connectors
    for connector in connector_dirs:
        with open(f"{connector}/pom.xml") as f:
            soup = BeautifulSoup(f, 'xml')
            artifactId = soup.find("project").find_all("artifactId", recursive=False, limit=1)[0].string
            connector_artifact_ids[connector] = artifactId

    # Bump the versions across all of the connectors including dependencies on other connectors    
    for connector in connector_dirs:
        with open(f"{connector}/pom.xml") as f:
            print(connector)
            soup = BeautifulSoup(f, 'xml')
    
            # First update the project's version
            project = soup.find("project")
            version = project.find_all("version", recursive=False, limit=1)[0]
            existing_version = version.string.replace(" ","").strip()
            new_version = get_new_version(existing_version)
            version.string = new_version
    
            # Then update any dependencies on other connectors to the new version as well
            for connector_artifact_id in connector_artifact_ids.values():
                # This is ourselves. We can't depend on ourselves so skip this.
                if connector_artifact_id == connector_artifact_ids[connector]:
                    continue
                dependencies_on_other_connectors = project.find_all("artifactId", string=connector_artifact_id)
                for dep in dependencies_on_other_connectors:
                    dep_version = dep.parent.find("version")
                    existing_dep_version = dep_version.string.replace(" ","").strip()
                    new_dep_version = get_new_version(existing_dep_version)
                    dep_version.string = new_dep_version

        # Write out the updated file
        with open(f"{connector}/pom.xml.unformatted", "w") as f:
            f.write(str(soup))

        # Reformat the file since beautifulsoup's formatting is bad (even if I use a formatter):
        subprocess.run([
            "xmllint",
                "--format", f"{connector}/pom.xml.unformatted",
                "--output", f"{connector}/pom.xml"
        ], env={"XMLLINT_INDENT": "    "})

        # Bump the versions in the yaml files
        yaml_files = glob.glob(f"{connector}/*.yaml") + glob.glob(f"{connector}/*.yml")
        for yml in yaml_files:
            subprocess.run(["sed", "-i", f"s/SemanticVersion:\s*{existing_version}/SemanticVersion: {new_version}/", yml])
            subprocess.run(["sed", "-i", f"s/\(CodeUri:.*\){existing_version}\(.*\)/\\1{new_version}\\2/", yml])
