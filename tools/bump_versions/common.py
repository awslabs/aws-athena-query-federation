import datetime
import subprocess
from bs4 import BeautifulSoup


def get_new_version(existing_version):
    existing_split = existing_version.split(".")
    existing_version_without_minor = ".".join(existing_split[0:2])
    new_version_without_minor = datetime.datetime.now().strftime(f"%Y.%U")
    minor_version = 1
    # Bump the minor version if the versions match
    if existing_version_without_minor == new_version_without_minor:
        minor_version = int(existing_split[-1]) + 1
    return f"{new_version_without_minor}.{minor_version}"


def output_xml(soup, filename):
    with open(f"{filename}.unformatted", "w") as f:
        f.write(str(soup))
    subprocess.run([
        "xmllint",
            "--format", f"{filename}.unformatted",
            "--output", filename,
    ], env={"XMLLINT_INDENT": "    "})


def update_yaml(yaml_files, existing_version, new_version):
    for yml in yaml_files:
        subprocess.run(["sed", "-i", f"s/\(SemanticVersion:\s*\){existing_version}/\\1{new_version}/", yml])
        subprocess.run(["sed", "-i", f"s/\(CodeUri:.*\){existing_version}\(.*\)/\\1{new_version}\\2/", yml])


def update_project_version(soup):
    project = soup.find("project")
    version = project.find_all("version", recursive=False, limit=1)[0]
    existing_version = version.string.replace(" ","").strip()
    new_version = get_new_version(existing_version)
    version.string = new_version
    return (existing_version, new_version)


def update_dependency_version(soup, dependencyArtifactId):
    project = soup.find("project")
    dependencies = project.find_all("artifactId", string=dependencyArtifactId)
    for dep in dependencies:
        dep_version = dep.parent.find("version")
        existing_dep_version = dep_version.string.replace(" ","").strip()
        new_dep_version = get_new_version(existing_dep_version)
        dep_version.string = new_dep_version


def get_projects_artifact_ids_map(project_dirs):
    project_artifact_ids = {}
    for project in project_dirs:
        with open(f"{project}/pom.xml") as f:
            soup = BeautifulSoup(f, 'xml')
            artifactId = soup.find("project").find_all("artifactId", recursive=False, limit=1)[0].string
            project_artifact_ids[project] = artifactId
    return project_artifact_ids

