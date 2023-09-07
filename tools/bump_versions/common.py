import datetime
import subprocess
from bs4 import BeautifulSoup


def get_new_version():
    # Get latest release version
    previous_release_version = subprocess.check_output(['''
        gh release list --exclude-drafts --exclude-pre-releases -L 1 |
        sed 's/.*\s\+Latest\s\+v\(.*\)\s\+.*/\\1/g'
    '''], shell=True).decode("utf-8")

    # Generate the version without iteration for this week
    new_version_without_iteration = datetime.datetime.now().strftime("%Y.%-U")

    # If the latest previous release version happened in the same week, bump the iteration
    if previous_release_version.startswith(new_version_without_iteration):
        version = previous_release_version.split(".")
        return f"{version[0]}.{version[1]}.{int(version[2]) + 1}"

    # Otherwise just return the version for this week at iteration 1
    return f"{new_version_without_iteration}.1"


def output_xml(soup, filename):
    with open(f"{filename}.unformatted", "w") as f:
        f.write(str(soup))
    subprocess.run([
        "xmllint",
            "--format", f"{filename}.unformatted",
            "--output", filename,
    ], env={"XMLLINT_INDENT": "    "})


def update_yaml(yaml_files, new_version):
    for yml in yaml_files:
        subprocess.run(["sed", "-i", f"s/\(SemanticVersion:\s*\).*/\\1{new_version}/", yml])
        subprocess.run(["sed", "-i", f"s/\(CodeUri:.*-\)[0-9]*\.[0-9]*\.[0-9]*\(-\?.*\.jar\)/\\1{new_version}\\2/", yml])


def update_project_version(soup, new_version):
    project = soup.find("project")
    version = project.find_all("version", recursive=False, limit=1)[0]
    existing_version = version.string.replace(" ","").strip()
    version.string = new_version


def update_dependency_version(soup, dependencyArtifactId, new_version):
    project = soup.find("project")
    dependencies = project.find_all("artifactId", string=dependencyArtifactId)
    for dep in dependencies:
        dep_version = dep.parent.find("version")
        dep_version.string = new_version


def get_projects_artifact_ids_map(project_dirs):
    project_artifact_ids = {}
    for project in project_dirs:
        with open(f"{project}/pom.xml") as f:
            soup = BeautifulSoup(f, 'xml')
            artifactId = soup.find("project").find_all("artifactId", recursive=False, limit=1)[0].string
            project_artifact_ids[project] = artifactId
    return project_artifact_ids

