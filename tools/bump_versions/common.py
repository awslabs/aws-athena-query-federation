import datetime
import subprocess
from bs4 import BeautifulSoup


def get_new_version():
    # Fetch tags first
    origin = "git@github.com:awslabs/aws-athena-query-federation.git"
    subprocess.run(["git", "fetch", "--tag", origin])
    # Then get the latest tag for this week
    new_version_without_iteration = datetime.datetime.now().strftime(f"%Y.%U")
    latest_tag_for_this_week = subprocess.check_output(
        [f'git tag --sort="version:refname" | grep "^v{new_version_without_iteration}" | tail -1'], shell=True)
    existing_version_without_iteration = None
    # Bump the iteration if there's already a tag for this week
    iteration = 1
    if latest_tag_for_this_week:
        existing_split = latest_tag_for_this_week[1:].decode("utf-8").split(".")
        existing_version_without_iteration = ".".join(existing_split[0:2])
        iteration = int(existing_split[-1]) + 1
    return f"{new_version_without_iteration}.{iteration}"


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

