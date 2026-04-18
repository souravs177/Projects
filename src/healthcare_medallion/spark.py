from __future__ import annotations

import os
import sys
import json
from importlib import metadata
from pathlib import Path
from urllib.request import urlopen, urlretrieve
from xml.etree import ElementTree

import pyspark
from pyspark.sql import SparkSession

from healthcare_medallion.config import PipelineConfig, SparkConfig

DELTA_SCALA_VERSION = "2.12"
MAVEN_CENTRAL = "https://repo1.maven.org/maven2"


def delta_cache_dir() -> Path:
    return Path.home() / ".healthcare_medallion" / "delta_jars"


def default_windows_hadoop_home() -> Path:
    local_app_data = Path(os.environ.get("LOCALAPPDATA", Path.home()))
    return local_app_data / "healthcare_medallion" / "hadoop"


def maven_jar_url(group: str, artifact: str, version: str) -> str:
    return (
        f"{MAVEN_CENTRAL}/{group.replace('.', '/')}/{artifact}/{version}/{artifact}-{version}.jar"
    )


def maven_pom_url(group: str, artifact: str, version: str) -> str:
    return (
        f"{MAVEN_CENTRAL}/{group.replace('.', '/')}/{artifact}/{version}/{artifact}-{version}.pom"
    )


def parse_delta_dependencies(delta_version: str) -> list[tuple[str, str, str]]:
    pom_url = maven_pom_url("io.delta", f"delta-spark_{DELTA_SCALA_VERSION}", delta_version)
    with urlopen(pom_url) as response:
        pom_tree = ElementTree.parse(response)

    namespace = {"mvn": "http://maven.apache.org/POM/4.0.0"}
    dependencies: list[tuple[str, str, str]] = [
        ("io.delta", f"delta-spark_{DELTA_SCALA_VERSION}", delta_version)
    ]

    for dependency in pom_tree.findall(".//mvn:dependency", namespace):
        group_id = dependency.findtext("mvn:groupId", default="", namespaces=namespace).strip()
        artifact_id = dependency.findtext("mvn:artifactId", default="", namespaces=namespace).strip()
        version = dependency.findtext("mvn:version", default="", namespaces=namespace).strip()
        scope = dependency.findtext("mvn:scope", default="", namespaces=namespace).strip()
        optional = dependency.findtext("mvn:optional", default="false", namespaces=namespace).strip().lower()

        if not group_id or not artifact_id or not version:
            continue
        if scope == "test" or optional == "true":
            continue

        dependencies.append((group_id, artifact_id, version))

    return dependencies


def ensure_delta_jars() -> list[Path]:
    cache_dir = delta_cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)

    delta_version = metadata.version("delta_spark")
    manifest_path = cache_dir / f"delta-jars-{delta_version}.json"

    if manifest_path.exists():
        jar_names = json.loads(manifest_path.read_text(encoding="utf-8"))
        jar_paths = [cache_dir / jar_name for jar_name in jar_names]
        if all(path.exists() for path in jar_paths):
            return jar_paths

    jar_paths: list[Path] = []

    for group_id, artifact_id, version in parse_delta_dependencies(delta_version):
        jar_path = cache_dir / f"{artifact_id}-{version}.jar"
        if not jar_path.exists():
            urlretrieve(maven_jar_url(group_id, artifact_id, version), jar_path)
        jar_paths.append(jar_path)

    manifest_path.write_text(
        json.dumps([path.name for path in jar_paths], indent=2),
        encoding="utf-8",
    )
    return jar_paths


def ensure_valid_spark_home() -> str:
    configured_home = os.environ.get("SPARK_HOME")
    bundled_home = Path(pyspark.__file__).resolve().parent
    bundled_submit = bundled_home / "bin" / "spark-submit.cmd"
    current_python = sys.executable

    os.environ["PYSPARK_PYTHON"] = current_python
    os.environ["PYSPARK_DRIVER_PYTHON"] = current_python
    if os.name == "nt":
        configured_hadoop_home = os.environ.get("HADOOP_HOME")
        configured_winutils = (
            Path(configured_hadoop_home) / "bin" / "winutils.exe"
            if configured_hadoop_home
            else None
        )
        default_hadoop_home = default_windows_hadoop_home()
        default_winutils = default_hadoop_home / "bin" / "winutils.exe"

        if configured_winutils and configured_winutils.exists():
            pass
        elif default_winutils.exists():
            os.environ["HADOOP_HOME"] = str(default_hadoop_home)
        else:
            os.environ.pop("HADOOP_HOME", None)
    elif os.environ.get("HADOOP_HOME") and not Path(os.environ["HADOOP_HOME"]).exists():
        os.environ.pop("HADOOP_HOME", None)

    if configured_home:
        configured_path = Path(configured_home)
        configured_submit = configured_path / "bin" / "spark-submit.cmd"
        if configured_submit.exists():
            return str(configured_path)

    if bundled_submit.exists():
        os.environ["SPARK_HOME"] = str(bundled_home)
        return str(bundled_home)

    if configured_home:
        return configured_home

    raise RuntimeError("Unable to locate a valid Spark installation for PySpark.")


def build_spark_session(config: SparkConfig, pipeline_config: PipelineConfig) -> SparkSession:
    ensure_valid_spark_home()
    builder = (
        SparkSession.builder.appName(config.app_name)
        .master(config.master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(config.shuffle_partitions))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )

    if pipeline_config.uses_delta():
        builder = builder.config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )

        if os.name == "nt":
            jar_paths = ensure_delta_jars()
            classpath = os.pathsep.join(str(path) for path in jar_paths)
            builder = builder.config("spark.driver.extraClassPath", classpath).config(
                "spark.executor.extraClassPath",
                classpath,
            )
            session = builder.getOrCreate()
        else:
            try:
                from delta import configure_spark_with_delta_pip
            except ImportError as error:
                raise RuntimeError(
                    "Delta Lake support is enabled in conf/pipeline.yml, but delta-spark is not installed. "
                    "Install project dependencies with `pip install -e .[dev]` on Python 3.10-3.12."
                ) from error

            session = configure_spark_with_delta_pip(builder).getOrCreate()
    else:
        session = builder.getOrCreate()

    session.sparkContext.setLogLevel("WARN")
    return session
