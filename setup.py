"""Build the redis-flow distribution and its development extras."""

import re
from pathlib import Path
from urllib.parse import urljoin

from setuptools import find_packages, setup

ROOT = Path(__file__).parent
REPOSITORY_URL = "https://github.com/forgeries/redis-flow"


def get_requirements():
    """Read runtime dependencies from the source distribution."""
    return (ROOT / "requirements.txt").read_text(encoding="utf-8").splitlines()


def get_long_description():
    """Resolve documentation links for the PyPI project page."""
    text = (ROOT / "README.markdown").read_text(encoding="utf-8")
    return re.sub(
        r"\]\(([^)]+)\)",
        lambda match: "](" + urljoin(REPOSITORY_URL + "/blob/main/", match[1]) + ")",
        text,
    )


setup(
    name="redis-flow",
    version="0.3.6",
    license_expression="MIT",
    license_files=["LICENSE"],
    author="Vincent Yao",
    author_email="552388264@qq.com",
    url=REPOSITORY_URL,
    project_urls={
        "Issues": REPOSITORY_URL + "/issues",
        "Documentation": REPOSITORY_URL + "/blob/main/README.markdown",
        "Changelog": REPOSITORY_URL + "/blob/main/CHANGELOG.markdown",
    },
    description="A lightweight Python library for Redis task workflows.",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=["tests", "tests.*"]),
    include_package_data=True,
    zip_safe=False,
    platforms="any",
    install_requires=get_requirements(),
    python_requires=">=3.10",
    extras_require={
        "dev": ["pytest>=8,<10", "build>=1", "twine>=6", "ruff>=0.17,<0.18"]
    },
    keywords=["redis", "tasks", "queue", "workflow"],
    classifiers=[
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
