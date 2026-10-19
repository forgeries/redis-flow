"""Build the redis-flow distribution and its development extras."""

from pathlib import Path

from setuptools import find_packages, setup

ROOT = Path(__file__).parent


def get_requirements():
    """Read runtime dependencies from the source distribution."""
    return (ROOT / "requirements.txt").read_text(encoding="utf-8").splitlines()


setup(
    name="rf",
    version="0.3.6",
    license_expression="MIT",
    license_files=["LICENSE"],
    author="Vincent Yao",
    author_email="552388264@qq.com",
    url="https://github.com/forgeries/redis-flow",
    project_urls={
        "Issues": "https://github.com/forgeries/redis-flow/issues",
    },
    description="A lightweight Python library for Redis task workflows.",
    long_description=(ROOT / "README.markdown").read_text(encoding="utf-8"),
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
    classifiers=["Programming Language :: Python :: 3"],
)
