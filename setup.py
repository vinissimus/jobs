from setuptools import find_packages
from setuptools import setup

try:
    README = open("README.md").read()
except IOError:
    README = None

setup(
    name="pgjobs",
    version="0.2.0",
    description="Postgresql job scheduling",
    long_description=README,
    long_description_content_type="text/markdown",
    author="jordi collell",
    author_email="jordic@gmail.com",
    url="https://github.com/vinissimus/jobs",
    package_data={"jobs": ["py.typed"]},
    packages=find_packages(),
    include_package_data=True,
    classifiers=[],
    install_requires=["asyncpg>=0.20.1,<0.21"],
    tests_require=["pytest", "pytest-docker-fixtures"],
    extras_require={
        "test": ["pytest", "pytest-asyncio", "pytest-cov", "coverage"]
    },
    entry_points={
        "console_scripts": [
            "jobs-worker = jobs.worker:run",
            "jobs-migrator = jobs.migrations:run",
        ]
    },
)
