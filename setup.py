from setuptools import find_packages
from setuptools import setup

try:
    README = open("README.md").read()
except IOError:
    README = None

setup(
    name="jobs",
    version="0.1.0",
    description="Postgres job scheduling",
    long_description=README,
    author="jordi collell",
    author_email="jordic@gmail.com",
    url="https://github.com/jordic/jobs",
    package_data={"jobs": ["py.typed"]},
    packages=find_packages(),
    include_package_data=True,
    extras_require={},
    classifiers=[],
    entry_points={
        "console_scripts": [
            "jobs-worker = jobs.worker:run",
            "jobs-migrator = jobs.migrations:run",
        ]
    },
)
