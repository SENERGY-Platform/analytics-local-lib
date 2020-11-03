from setuptools import setup, find_packages

with open("README.md", "r") as readme_file:
    readme = readme_file.read()

requirements = ["paho-mqtt~=1.5.1", "jsonpath-rw", "jsonpath-rw-ext"]

setup(
    name="senergy-local-analytics",
    version="0.0.1",
    author="CC SES",
    author_email="zsco@users.noreply.github.com",
    description="A package to enable local analytics",
    long_description=readme,
    long_description_content_type="text/markdown",
    url="https://github.com/SENERGY-Platform/analytics-local-lib/",
    packages=find_packages(),
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 3.9",
    ],
)