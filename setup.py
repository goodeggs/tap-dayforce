from setuptools import find_packages, setup


def get_version():
    version = {}
    with open("tap_dayforce/version.py") as fp:
        exec(fp.read(), version)
    return version["__version__"]


with open("README.md", "r") as f:
    readme = f.read()


setup(
    name="tap_dayforce",
    author="David Wallace",
    author_email="david.wallace@goodeggs.com",
    version=get_version(),
    url="https://github.com/goodeggs/tap-dayforce",
    description="Singer.io tap for extracting data from Dayforce REST API v1",
    long_description=readme,
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Natural Language :: English",
        "Topic :: Software Development",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    keywords="singer tap python dayforce",
    license="GPLv3",
    packages=find_packages(exclude=["tests"]),
    package_data={"tap_dayforce": ["schemas/*.json"]},
    install_requires=["dayforce-client==0.2.0", "singer-python==5.9.0", "backoff==1.8.0", "rollbar==0.14.7"],
    python_requires=">=3.6",
    entry_points={"console_scripts": ["tap-dayforce = tap_dayforce:main"]},
)
