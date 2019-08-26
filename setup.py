import os
import subprocess
import sys
from shutil import rmtree

from setuptools import Command, find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))


def get_version():
    version = {}
    with open('tap_dayforce/version.py') as fp:
        exec(fp.read(), version)
    return version['__version__']


with open('README.md', 'r') as f:
    readme = f.read()


class BaseCommand(Command):
    """Base Command"""

    user_options = []

    @staticmethod
    def status(s):
        """Prints things in bold."""
        print("\033[1m{0}\033[0m".format(s))

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def _run(self, s, command):
        try:
            self.status(s + "\n" + " ".join(command))
            subprocess.check_call(command)
        except subprocess.CalledProcessError as error:
            sys.exit(error.returncode)


class UploadCommand(BaseCommand):
    """Support setup.py upload. Thanks @kennethreitz!"""

    description = "Build and publish the package."

    def run(self):
        try:
            self.status("Removing previous builds…")
            rmtree(os.path.join(here, "dist"))
        except OSError:
            pass

        self._run(
            "Building Source and Wheel (universal) distribution…",
            [sys.executable, "setup.py", "sdist", "bdist_wheel", "--universal"],
        )

        self._run(
            "Installing Twine dependency…",
            [sys.executable, "-m", "pip", "install", "twine"],
        )

        self._run(
            "Uploading the package to PyPI via Twine…",
            [sys.executable, "-m", "twine", "upload", "dist/*"],
        )

        self._run("Creating git tags…", ["git", "tag", f"v{get_version()}"])
        self._run("Pushing git tags…", ["git", "push", "--tags"])


setup(
    name='tap_dayforce',
    author='David Wallace',
    author_email='david.wallace@goodeggs.com',
    version=get_version(),
    url='https://github.com/goodeggs/tap-dayforce',
    description='Singer.io tap for extracting data from Dayforce REST API v1',
    long_description=readme,
    long_description_content_type='text/markdown',
    classifiers=[
        'Development Status :: 1 - Planning',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Natural Language :: English',
        'Topic :: Software Development',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'
    ],
    keywords="singer tap python dayforce",
    license='GPLv3',
    packages=find_packages(exclude=['tests']),
    package_data={
        'tap_dayforce': ['schemas/*.json']
    },
    install_requires=[
        'requests==2.22.0',
        'singer-python==5.7.0'
    ],
    python_requires='>=3.6',
    entry_points={
        'console_scripts': ['tap-dayforce = tap_dayforce:main']
    }
)
