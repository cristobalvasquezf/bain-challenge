from setuptools import find_packages, setup
from src import __version__
from typing import List


def load_deps() -> List[str]:
    with open("requirements.txt", "r") as f:
        deps = map(lambda x: x.strip(), f.readlines())
    return list(deps)


setup(
    name='Bain-challenge',
    packages=find_packages(),
    version=__version__,
    description='Bain challenge for MLE',
    author='cristobal.vasquezf@gmail.com',
    install_requires=load_deps(),
)
