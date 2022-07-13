from setuptools import find_packages, setup
from typing import List


def load_deps() -> List[str]:
    with open("requirements.txt", "r") as f:
        deps = map(lambda x: x.strip(), f.readlines())
    return list(deps)


setup(
    name='bain-challenge',
    packages=find_packages(),
    version='0.1.0',
    description='Bain challenge for MLE',
    author='cristobal.vasquezf@gmail.com',
    install_requires=load_deps(),
)
