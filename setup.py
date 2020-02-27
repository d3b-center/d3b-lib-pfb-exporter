import os
from setuptools import setup, find_packages

root_dir = os.path.dirname(os.path.abspath(__file__))
req_file = os.path.join(root_dir, 'requirements.txt')
with open(req_file) as f:
    requirements = f.read().splitlines()

version = __import__('pfb_exporter').__version__

setup(
    name='kf-lib-pfb-exporter',
    version=version,
    description='Kids First PFB Exporter',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'pfbe=pfb_exporter.cli:cli',
        ],
    },
    include_package_data=True,
    install_requires=requirements
)
