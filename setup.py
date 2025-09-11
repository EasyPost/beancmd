from setuptools import setup, find_packages
from beancmd import __version__


install_requires = []
with open('requirements.txt', 'r') as f:
    for line in f:
        install_requires.append(line.rstrip())


setup(
    name="beancmd",
    version=__version__,
    author="EasyPost",
    author_email="support@easypost.com",
    url="https://github.com/easypost/beancmd",
    description="Self-contained command-line tool for administrating beanstalkd",
    license="ISC",
    install_requires=install_requires,
    include_package_data=True,
    packages=find_packages(exclude=['tests', 'tests.*']),
    entry_points={
        'console_scripts': [
            'beancmd = beancmd.beancmd:main',
        ]
    },
    python_requires='>=3.9, <4',
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.9",
        "Intended Audience :: System Administrators",
        "Operating System :: OS Independent",
        "Topic :: Database",
        "License :: OSI Approved :: ISC License (ISCL)",
    ]
)
