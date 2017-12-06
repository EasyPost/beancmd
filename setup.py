from setuptools import setup, find_packages


install_requires = []
with open('requirements.txt', 'r') as f:
    for line in f:
        install_requires.append(line.rstrip())


setup(
    name="beancmd",
    version="0.4.0",
    author="James Brown",
    author_email="jbrown@easypost.com",
    url="https://github.com/easypost/beancmd",
    description="Self-contained command-line tool for administrating beanstalkd",
    license="ISC",
    install_requires=install_requires,
    packages=find_packages(exclude=['tests']),
    entry_points={
        'console_scripts': [
            'beancmd = beancmd.beancmd:main',
        ]
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Intended Audience :: System Administrators",
        "Operating System :: OS Independent",
        "Topic :: Database",
        "License :: OSI Approved :: ISC License (ISCL)",
    ]
)
