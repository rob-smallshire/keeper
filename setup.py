from setuptools import setup, find_packages

version = "1.1.0"

with open('README.rst', 'r') as readme:
    long_description = readme.read()

setup(
    name = "keeper",
    package_dir={'': 'source'},
    packages=find_packages('source'),
    version = "{version}".format(version=version),
    description = "A file-based value store for bytes and strings.",
    author = "Robert Smallshire",
    author_email = "robert@smallshire.org.uk",
    url = "https://github.com/rob-smallshire/keeper/",
    download_url="https://github.com/rob-smallshire/keeper/archive/master.zip".format(version=version),
    keywords = ["Python"],
    license="MIT License",
    classifiers = [
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Environment :: Other Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Utilities",
        ],
    long_description = long_description,
    # List run-time dependencies here.  These will be installed by pip when your
    # project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=['atomicwrites'],
)
