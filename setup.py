from distutils.core import setup

from melastic import __version__

setup(
    name="melastic",
    version=__version__,
    author="Michael Penkov",
    author_email="misha.penkov@gmail.com",
    packages=["melastic"],
    description="Performs ElasticSearch bulk and scroll tasks",
    url="https://github.com/mpenkov/melastic",
    download_url="https://github.com/mpenkov/melastic/tarball/v" + __version__,
    keywords=["elastic"],
    classifiers=[]
)
