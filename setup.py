"""setup.py for pypbcms."""

from setuptools import setup


setup(
    # TODO: the following values are defined in two places at once. This should be fixed.
    name="pypbcms",
    version="1.0.0",
    author="bennr01",
    description="A plugin based cluster management system written in python",
    license="MIT",
    keyword="cluster tools plugin plugins cmd rpc management",
    url="https://github.com/bennr01/pypbcms/",
    classifiers=[
        "Topic :: Utilities",
        "License :: MIT License",
        ],
    py_modules=[
        "pypbcms",
        ],
    install_requires=[
        "zope.interface",
        "Twisted",
        ],
    entry_points={
        "console_scripts": [
            "pypbcms = pypbcms:main"
            ],
        },
    )
