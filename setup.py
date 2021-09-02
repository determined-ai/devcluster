import pathlib
import setuptools

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()

with (HERE / "devcluster" / "__init__.py").open() as f:
    firstline = next(iter(f))
    assert firstline.startswith("__version__ = ")
    version = firstline.split('"')[1]

setuptools.setup(
    name="devcluster",
    version=version,
    author="Determined AI",
    author_email="hello@determined.ai",
    url="https://github.com/determined-ai/devcluster/",
    license="Apache License 2.0",
    classifiers=["License :: OSI Approved :: Apache Software License"],
    description="Developer tool for running the Determined cluster",
    long_description=README,
    long_description_content_type="text/markdown",
    python_requires=">=3.6",
    packages=["devcluster"],
    entry_points={"console_scripts": ["devcluster = devcluster.__main__:main"]},
    include_package_data=True,
    install_requires=[
        "appdirs",
        "pyyaml",
        # in python 3.9, we use importlib.resources instead
        "setuptools;python_version<'3.9'",
    ],
)
