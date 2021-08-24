import setuptools

setuptools.setup(
    name="devcluster",
    version="0.1",
    author="Determined AI",
    author_email="rb@determined.ai",
    url="https://github.com/determined-ai/devcluster/",
    description="Developer tool for running the Determined cluster",
    python_requires=">=3.6",
    packages=["devcluster"],
    entry_points={"console_scripts": ["devcluster = devcluster.__main__:main"]},
)
