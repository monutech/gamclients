import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="gamclients",
    version="0.0.2",
    author="Monumetric",
    author_email="tech@monumetric.com",
    description="A small wrapper for the GAM python API to enable easier " +
                "access to reporting, and kvp tools",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/monutech/admanager-reports",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires = ['tqdm', 'googleads', 'oauth2client', 'pandas', 'ipython', 'polars'],
)
