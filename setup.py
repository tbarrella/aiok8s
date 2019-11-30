import setuptools

with open("README.md") as f:
    long_description = f.read()

setuptools.setup(
    name="aiok8s",
    version="0.0.1",
    author="Taylor Barrella",
    author_email="tbarrella@gmail.com",
    description="asyncio client tools for Kubernetes",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/tbarrella/aiok8s",
    packages=setuptools.find_packages(),
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    python_requires=">=3.6",
)
