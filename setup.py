from setuptools import setup, find_packages

setup(
    name="deltatask",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "python-frontmatter>=3.0.0",
        "SQLAlchemy>=2.0.0",
        "FastMCP==0.4.1",
    ],
    description="A powerful, locally-hosted todo application backend",
    author="DeltaTask Team",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.8",
)