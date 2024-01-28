from setuptools import setup,find_packages
setup(
    name="spark-func-py",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pyspark==3.5.0",
        "numpy==1.21.2",
        "pandas==1.3.2",
    ],
    entry_points={
        'console_scripts': [
            'my_script=mains:main',
        ],
    },
)