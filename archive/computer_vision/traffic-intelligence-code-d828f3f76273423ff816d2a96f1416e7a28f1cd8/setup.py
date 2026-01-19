import setuptools

with open('README') as file:
    long_description = file.read()
    
setuptools.setup(
    name='trafficintelligence',
    version='0.2.10',
    author='Nicolas Saunier',
    author_email='nicolas.saunier@polymtl.ca',
    url='https://trafficintelligence.confins.net',
    packages=setuptools.find_packages(),
    description='Python modules of the Traffic Intelligence project',
    long_description=long_description,
    license = 'MIT License',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
