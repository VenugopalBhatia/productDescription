from setuptools import find_packages, setup

setup(
    name= 'productDescription',
    packages=find_packages(include=['productDescription']),
    version= '0.1.0',
    description= 'Library to test and score product description and images',
    author= '1DS',
    license= '1DS',
    install_requires = [
        'psycopg2',
        'pandas',
        'numpy',
        'datetime',
        'language_tool_python',
        'fuzzywuzzy',
        'fuzzysearch',
        'statistics',
        'Pillow',
        'requests',
        'opencv-python',
        'pytesseract'

    ]
)