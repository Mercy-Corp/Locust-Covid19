# Main instructions


There are several files in the root folder. The code goes in *my_package".

- *README.md* (this file) has a description of what the code does. It also shows up in the repository web interface.
- *requirements.txt* has all the non-standard packages that are needed to run our code (pandas, numpy)
- *MANIFEST.in* has a list of the folders that will be included when the package is generated. It has to be edited.
- *.gitignore* contains a list of file specifications that will not be part of the repository
- *setup.py* has the installation instructions for our package. It follows the setuptools guidelines. Running "python setup.py install" will install the package in our current environment.

## Folder structure
- *my_package* is the folder that has the module code. Change the name to something meaningful.
- *tests* is the folder where python unittests are run
- *scripts* have auxiliary script that may be needed to make the package work, for example table creation scripts
- *extras* this is a cointainer for anything that might be useful but is not part of the package itself. For example Jupyter notebooks, log outputs, test files, etc.
