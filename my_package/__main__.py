# This file is executed when the module is run from the command line:
# python -m my_package
from my_package.example import greet


if __name__ == '__main__':
    # A dummy main
    print('Running module')

    greet('developer')

    print("It worked!")
