from setuptools import setup, find_packages

# get version
from os import path
verfile = path.join(path.dirname(__file__), 'medoo', '__init__.py')
with open(verfile) as vf:
    VERSION = vf.readline().split('=')[1].strip()[1:-1]

setup (
	name             = 'medoo',
	version          = VERSION,
	description      = "A lightweight database framework for python",
	url              = "https://github.com/pwwang/pymedoo",
	author           = "pwwang",
	author_email     = "pwwang@pwwang.com",
	license          = "MIT License",
	long_description = 'https://github.com/pwwang/pymedoo',
	packages         = find_packages(),
	install_requires = [
		'six'
    ],
)
