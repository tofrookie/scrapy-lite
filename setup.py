from havok_lib import NAME,VERSION

LICENSE = "MIT"

DESCRIPTION = ''

LONG_DESCRIPTION = ''

KEYWORDS = u"scrapy lite python package"
try:
    from setuptools import setup
except:
    from distutils.core import setup

AUTHOR = 'zz'

AUTHOR_EMAIL = ''

URL = ''

PACKAGES = []

if __name__ == "__main__":
    setup(
        name=NAME,
        version=VERSION,
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        classifiers=[
            'License :: OSI Approved :: MIT License',
            'Programming Language :: Python',
            'Intended Audience :: Developers',
            'Operating System :: OS Independent',
        ],
        keywords=KEYWORDS,
        author=AUTHOR,
        author_email=AUTHOR_EMAIL,
        url=URL,
        license=LICENSE,
        packages=PACKAGES,
        package_data={
            "": ["README.md"],
        },
        exclude=["test.*","test*"],
        requires=["requests",],
        include_package_data=True,
        zip_safe=True,
    )