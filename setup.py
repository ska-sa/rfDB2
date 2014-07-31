from distutils.core import setup, Extension
import os, sys, glob

__version__ = '0.0.1'

root_install = '/home/ratty2/monitor'
setup(name = 'rfDB2',
    version = __version__,
    description = 'Interface to an RF database',
    long_description = 'Provides interfaces to RF database of spectral data.',
    license = 'GPL',
    author = 'Christopher Schollar',
    author_email = 'ctgschollar at gmail.com',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Topic :: Scientific/Engineering :: Astronomy',
        'Topic :: Software Development :: Libraries :: Python Modules',
        ],
    requires=['h5py', 'pylab','matplotlib','numpy','corr', 'ratty2'],
    provides=['rfDB'],
    package_dir = {'rfDB2':'src'},
    packages = ['rfDB2'],
    data_files=[('%s/etc/ratty2'%root_install, glob.glob('conf/*'))])
