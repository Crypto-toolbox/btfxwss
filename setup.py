from distutils.core import setup

setup(name='btfxwss', version='1.0', author='Nils Diefenbach',
      author_email='23okrs20+pypi@mykolab.com',
      url="https://github.com/nlsdfnbch/bitfinex_wss", license='LICENCSE',
      packages=['btfxwss'], install_requires=['websocket-client'],
      description="Python 3.5+ Websocket Client for the Bitfinex WSS API.",
      long_description=open('README.rst').read())
