from setuptools import setup


setup(name='btfxwss', version='1.2.2', author='Nils Diefenbach',
      author_email='nlsdfnbch.foss@kolabnow.com',
      url="https://github.com/nlsdfnbch/bitfinex_wss", license='MIT',
      packages=['btfxwss'], install_requires=['websocket-client'],
      description="Python 3.5+ Websocket Client for the Bitfinex WSS API.")
