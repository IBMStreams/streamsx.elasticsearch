from setuptools import setup
setup(
  name = 'streamsx.elasticsearch',
  packages = ['streamsx.elasticsearch'],
  include_package_data=True,
  version = '0.1.0',
  description = 'IBM Streams Elasticsearch integration',
  long_description = open('DESC.txt').read(),
  author = 'IBM Streams @ github.com',
  author_email = 'hegermar@us.ibm.com',
  license='Apache License - Version 2.0',
  url = 'https://github.com/IBMStreams/streamsx.elasticsearch',
  keywords = ['streams', 'ibmstreams', 'streaming', 'analytics', 'streaming-analytics', 'elasticsearch'],
  classifiers = [
    'Development Status :: 1 - Planning',
    'License :: OSI Approved :: Apache Software License',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.5',
  ],
  install_requires=['streamsx'],
  
  test_suite='nose.collector',
  tests_require=['nose']
)
