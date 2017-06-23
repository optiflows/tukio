echo -e "[distutils]
index-servers=pypi

[pypi]
username = $PYPI_USER
password = $PYPI_PASSWORD
" > ~/.pypirc

echo $CIRCLE_TAG > VERSION.txt
python setup.py sdist upload
