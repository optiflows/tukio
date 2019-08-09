VERSION_TAG=$1

if [ ! -z $VERSION_TAG ]; then
    echo $VERSION_TAG > VERSION.txt
    python setup.py sdist
    twine upload -u $PYPI_USER -p $PYPI_PASSWORD dist/tukio-$VERSION_TAG.tar.gz
else
    echo "No VERSION_TAG defined, skipping packaging and upload."
    exit 1;
fi
