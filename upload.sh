sudo rm dist/* || true
python3 -m build
python3 -m pip install --upgrade twine
python3 -m twine upload --repository pypi dist/* -u __token__ -p $PYPI_TOKEN
