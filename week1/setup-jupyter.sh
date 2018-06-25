#! /bin/bash
. ~/env3/bin/activate
pip install --upgrade "ipython[all]"
curl -L -o jupyter-scala https://git.io/vrHhi && chmod +x jupyter-scala && ./jupyter-scala && rm -f jupyter-scala
jupyter kernelspec list
deactivate
echo To run Jupyter with Scala support, issue:
echo "	./start-jupyter.sh"