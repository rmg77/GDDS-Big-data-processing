#! /bin/bash
IP="$(hostname -I | cut -d' ' -f1)"
THIS_USER="$(whoami)"
. ~/env3/bin/activate
echo
echo
echo Please do the following:
echo
echo 1- On MoVE Terminal issue:
echo "	ssh -N -f -L localhost:8888:localhost:8888 ${THIS_USER}@${IP}"
echo and enter your password.
echo
echo 2- Open your browser and go to localhost:8888
echo
echo
echo

jupyter notebook --NotebookApp.token='' --no-browser

deactivate
