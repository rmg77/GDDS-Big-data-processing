# Jupyter with Scala Support on BigVM

## Installation

**On Big VM terminal:**

After a successful `ssh` connection to your BigVM instance from a [MoVE](move.monash.edu) terminal do the following only **once** (or just download and run `setup-jupyter.sh`):
1- Go to Python 3 environment

`cd ~/env3`

2- Activate the environment (note the dot at the beginning and a space between the terms)

`. bin/activate`

3- upgrade your pip

`pip install --upgrade "ipython[all]"`

4- download and isnstall Scala kernel (https://libraries.io/github/jupyter-scala/jupyter-scala):

`curl -L -o jupyter-scala https://git.io/vrHhi && chmod +x jupyter-scala && ./jupyter-scala && rm -f jupyter-scala`

5- make sure Scala kernel is installed:

`jupyter kernelspec list`

It should print something like: 
> scala                   /home/user/.local/share/jupyter/kernels/scala

## Running

1. After a successful `ssh` connection to your **BigVM instance** from a [MoVE](move.monash.edu) terminal do the following every time you need to run Jupyter Notebook (or just download and run `start-jupyter.sh`):

   `jupyter notebook --NotebookApp.token='' --no-browser`

2. On a **MoVE Terminal** map the ports (change the <username> and <VM-IP> with your username and BigVM IP address):

   `ssh -N -f -L localhost:8888:localhost:8888 <username>@<VM-IP>`

   .. and then enter your password.

3. On **MoVE Linux Desktop**, open Firefox and go to: [localhost:8888](localhost:8888)


Now, you should be able to see a Jupyter Notebook that supports Python 3 and Scala