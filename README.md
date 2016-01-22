Distributed Systems by MIT
==========

* source url : http://nil.csail.mit.edu/6.824/2015/

## Setup
* Config Golang running env.
* Or Running [script](https://github.com/haocs/dotfiles/blob/master/scripts/InstallLangs.sh) to install Golang and config PATH on Ubuntu.
* Set $GOPATH ``` export GOPATH="<project_root>/labs"```
* OR running the [init.sh](https://github.com/haocs/MIT_Distributed_Systems/blob/master/init.sh)

## Lab 1 
### Implement simple MapReduce framework
``` bash
	$ go run wc.go master kjv12.txt sequential
```
