# Development Guide

## Operating System

* Install Linux and setup the project there.

* If you are on Windows, then install WSL:
	 
	 1. Eable WSL as a Windows Feature (see Programs and Features -> Windows Features on Control Panel).
     2. Install Ubuntu from Windows Store.

## Project Setup

1. Setup your IDE to use the system wide python interpreter or create a virtualenv and enable it in your IDE.

   * If you use IntelliJ as IDE and WSL, then go to Project Structure -> SDKs -> + Add Python SDK -> WSL -> set the interpreter path. (Run `which python3` in the WSL console to find out the interpreter path.)

2. Install the requirements from requirements.txt: `pip install -r requirements.txt`