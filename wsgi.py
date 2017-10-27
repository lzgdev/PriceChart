
import os
import sys

DIR_APPKKEV = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, DIR_APPKKEV)

from appPriceChart import app as application

if __name__ == "__main__":
	application.run()

