#!/bin/bash

#!/bin/bash

# 1. Move to the directory first so any relative paths (like saving files) work correctly
cd /home/werowe/Documents/deepstate

# 2. Run the specific python executable directly using its full path
#    This automatically uses the libraries in 'tf_env' without needing 'source activate'
/home/werowe/tf_env/bin/python /home/werowe/Documents/deepstate/download_deepstate.py

