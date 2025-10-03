#export UV_PYTHON=3.14.0rc3+freethreaded
export UV_PYTHON=3.13
export UV_PROJECT_ENVIRONMENT=.venv_$UV_PYTHON

if [ ! -d $UV_PROJECT_ENVIRONMENT ]; then
    uv venv
else
    echo $UV_PROJECT_ENVIRONMENT exists
fi 

source $UV_PROJECT_ENVIRONMENT/bin/activate
uv sync --no-install-project

export CCACHE_PROGRAM=ccache
export PYTHON_GIL=1
# export UV_CACHE_DIR="$(pwd)/.cache"
export CCACHE_BASEDIR=/home/ec2-user/git/duckdb-pythonf
#export CCACHE_NOHASHDIR=1
#export UV_NO_BUILD_ISOLATION=true
#export UV_NO_EDITABLE=true
#export SKBUILD_EDITABLE_MODE=redirect
export UV_BUILD_DIR=/home/ec2-user/git/duckdb-pythonf/build
export CMAKE_BUILD_DIR="/home/ec2-user/git/duckdb-pythonf/build"
export SKBUILD_BUILD_DIR="/home/ec2-user/git/duckdb-pythonf/build"
export SKBUILD_CMAKE_ARGS="-B/home/ec2-user/git/duckdb-pythonf/build"


export CMAKE_ARGS="-DPython3_EXECUTABLE=$UV_PROJECT_ENVIRONMENT/bin/python -DPython_EXECUTABLE=$UV_PROJECT_ENVIRONMENT/bin/python -DCMAKE_PREFIX_PATH=$UV_PROJECT_ENVIRONMENT"

uv sync --no-build-isolation -v  --reinstall

# uv sync --no-build-isolation --reinstall -vv --no-editable 

