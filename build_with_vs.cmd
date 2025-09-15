@echo off
echo Setting up x64 build environment...
if not defined VS_ENV_LOADED (
      echo Setting up Visual Studio environment...
      call "C:\Program Files\Microsoft Visual Studio\2022\Enterprise\VC\Auxiliary\Build\vcvars64.bat"
      set VS_ENV_LOADED=1
  ) else (
      echo Visual Studio environment already loaded
  )
echo Verifying compiler paths:
where cl
echo VCINSTALLDIR=%VCINSTALLDIR%
echo Platform=%Platform%

where sccache.exe.disabled >nul 2>&1 && for /f %%i in ('where sccache.exe.disabled') do ren "%%i" "sccache.exe"
rem where sccache >nul 2>&1 && for /f %%i in ('where sccache') do ren "%%i" "sccache.exe.disabled"

REM Create comprehensive cache buster for Enterprise edition
for /f %%i in ('cl 2^>^&1 ^| findstr "Version"') do set CL_VERSION=%%i
set SCCACHE_C_CUSTOM_CACHE_BUSTER=Enterprise-v17.14-cl_%CL_VERSION%-win32-e-cores-disabled-2
echo Cache buster: %SCCACHE_C_CUSTOM_CACHE_BUSTER%


set SCCACHE_BUCKET=paul-cache-3873478kjgvnmvcxq
set SCCACHE_REGION=us-east-1
set CIBW_ARCHS=AMD64
set CIBW_BUILD=cp314t-win_amd64
rem  CMAKE_BUILD_PARALLEL_LEVEL=6
set CIBW_ENVIRONMENT_WINDOWS=SKBUILD_BUILD_DIR=build/cp314t-amd64 CMAKE_VERBOSE_MAKEFILE=ON PYTHON_GIL=1 SCCACHE_BUCKET=paul-cache-3873478kjgvnmvcxq SCCACHE_REGION=us-east-1 SCCACHE_C_CUSTOM_CACHE_BUSTER=%SCCACHE_C_CUSTOM_CACHE_BUSTER%
set CIBW_BEFORE_BUILD=echo "Hi"
set CIBW_BUILD_VERBOSITY=3
set CIBW_TEST_SKIP=*

echo Archiving old wheels...
if exist "wheelhouse\duckdb-*.whl" (
    if not exist "wheelhouse\old" mkdir "wheelhouse\old"
    move "wheelhouse\duckdb-*.whl" "wheelhouse\old\" >nul 2>&1
)

uv run --no-sync --with cibuildwheel cibuildwheel . --output-dir wheelhouse > BUILDLOG.md 2>&1
set BUILD_EXIT_CODE=%ERRORLEVEL%

if %BUILD_EXIT_CODE% equ 0 (
    if exist "wheelhouse\*.whl" (
        echo Installing built wheel...
        call wheelhouse\.venv\scripts\activate.bat
        for %%f in (wheelhouse\duckdb-*.whl) do (
            echo Installing newly built wheel: %%f
            uv pip install "%%f" --force-reinstall
        )

        if %ERRORLEVEL% equ 0 (
            echo Running crash test...
            echo.
            echo --- Debug Output ---
            set PYTHON_GIL=1
            wheelhouse\.venv\Scripts\python.exe -c "print(\"Started\"); import duckdb; print(\"Imported\"); conn = duckdb.connect(); conn.execute(\"SET errors_as_json='true'\"); conn.execute('SELECT * FROM nonexistent_table')"

            echo.
            echo --- Pytest Test ---
            set PYTHON_GIL=1
            wheelhouse\.venv\Scripts\python.exe -m pytest -q -s tests\fast\test_json_logging.py::test_json_syntax_error -v

        ) else (
            echo Failed to install wheel
        )
    ) else (
        echo Build succeeded but no wheel files found
        echo Listing wheelhouse directory:
        dir wheelhouse
    )
) else (
    echo BUILD FAILED - Exit code: %BUILD_EXIT_CODE%
    echo Build log contents:
    type BUILDLOG.md
    exit /b %BUILD_EXIT_CODE%
)