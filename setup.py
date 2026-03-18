from setuptools import setup  # type: ignore


def main():

    setup(
        name="hpc_funcs",
        version="0",
        python_requires=">=3.6",
        install_requires=[],
        packages=["hpc_funcs"],
        package_dir={"": "src"},
    )

    return


main()
