import subprocess
import sys

markers = ["mariadb", "mysql", "postgresql", "mssql", "sqlite3", "mongodb"]  # Corrected spelling


def test():
    if len(sys.argv) == 1:
        args = []
    else:
        args = sys.argv[1:]

    stop = ""
    if "x" in args:
        stop = "x"
        args.remove("x")
    if len(args) == 1:
        include_markers = "-m " + args[0]
    elif len(args) > 1:
        include_markers = "-m " + " or ".join(args)
    else:
        include_markers = ""

    print(include_markers)

    print(["pytest", "tests", "-vvvv" + stop, "--cov", "--durations=5", include_markers])

    subprocess.run(["pytest", "tests", "-vvvv", "--cov", "--durations=5", include_markers])


if __name__ == "__main__":
    test()
