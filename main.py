import subprocess
import sys
import time
import getpass


HADOOP_STREAMING_JAR = "/usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.6.jar"


def hdfs_available():
    try:
        subprocess.run(
            ["hdfs", "dfs", "-ls", "/"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
        return True
    except Exception:
        return False


def get_handles(base_name="counts"):
    user = getpass.getuser()
    use_hdfs = hdfs_available()

    paths = {
        "category": f"{base_name}_category.out",
        "token": f"{base_name}_token.out",
        "category_token": f"{base_name}_category_token.out",
    }

    if use_hdfs:
        print("[INFO] HDFS detected, writing outputs to HDFS", file=sys.stderr)
        hdfs_base = f"hdfs:///user/{user}"
        paths = {k: f"{hdfs_base}/{v}" for k, v in paths.items()}

        procs = {
            k: subprocess.Popen(
                ["hdfs", "dfs", "-put", "-f", "-", path],
                stdin=subprocess.PIPE,
                stderr=sys.stderr,
                text=True,
            )
            for k, path in paths.items()
        }

        handles = {k: proc.stdin for k, proc in procs.items()}
        wait_procs = list(procs.values())
    else:
        print("[INFO] HDFS not found, writing outputs to local files", file=sys.stderr)
        handles = {k: open(path, "w") for k, path in paths.items()}
        wait_procs = []

    return handles, wait_procs, paths, use_hdfs


def run_preprocessor(command, handles):
    print(f"Running {' '.join(command)}", file=sys.stderr)
    preprocessor = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=sys.stderr,
        text=True,
        bufsize=1,
    )

    n = "0"
    for line in preprocessor.stdout:
        key, value = line.strip().split("\t")
        if key.startswith("TC"):
            handles["category_token"].write(line)
        elif key.startswith("C"):
            _, _, category = key.split(".")
            handles["category"].write(f"{category}\t{value}\n")
        elif key.startswith("T"):
            _, token, _ = key.split(".")
            handles["token"].write(f"{token}\t{value}\n")
        elif key.startswith("D"):
            n = value

    preprocessor.wait()
    if preprocessor.returncode != 0:
        print("Job failed", file=sys.stderr)
        sys.exit()
    return n


def run_chisquared(paths, n, use_hdfs):
    command = [
        "python",
        "chisquared.py",
        paths["category_token"],
        "--category_counts",
        paths["category"],
        "--token_counts",
        paths["token"],
        "--n",
        n,
    ]

    if use_hdfs:
        command = [
            "python",
            "chisquared.py",
            "--hadoop-streaming-jar",
            HADOOP_STREAMING_JAR,
            "-r",
            "hadoop",
        ] + command[2:]

    print(f"Running {' '.join(command)}", file=sys.stderr)
    chisquared = subprocess.Popen(
        command,
        stdout=sys.stdout,
        stderr=sys.stderr,
        text=True,
        bufsize=1,
    )

    chisquared.wait()
    if chisquared.returncode != 0:
        print("Job failed", file=sys.stderr)
        sys.exit()


def main():
    print("Starting script", file=sys.stderr)
    start = time.time()

    handles, wait_procs, paths, use_hdfs = get_handles()

    try:
        preprocessor_cmd = ["python", "preprocessor.py"] + sys.argv[1:]
        if use_hdfs:
            preprocessor_cmd = [
                "python",
                "preprocessor.py",
                "--hadoop-streaming-jar",
                HADOOP_STREAMING_JAR,
                "-r",
                "hadoop",
            ] + sys.argv[1:]

        n = run_preprocessor(preprocessor_cmd, handles)

    finally:
        for handle in handles.values():
            handle.close()
        for proc in wait_procs:
            proc.wait()

    run_chisquared(paths, n, use_hdfs)

    end = time.time()
    print(f"Job execution time: {end - start:.2f} seconds", file=sys.stderr)


if __name__ == "__main__":
    main()
