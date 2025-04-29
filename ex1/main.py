import subprocess
import sys
import time
import getpass


HADOOP_STREAMING_JAR = "/usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.6.jar"


def hdfs_available():
    """
    Check if HDFS is available.    
    """
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
    """
    Provide handles depending on user and HDFS availability   
    """
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


def run_preprocessor(use_hdfs, handles, wait_procs):
    """
    Start Preprocessor MRJob.
    """
    command = ["python", "preprocessor.py"] + sys.argv[1:]
    if use_hdfs:
        command = [
            "python",
            "preprocessor.py",
            "--hadoop-streaming-jar",
            HADOOP_STREAMING_JAR,
            "-r",
            "hadoop",
        ] + sys.argv[1:]

    print(f"Running {' '.join(command)}", file=sys.stderr)
    preprocessor = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=sys.stderr,
        text=True,
        bufsize=1,
    )

    n = 0
    for line in preprocessor.stdout:
        key, value = line.strip().split("\t")
        if key.startswith("TC"):
            handles["category_token"].write(line)
        elif key.startswith("C"):
            _, _, category = key.split(".")
            handles["category"].write(f"{category}\t{value}\n")
            n += int(value)
        elif key.startswith("T"):
            _, token, _ = key.split(".")
            handles["token"].write(f"{token}\t{value}\n")

    preprocessor.wait()
    if preprocessor.returncode != 0:
        print("Job failed", file=sys.stderr)
        sys.exit()

    for handle in handles.values():
        handle.close()
    for proc in wait_procs:
        proc.wait()

    return str(n)


def run_chisquared(paths, n, use_hdfs):
    """
    Start Chisquared MRJob.
    """
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
        stdout=subprocess.PIPE,
        stderr=sys.stderr,
        text=True,
        bufsize=1,
    )

    all_tokens = set()
    result = {}
    for line in chisquared.stdout:
        category, value = line.split("\t")
        value = value.strip()
        result[category] = value
        for pair in value.split(" "):
            token, chi2 = pair.split(":")
            all_tokens.add(token)

    for category, value in sorted(result.items()):
        print(f"{category}\t{value}")
    print(" ".join(sorted(all_tokens)))

    chisquared.wait()
    if chisquared.returncode != 0:
        print("Job failed", file=sys.stderr)
        sys.exit()


def main():
    print("Starting script", file=sys.stderr)
    start = time.time()

    handles, wait_procs, paths, use_hdfs = get_handles()

    n = run_preprocessor(use_hdfs, handles, wait_procs)

    mid = time.time()
    print(f"Preprocessor execution time: {mid - start:.2f} seconds", file=sys.stderr)

    run_chisquared(paths, n, use_hdfs)

    end = time.time()
    print(f"Chisquared execution time: {end - mid:.2f} seconds", file=sys.stderr)
    print(f"Total execution time: {end - start:.2f} seconds", file=sys.stderr)


if __name__ == "__main__":
    main()
