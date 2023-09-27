import sys, getopt, os, math, io, re, subprocess, functools
from datetime import date
from datetime import timedelta

def run_analysis(directory, fromDate: date, toDate: date, deltaTime: int):
    delta = toDate - fromDate
    subfolders = [f.path for f in os.scandir(directory) if f.is_dir()]

    # Display the CSV header
    print(functools.reduce(lambda x, y: x + "," + y, subfolders, "date"))

    # Display one row in the CSV per time ( to have a good chart )
    for m in range(math.ceil(delta.days / deltaTime)):
        rowDateStart = fromDate + timedelta(days=m * deltaTime)
        rowDateEnd = rowDateStart + timedelta(days=(m + 1) * deltaTime)
        rowStr = str(rowDateStart) + ","

        for folder in subfolders:
            process = subprocess.Popen(
                [
                    "git",
                    "log",
                    "--numstat", 
                    "--format=",
                    "--since=" + rowDateStart.isoformat(),
                    "--before=" + rowDateEnd.isoformat(),
                    folder,
                ],
                stdout=subprocess.PIPE
            )
            added=0
            deleted=0
            for nline in io.TextIOWrapper(process.stdout, encoding="utf-8"):  # or another encoding
                 # Sum all the files modifications
                match = re.search('^(\d+)\s+(\d+)\s+(.*)\s*$',nline) 
                added =+  int(match.group(1))
                deleted +=   int(match.group(2))

            rowStr += str(added)+ ","
        print(rowStr)


def main(argv):
    directory = "."
    toDate = date.today()
    fromDate = toDate - timedelta(days=365)
    delta = 30

    opts, args = getopt.getopt(argv, "hd:f:t:d:", ["dir=", "from=", "to=", "delta="])
    for opt, arg in opts:
        if opt == "-h":
            print("repo_heatpoints_analysis.py -d <inputDir> -o <outputfile>")
            sys.exit()
        elif opt in ("-d", "--dir"):
            directory = arg
        elif opt in ("-f", "--from"):
            fromDate = date.fromisoformat(arg)
        elif opt in ("-t", "--to"):
            toDate = date.fromisoformat(arg)
        elif opt in ("-d", "--delta"):
            delta = int(arg)
    if fromDate > toDate:
        raise "From must be before To"

    run_analysis(directory, fromDate, toDate, delta)


if __name__ == "__main__":
    main(sys.argv[1:])
