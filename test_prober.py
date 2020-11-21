import subprocess

if __name__ == "__main__":
    proc = subprocess.Popen(["/Users/maxmouchet/Clones/github.com/dioptra-io/diamond-miner-prober/build/diamond-miner-prober", "-i", "NONE", "-o", "tmp", "-r", "100000", "-L", "trace"], stdin=subprocess.PIPE)
    for i in range(1000):
        proc.stdin.write('8.8.8.8,24000,33434,10\n'.encode('ascii'))
