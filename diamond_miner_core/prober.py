from subprocess import PIPE, Popen


# TODO: Raise error if the subprocess crash.
class Prober:
    def __init__(self, exe="diamond-miner-prober", options=[]):
        self.exe = exe
        self.options = options
        self.process = None

    def open(self):
        self.process = Popen([self.exe, *self.options], stdin=PIPE)

    def close(self):
        self.process.stdin.close()
        self.process.wait()

    def send(self, probe):
        s = probe.to_csv() + "\n"
        self.process.stdin.write(s.encode("ascii"))

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, type, value, traceback):
        self.close()
        return False
