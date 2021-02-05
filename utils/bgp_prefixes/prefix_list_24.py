import sys
from pathlib import Path

if __name__ == "__main__":
    input_path = Path(sys.argv[1])
    output_path = input_path.with_suffix(".prefix-list.txt")
    prefixes_24 = set()
    with input_path.open() as f:
        for line in f:
            network, _ = line.rsplit(".", maxsplit=1)
            network += ".0"
            prefixes_24.add(network)
    with output_path.open("w") as f:
        f.writelines((prefix + "/24\n" for prefix in prefixes_24))
