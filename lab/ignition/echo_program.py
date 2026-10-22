"""Only executable payload permitted by the IGNITION lab. No input or arguments."""
import os
from pathlib import Path
import sys
import time


def main():
    if os.geteuid() == 0 or len(sys.argv) != 1:
        raise SystemExit(2)
    # Fixed finite output exercises draining and truncation on both pipes.
    sys.stdout.buffer.write(b'O' * 131072)
    sys.stdout.buffer.flush()
    sys.stderr.buffer.write(b'E' * 131072)
    sys.stderr.buffer.flush()
    time.sleep(0.4)
    Path('artifact.txt').write_bytes(b'IGNITION isolated-echo-v1\n')
    print('isolated-echo-v1 complete', flush=True)


if __name__ == '__main__':
    main()
