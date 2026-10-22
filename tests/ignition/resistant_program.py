"""Fixed test fixture ONLY: finite SIGTERM-resistant child, no arguments/input."""
import os
from pathlib import Path
import signal
import time

if os.geteuid() == 0:
    raise SystemExit(2)
signal.signal(signal.SIGTERM, signal.SIG_IGN)
Path('ready').write_text('ready')
time.sleep(1.0)
Path('artifact.txt').write_bytes(b'IGNITION isolated-echo-v1\n')
