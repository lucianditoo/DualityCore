"""Reserved fixture; heartbeat and ignore-term are disabled in Phase 2C.

Only the fixed unexpected-exec canary remains. It returns 126 and writes at most
one short marker using nonblocking stdout. No subprocess, socket or signal action.
"""
import sys


def main():
    if sys.argv[1:] != ['--unexpected-exec']:
        raise SystemExit('IRONCLAD active process fixture disabled in Phase 2C')
    import os
    os.set_blocking(1, False)
    try:
        os.write(1, b'IRONCLAD_UNEXPECTED_EXEC\n')
    except BlockingIOError:
        pass
    return 126


if __name__ == '__main__':
    if not (sys.flags.isolated and sys.dont_write_bytecode):
        raise SystemExit('IRONCLAD requires python3 -I -B')
    raise SystemExit(main())
