"""Reserved synthetic gateway. Phase 2C deliberately provides no socket operations.

Re-enabling requires descriptor-bound paths, global exclusion and supervision.
"""
import sys


def main():
    raise SystemExit('IRONCLAD active gateway fixture disabled in Phase 2C')


if __name__ == '__main__':
    if not (sys.flags.isolated and sys.dont_write_bytecode):
        raise SystemExit('IRONCLAD requires python3 -I -B')
    main()
