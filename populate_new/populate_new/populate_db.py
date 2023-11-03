from migrate_detection import migrate_detection
import sys

if __name__ == "__main__":
    if len(sys.argv) == 4:
        dry_run = sys.argv[3] == "--dry-run"
    else:
        dry_run = False

    read_batch_size = int(sys.argv[1])
    write_batch_size = int(sys.argv[2])

    migrate_detection(read_batch_size, write_batch_size, dry_run)
