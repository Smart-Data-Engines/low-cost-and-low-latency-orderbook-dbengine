#!/usr/bin/env bash
# What an acknowledged write survives when the power goes (#160).
#
# A power cut with the kernel doing the losing: dm-flakey over a loop device, switched to drop every
# write at the chosen moment - the way xfstests simulate one. What the filesystem shows after a
# remount is what had reached the device when the switch happened, and nothing more. A killed
# process cannot show this: the page cache survives a kill, and every other crash test in this
# repository kills processes.
#
# The run: ROWS inserts, a flush tick that puts them in a segment and appends its checkpoint, one
# more insert (under `every` its sync takes the checkpoint to the device), the cut, a kill, an
# unmount while writes are still being dropped, a remount, a restart, and a count.
#
#   scripts/power_cut.sh build/ob_tcp_server /path/to/workdir            # the cut
#   CONTROL=1 scripts/power_cut.sh build/ob_tcp_server /path/to/workdir  # the same run without it
#
# Measured before #160 was fixed: "rows 1" after the cut, "rows 201" for the control - segment
# files were never synced, and a synced checkpoint claimed them. Since the fix: "rows 201" both
# times. tests/integration/test_power_cut.py runs the same procedure in the battery, together with
# the cuts this script does not make: during a flush's segment sync, and before the first flush.
# POLICY (every), ROWS (200), FLUSH_MS (1000), PORT and METRICS change the run.
#
# Needs sudo for losetup, dmsetup and mount, and the dm-flakey module. Everything it creates lives
# under <workdir> - a 1 GiB image, a loop device, one dm target, one mount - and the cleanup trap
# removes all of it but the image and the node's log.
set -u
SERVER=$(readlink -f "$1")
WORK=$(readlink -f "$2")
IMG="$WORK/powercut.img"
MNT="$WORK/mnt"
DM=ob-powercut-160
PORT=${PORT:-19610}
METRICS=${METRICS:-19611}
FLUSH_MS=${FLUSH_MS:-1000}
POLICY=${POLICY:-every}
ROWS=${ROWS:-200}

mkdir -p "$WORK" "$MNT"
LOOP=""
NODE=""
cleanup() {
  [ -n "$NODE" ] && kill -9 "$NODE" 2>/dev/null
  mountpoint -q "$MNT" && sudo umount "$MNT"
  sudo dmsetup info "$DM" >/dev/null 2>&1 && sudo dmsetup remove "$DM"
  [ -n "$LOOP" ] && sudo losetup -d "$LOOP"
}
trap cleanup EXIT

allow_writes() { echo "0 $SECTORS flakey $LOOP 0 180 0"; }
drop_writes()  { echo "0 $SECTORS flakey $LOOP 0 0 180 1 drop_writes"; }
switch_table() {
  # --nolockfs, or suspend freezes the filesystem - which writes back every dirty page first and
  # turns the power cut into a clean shutdown.
  sudo dmsetup suspend --nolockfs "$DM" && "$1" | sudo dmsetup load "$DM" && sudo dmsetup resume "$DM"
}

rm -f "$IMG"
truncate -s 1G "$IMG"
LOOP=$(sudo losetup --find --show "$IMG") || exit 2
SECTORS=$(sudo blockdev --getsz "$LOOP")
sudo modprobe dm-flakey || exit 2
allow_writes | sudo dmsetup create "$DM" || exit 2
sudo mkfs.ext4 -q "/dev/mapper/$DM" || exit 2
sudo mount "/dev/mapper/$DM" "$MNT" || exit 2
sudo chown "$(id -u):$(id -g)" "$MNT"
DATA="$MNT/data"
mkdir -p "$DATA"

start_node() {
  "$SERVER" --port "$PORT" --metrics-port "$METRICS" --data-dir "$DATA" --fsync-policy "$POLICY" \
    --flush-interval-ms "$FLUSH_MS" --drain-timeout-ms 2000 >> "$WORK/node.log" 2>&1 &
  NODE=$!
  for _ in $(seq 1 100); do
    python3 - "$PORT" <<'EOF' && return 0
import socket, sys
try:
    s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=1)
    s.recv(4096); s.sendall(b"PING\n"); sys.exit(0 if b"PONG" in s.recv(4096) else 1)
except OSError:
    sys.exit(1)
EOF
    sleep 0.1
  done
  echo "node did not answer"; exit 2
}

client() {
  python3 - "$PORT" "$@" <<'EOF'
import socket, sys
port, cmd = int(sys.argv[1]), sys.argv[2]
s = socket.create_connection(("127.0.0.1", port), timeout=10)
r = s.makefile("rb")
while r.readline().strip():
    pass
if cmd == "insert":
    lo, hi = int(sys.argv[3]), int(sys.argv[4])
    bad = 0
    for p in range(lo, hi):
        s.sendall(f"INSERT SYM EX bid {p} 1 1\n".encode())
        line = r.readline().strip()
        if line == b"OK":
            r.readline()
        else:
            bad += 1
    print(f"inserted {hi - lo - bad} of {hi - lo}")
elif cmd == "count":
    s.sendall(b"SELECT * FROM 'SYM'.'EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999\n")
    prices = []
    while True:
        line = r.readline()
        if not line or line == b"\n":
            break
        parts = line.decode().split("\t")
        if len(parts) >= 7 and parts[0].isdigit():
            prices.append(int(parts[1]))
    print(f"rows {len(prices)} min {min(prices) if prices else '-'} max {max(prices) if prices else '-'}")
EOF
}

metric() {
  curl -s "http://127.0.0.1:$METRICS/metrics" | awk -v n="$1" '$1 == n || index($1, n "{") == 1 {print $2; exit}'
}

start_node
client insert 1000 $((1000 + ROWS))
# Wait for a tick to put them in a segment and append the checkpoint that claims them.
for _ in $(seq 1 100); do
  [ "$(metric ob_segment_count)" != "" ] && [ "$(metric ob_segment_count)" != "0" ] && break
  sleep 0.1
done
echo "segments after the tick: $(metric ob_segment_count)"
# One more write: under `every` its fsync takes the checkpoint to the device with it.
client insert 5000 5001
if [ "${CONTROL:-0}" = "1" ]; then
  echo "--- control: the same kill, the same unmount and mount, and no power cut ---"
else
  echo "--- the power cut ---"
  switch_table drop_writes
fi
kill -9 "$NODE"; wait "$NODE" 2>/dev/null; NODE=""
sudo umount "$MNT"                     # what it writes back now is dropped
switch_table allow_writes
sudo mount "/dev/mapper/$DM" "$MNT"
sudo chown "$(id -u):$(id -g)" "$MNT"
echo "files after the cut:"; find "$DATA" -type f -printf '%s %P\n' | sort -k2 | head -30
start_node
client count
grep -h '"level":"\(WARN\|ERROR\)"' "$WORK/node.log" | tail -5
grep -h "Replay after checkpoint\|WAL replay:" "$WORK/node.log" | tail -2
