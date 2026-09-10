import sys, time
# First-touch every page of a large anonymous buffer, then release it. The host
# pages stay assigned to the guest, so the page cache the import fills next is
# no longer faulting them in from the host for the first time.
mib = int(sys.argv[1])
t = time.perf_counter()
buf = bytearray(mib * 1024 * 1024)
for off in range(0, len(buf), 4096):
    buf[off] = 1
del buf
print(f"prefault {mib} MiB in {(time.perf_counter() - t) * 1000:.0f} ms", flush=True)
