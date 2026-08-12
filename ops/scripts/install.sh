#!/usr/bin/env bash
# Install Quantum's fluxor-native runtime to /opt/quantum.
#
# Layout after install:
#   /opt/quantum/
#     bin/fluxor           # fluxor toolchain binary
#     bin/fluxor-linux     # fluxor linux runtime
#     target/bcm2712/modules/ → /opt/quantum/modules/
#     target/linux/modules/ → /opt/quantum/modules/
#     modules/             # .fmod artifacts (Quantum + Clustor + foundation)
#
# Then `make` configs at /etc/quantum/*.yaml and start with:
#   systemctl enable --now quantum

set -euo pipefail

PREFIX=${PREFIX:-/opt/quantum}
CONFIG_DIR=${CONFIG_DIR:-/etc/quantum}
DATA_DIR=${DATA_DIR:-/var/lib/quantum}
LOG_DIR=${LOG_DIR:-/var/log/quantum}
USER_NAME=${USER_NAME:-quantum}

QUANTUM_ROOT=$(cd "$(dirname "$0")/../.." && pwd)
FLUXOR_ROOT=${QUANTUM_ROOT}/deps/fluxor

if [ "$(id -u)" -ne 0 ]; then
    echo "ERROR: install.sh must run as root (sudo)" >&2
    exit 1
fi

echo "==> creating system user ${USER_NAME}"
if ! id -u "${USER_NAME}" >/dev/null 2>&1; then
    useradd -r -s /usr/sbin/nologin -d "${DATA_DIR}" "${USER_NAME}"
fi

echo "==> creating directories"
install -d -o "${USER_NAME}" -g "${USER_NAME}" "${PREFIX}/bin" "${PREFIX}/modules" \
    "${PREFIX}/target/bcm2712" "${PREFIX}/target/linux/quantum-pi5" \
    "${CONFIG_DIR}" "${DATA_DIR}" "${LOG_DIR}"

echo "==> installing fluxor binaries"
install -m 0755 "${FLUXOR_ROOT}/target/aarch64-unknown-linux-gnu/release/fluxor" \
    "${PREFIX}/bin/fluxor"
install -m 0755 "${FLUXOR_ROOT}/target/aarch64-unknown-linux-gnu/release/fluxor-linux" \
    "${PREFIX}/bin/fluxor-linux"

echo "==> installing .fmod artifacts"
cp "${FLUXOR_ROOT}/target/fluxor/bcm2712/modules/"*.fmod "${PREFIX}/modules/"
chown -R "${USER_NAME}:${USER_NAME}" "${PREFIX}/modules"

echo "==> setting up target/ symlinks"
ln -sfn "${PREFIX}/modules" "${PREFIX}/target/bcm2712/modules"
ln -sfn "${PREFIX}/modules" "${PREFIX}/target/linux/modules"

# Graphs are shadow-tracked (standards/test-tracking.md), so a clone of the
# primary repository has no examples/ — an operator installs the graph they
# actually run. CONFIGS names it/them; the defaults are what examples/ ships
# when the shadow checkout is present, and a missing one is not fatal.
echo "==> installing configs"
CONFIGS=${CONFIGS:-"examples/rig/pi5.yaml examples/linux/minimal.yaml examples/linux/full.yaml"}
staged=0
for c in $CONFIGS; do
    src="$c"
    [ -f "$src" ] || src="${QUANTUM_ROOT}/$c"
    if [ -f "$src" ]; then
        cp "$src" "${CONFIG_DIR}/"
        staged=$((staged + 1))
    else
        echo "    skip: $c not present"
    fi
done
if [ "$staged" -eq 0 ]; then
    echo "    no graph installed — pass CONFIGS=/path/to/graph.yaml" >&2
fi

echo "==> installing systemd unit"
install -m 0644 "${QUANTUM_ROOT}/ops/systemd/quantum.service" \
    /etc/systemd/system/quantum.service
systemctl daemon-reload

echo ""
echo "==> install complete. To start:"
echo "    systemctl enable --now quantum"
echo ""
echo "==> to switch graph configs:"
echo "    edit /etc/systemd/system/quantum.service.d/override.conf"
echo "    set Environment=QUANTUM_CONFIG=/etc/quantum/<config>.yaml"
echo ""
echo "==> to inspect:"
echo "    journalctl -u quantum -f"
