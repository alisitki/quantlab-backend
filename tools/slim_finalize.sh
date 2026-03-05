#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "usage: $0 <pack_name> <pack_dir> [archive_root]" >&2
  exit 2
fi

PACK_NAME="$1"
PACK_DIR_INPUT="$2"
ARCHIVE_ROOT_INPUT="${3:-/home/deploy/quantlab-evidence-archive/$(date -u +%Y%m%d)_slim}"

if [[ -z "${PACK_NAME}" ]]; then
  echo "[slim_finalize] pack_name is empty" >&2
  exit 3
fi

if [[ ! -d "${PACK_DIR_INPUT}" ]]; then
  echo "[slim_finalize] pack_dir not found: ${PACK_DIR_INPUT}" >&2
  exit 4
fi

PACK_DIR_ABS="$(readlink -f "${PACK_DIR_INPUT}")"
PACK_DIR_NAME="$(basename "${PACK_DIR_ABS}")"
EVIDENCE_DIR_ABS="$(dirname "${PACK_DIR_ABS}")"
ARCHIVE_ROOT_ABS="$(readlink -m "${ARCHIVE_ROOT_INPUT}")"
ARCHIVE_DEST_ABS="${ARCHIVE_ROOT_ABS}/${PACK_NAME}"

TAR_PATH="${EVIDENCE_DIR_ABS}/${PACK_NAME}.tar.gz"
SHA_PATH="${TAR_PATH}.sha256"
SHA_VERIFY_PATH="${EVIDENCE_DIR_ABS}/${PACK_NAME}.sha_verify.txt"
MOVED_TO_PATH="${EVIDENCE_DIR_ABS}/${PACK_NAME}.moved_to.txt"

if [[ "${PACK_DIR_NAME}" != "${PACK_NAME}" ]]; then
  echo "[slim_finalize] pack_name mismatch: name=${PACK_NAME} dir_name=${PACK_DIR_NAME}" >&2
  exit 5
fi

if [[ -e "${ARCHIVE_DEST_ABS}" ]]; then
  echo "[slim_finalize] archive destination already exists: ${ARCHIVE_DEST_ABS}" >&2
  exit 6
fi

# Guard: ensure output targets are exactly in evidence dir with expected names.
if [[ "$(dirname "${TAR_PATH}")" != "${EVIDENCE_DIR_ABS}" ]] || [[ "$(basename "${TAR_PATH}")" != "${PACK_NAME}.tar.gz" ]]; then
  echo "[slim_finalize] tar target guard failed: ${TAR_PATH}" >&2
  exit 7
fi
if [[ "$(dirname "${SHA_PATH}")" != "${EVIDENCE_DIR_ABS}" ]] || [[ "$(basename "${SHA_PATH}")" != "${PACK_NAME}.tar.gz.sha256" ]]; then
  echo "[slim_finalize] sha target guard failed: ${SHA_PATH}" >&2
  exit 8
fi
if [[ "$(dirname "${SHA_VERIFY_PATH}")" != "${EVIDENCE_DIR_ABS}" ]] || [[ "$(basename "${SHA_VERIFY_PATH}")" != "${PACK_NAME}.sha_verify.txt" ]]; then
  echo "[slim_finalize] sha_verify target guard failed: ${SHA_VERIFY_PATH}" >&2
  exit 13
fi
if [[ "$(dirname "${MOVED_TO_PATH}")" != "${EVIDENCE_DIR_ABS}" ]] || [[ "$(basename "${MOVED_TO_PATH}")" != "${PACK_NAME}.moved_to.txt" ]]; then
  echo "[slim_finalize] moved_to target guard failed: ${MOVED_TO_PATH}" >&2
  exit 9
fi

# Build tar and sha (deterministic gzip header).
tar -C "${EVIDENCE_DIR_ABS}" -cf - "${PACK_NAME}" | gzip -n > "${TAR_PATH}"
sha256sum "${TAR_PATH}" > "${SHA_PATH}"
sha256sum -c "${SHA_PATH}" | tee "${SHA_VERIFY_PATH}"

mkdir -p "${ARCHIVE_ROOT_ABS}"
mv "${PACK_DIR_ABS}" "${ARCHIVE_DEST_ABS}"
cp "${SHA_VERIFY_PATH}" "${ARCHIVE_DEST_ABS}/sha_verify.txt"
printf '%s\n' "${ARCHIVE_DEST_ABS}" > "${MOVED_TO_PATH}"

# Post-guards.
if [[ ! -f "${TAR_PATH}" || ! -f "${SHA_PATH}" || ! -f "${SHA_VERIFY_PATH}" || ! -f "${MOVED_TO_PATH}" ]]; then
  echo "[slim_finalize] expected final triple missing" >&2
  exit 10
fi
if [[ -d "${PACK_DIR_ABS}" ]]; then
  echo "[slim_finalize] unpacked pack still exists in repo: ${PACK_DIR_ABS}" >&2
  exit 11
fi
if [[ ! -d "${ARCHIVE_DEST_ABS}" ]]; then
  echo "[slim_finalize] archive destination missing after move: ${ARCHIVE_DEST_ABS}" >&2
  exit 12
fi
if [[ ! -f "${ARCHIVE_DEST_ABS}/sha_verify.txt" ]]; then
  echo "[slim_finalize] archive sha_verify missing: ${ARCHIVE_DEST_ABS}/sha_verify.txt" >&2
  exit 14
fi

echo "[slim_finalize] tar=${TAR_PATH}"
echo "[slim_finalize] sha=${SHA_PATH}"
echo "[slim_finalize] sha_verify=${SHA_VERIFY_PATH}"
echo "[slim_finalize] moved_to=${MOVED_TO_PATH}"
