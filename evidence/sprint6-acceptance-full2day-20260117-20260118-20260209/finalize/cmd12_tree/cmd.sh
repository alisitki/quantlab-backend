set -euo pipefail
EVD_ROOT="evidence/sprint6-acceptance-full2day-20260117-20260118-20260209"
mkdir -p "$EVD_ROOT/inventory"

tree -a "$EVD_ROOT" >"$EVD_ROOT/inventory/tree.txt"

# Show first part
sed -n '1,200p' "$EVD_ROOT/inventory/tree.txt"
