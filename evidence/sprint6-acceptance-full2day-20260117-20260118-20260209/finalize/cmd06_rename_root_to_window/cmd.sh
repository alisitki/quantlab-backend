set -euo pipefail
if [ -e "evidence/sprint6-acceptance-full2day-20260117-20260118-20260209" ]; then
  echo "DEST_EXISTS: evidence/sprint6-acceptance-full2day-20260117-20260118-20260209"
  exit 2
fi
mv "evidence/sprint6-acceptance-full2day-__PENDING__-__PENDING__-20260209" "evidence/sprint6-acceptance-full2day-20260117-20260118-20260209"
echo "RENAMED: evidence/sprint6-acceptance-full2day-__PENDING__-__PENDING__-20260209 -> evidence/sprint6-acceptance-full2day-20260117-20260118-20260209"
