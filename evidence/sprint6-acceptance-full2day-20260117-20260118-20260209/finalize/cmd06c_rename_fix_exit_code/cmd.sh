set -euo pipefail
NEW_ROOT="evidence/sprint6-acceptance-full2day-20260117-20260118-20260209"
OLD_ROOT="evidence/sprint6-acceptance-full2day-__PENDING__-__PENDING__-20260209"

echo "new_root_exists=$(if [ -d "$NEW_ROOT" ]; then echo yes; else echo no; fi)"
echo "old_root_exists=$(if [ -d "$OLD_ROOT" ]; then echo yes; else echo no; fi)"

ls -la "$NEW_ROOT/finalize/cmd06_rename_root_to_window" || true

# Fill missing exit_code.txt for the mv step (time -v is the source of truth here)
if [ -f "$NEW_ROOT/finalize/cmd06_rename_root_to_window/time-v.log" ]; then
  mv_ec=$(sed -n 's/^[[:space:]]*Exit status: \([0-9][0-9]*\)$/\1/p' \
    "$NEW_ROOT/finalize/cmd06_rename_root_to_window/time-v.log" | tail -n 1)
  if [ -n "$mv_ec" ]; then
    printf '%s\n' "$mv_ec" >"$NEW_ROOT/finalize/cmd06_rename_root_to_window/exit_code.txt"
    echo "rename_mv_exit_code_written=$mv_ec"
  else
    echo "rename_mv_exit_code_parse_failed"
  fi
else
  echo "rename_mv_missing_time_v_log"
fi
