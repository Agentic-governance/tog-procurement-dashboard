#!/bin/bash
# Daily procurement monitor cron script
# Add to crontab: 0 6 * * * /home/deploy/procurement_monitor/run_daily.sh
#
# Runs at 6:00 JST (21:00 UTC previous day) to catch overnight updates.

set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

# Load API key
if [ -f "$DIR/.env" ]; then
    export $(grep -v '^#' "$DIR/.env" | xargs)
fi

LOG="$DIR/logs/$(date +%Y-%m-%d).log"
mkdir -p "$DIR/logs" "$DIR/output"

echo "=== Procurement Monitor $(date) ===" >> "$LOG"

# Run monitor
python3 -u "$DIR/monitor.py" \
    --master "$DIR/procurement_url_master_v2.csv" \
    >> "$LOG" 2>&1

echo "=== Done $(date) ===" >> "$LOG"

# Cleanup: keep 30 days of logs and snapshots
find "$DIR/logs" -name "*.log" -mtime +30 -delete 2>/dev/null || true

# Compact DB monthly (first of month)
if [ "$(date +%d)" = "01" ]; then
    python3 -c "
import sqlite3
conn = sqlite3.connect('$DIR/monitor.db')
# Delete snapshots older than 30 days (keep latest per muni)
conn.execute('''
    DELETE FROM snapshots
    WHERE rowid NOT IN (
        SELECT MAX(rowid) FROM snapshots GROUP BY muni_code
    )
    AND fetched_at < datetime('now', '-30 days')
''')
conn.execute('VACUUM')
conn.commit()
conn.close()
print('DB compacted.')
" >> "$LOG" 2>&1
fi
