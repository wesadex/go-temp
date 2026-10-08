#!/usr/bin/env bash
# Полный прогон бенчмарка брокера.
# Использование:
#   ./bench.sh                 # всё, по 100 млн сообщений, 1 повтор
#   TOTAL=20000000 ./bench.sh  # быстрее, для проверки
#   REPEAT=3 ./bench.sh        # по 3 прогона на точку (медиану считайте сами)
#   ./bench.sh single          # только одну серию: single | scale | skew
set -euo pipefail

BIN=${BIN:-./mqbench}
TOTAL=${TOTAL:-100000000}
REPEAT=${REPEAT:-1}
ONLY=${1:-all}

STAMP=$(date +%Y%m%d-%H%M%S)
OUT=${OUT:-bench-$STAMP}
mkdir -p "$OUT"
CSV="$OUT/results.csv"
LOG="$OUT/full.log"

echo "series,topics,producers,consumers,total,run,send_time,send_mmps,drain,total_time,total_mmps,ns_per_msg,empty_polls,empty_pct,gc_cycles,gc_pauses,alloc_mb,status" > "$CSV"

run() { # series topics producers consumers
  local series=$1 t=$2 p=$3 c=$4
  for r in $(seq 1 "$REPEAT"); do
    printf '%-7s topics=%-4s producers=%-6s consumers=%-6s run=%s ... ' "$series" "$t" "$p" "$c" "$r"
    local out
    out=$("$BIN" -total "$TOTAL" -topics "$t" -producers "$p" -consumers "$c" 2>&1) || true
    { echo "=== $series topics=$t producers=$p consumers=$c run=$r"; echo "$out"; echo; } >> "$LOG"
    echo "$out" | awk -v s="$series" -v t="$t" -v p="$p" -v c="$c" -v n="$TOTAL" -v r="$r" '
      /^запись:/          { st=$2; sm=$3 }
      /^дочитка хвоста:/  { dr=$3 }
      /^всего:/           { tt=$2; tm=$3; ns=$6 }
      /^холостых Recv:/   { ep=$3; pc=$4; gsub(/[(%]/, "", pc) }
      /^GC:/              { gc=$3; gp=$5; am=$7; gsub(/,/, "", gc); gsub(/,/, "", gp) }
      /^OK:/              { status="OK" }
      /^ОШИБКА:/          { status="FAIL" }
      END {
        if (status == "") status = "CRASH"
        printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
          s,t,p,c,n,r,st,sm,dr,tt,tm,ns,ep,pc,gc,gp,am,status
      }' >> "$CSV"
    tail -1 "$CSV" | awk -F, '{ printf "%s млн msg/s, %s ns/msg, холостых %s%%, %s\n", $11, $12, $14, $18 }'
  done
}

if [[ $ONLY == all || $ONLY == single ]]; then
  # 1. Один топик, одинаковое число продюсеров и консьюмеров.
  for n in 1 2 4 8 16 32 64; do run single 1 "$n" "$n"; done
fi

if [[ $ONLY == all || $ONLY == scale ]]; then
  # 2. 100 топиков, конкурентность ×100.
  for n in 100 200 400 800 1600 3200 6400 12800; do run scale 100 "$n" "$n"; done
fi

if [[ $ONLY == all || $ONLY == skew ]]; then
  # 3. 100 топиков, перекос продюсеры/консьюмеры в обе стороны.
  for pc in 150:100 100:150 200:100 100:200 400:100 100:400 \
            800:400 400:800 1600:400 400:1600 3200:800 800:3200; do
    run skew 100 "${pc%%:*}" "${pc##*:}"
  done
fi

echo
echo "Готово: $CSV (сводка), $LOG (полный вывод)"
if command -v column >/dev/null; then
  column -s, -t < "$CSV"
else
  cat "$CSV"
fi