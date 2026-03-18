#!/bin/bash

set -e

MAX_PARALLEL=${PARALLEL:-8}
URL_PREFIX="https://d37ci6vzurychx.cloudfront.net/trip-data"
START_TIME=$(date +%s)
JOBS=0

download_file() {
  local URL=$1
  local LOCAL_PATH=$2
  echo "Downloading ${URL} to ${LOCAL_PATH}"
  mkdir -p $(dirname ${LOCAL_PATH})
  wget -q ${URL} -O ${LOCAL_PATH} && echo "✅ Done: ${LOCAL_PATH}" || echo "❌ Failed: ${URL}"
}

for TAXI_TYPE in "$@"; do
  [[ "$TAXI_TYPE" =~ ^[0-9]{4}$ ]] && continue

  for YEAR in "$@"; do
    [[ ! "$YEAR" =~ ^[0-9]{4}$ ]] && continue

    for MONTH in {1..12}; do
      FMONTH=$(printf "%02d" ${MONTH})
      URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet"
      LOCAL_PATH="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}/${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.parquet"

      download_file "${URL}" "${LOCAL_PATH}" &
      JOBS=$((JOBS + 1))

      if (( JOBS >= MAX_PARALLEL )); then
        wait
        JOBS=0
      fi
    done
  done
done

wait

END_TIME=$(date +%s)
echo "⏱️  Total time: $((END_TIME - START_TIME))s"