#!/bin/bash

PYTHON="/usr/bin/python3"
SPARK="/opt/spark2/bin/spark-submit"

# Paths to your Python scripts
LISTENER_SCRIPT="/home/itversity/itversity-material/Zzcase-study/twitter_listener.py"
LANDING_SCRIPT="/home/itversity/itversity-material/Zzcase-study/spark_landing.py"
DIM_SCRIPT="/home/itversity/itversity-material/Zzcase-study/spark_Dim.py"
FACT_SCRIPT="/home/itversity/itversity-material/Zzcase-study/spark_Fact.py"

echo "************starting twitter listener************"
pgrep -f "$LISTENER_SCRIPT" >/dev/null || "$PYTHON" "$LISTENER_SCRIPT" &

sleep 5

echo "***********starting Spark stream***********"
pgrep -f "$LANDING_SCRIPT" >/dev/null || "$SPARK" "$LANDING_SCRIPT" &

echo "***********sleep for 1 min***********"
sleep 120

echo "***********starting create dim and fact table***********"
"$SPARK" --deploy-mode cluster "$DIM_SCRIPT"

"$SPARK" --deploy-mode cluster "$FACT_SCRIPT"









