#!/bin/bash
# broke - "is anything costing me money right now?"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'
BOLD='\033[1m'

AWS=$(which aws 2>/dev/null || echo "/usr/local/bin/aws")

echo -e "${BOLD}=== AWS Cost Check ===${NC}"
echo "$(date)"
echo ""

REGIONS=("us-east-1" "ap-south-1")

# ── 1. GLUE INTERACTIVE SESSIONS ──────────────────────────
echo -e "${BOLD}[1] Glue Interactive Sessions${NC}"
for region in "${REGIONS[@]}"; do
  sessions=$($AWS glue list-sessions --region "$region" \
    --query 'Ids' --output text 2>/dev/null)
  if [ -z "$sessions" ] || [ "$sessions" == "None" ]; then
    echo -e "  $region: ${GREEN}✓ None${NC}"
  else
    echo -e "  $region: ${RED}⚠ RUNNING: $sessions${NC}"
  fi
done

# ── 2. EC2 INSTANCES ───────────────────────────────────────
echo -e "\n${BOLD}[2] EC2 Instances${NC}"
for region in "${REGIONS[@]}"; do
  instances=$($AWS ec2 describe-instances --region "$region" \
    --filters "Name=instance-state-name,Values=running" \
    --query 'Reservations[*].Instances[*].[InstanceId,InstanceType]' \
    --output text 2>/dev/null)
  if [ -z "$instances" ]; then
    echo -e "  $region: ${GREEN}✓ None running${NC}"
  else
    echo -e "  $region: ${YELLOW}⚡ RUNNING:${NC}"
    echo "$instances" | while read line; do
      echo "    → $line"
    done
  fi
done

# ── 3. GLUE ETL JOBS RUNNING ───────────────────────────────
echo -e "\n${BOLD}[3] Glue ETL Jobs${NC}"
for region in "${REGIONS[@]}"; do
  job_names=$($AWS glue list-jobs --region "$region" \
    --query 'JobNames[]' --output text 2>/dev/null)
  found_running=0
  for job in $job_names; do
    runs=$($AWS glue get-job-runs --job-name "$job" --region "$region" \
      --query 'JobRuns[?JobRunState==`RUNNING`].[JobName,JobRunState]' \
      --output text 2>/dev/null)
    if [ -n "$runs" ]; then
      echo -e "  $region: ${RED}⚠ RUNNING: $runs${NC}"
      found_running=1
    fi
  done
  if [ $found_running -eq 0 ]; then
    echo -e "  $region: ${GREEN}✓ None${NC}"
  fi
done

# ── 4. RDS INSTANCES ───────────────────────────────────────
echo -e "\n${BOLD}[4] RDS Instances${NC}"
for region in "${REGIONS[@]}"; do
  rds=$($AWS rds describe-db-instances --region "$region" \
    --query 'DBInstances[?DBInstanceStatus==`available`].[DBInstanceIdentifier]' \
    --output text 2>/dev/null)
  if [ -z "$rds" ]; then
    echo -e "  $region: ${GREEN}✓ None${NC}"
  else
    echo -e "  $region: ${YELLOW}⚡ RUNNING: $rds${NC}"
  fi
done

# ── 5. MTD COST ────────────────────────────────────────────
echo -e "\n${BOLD}[5] Month-to-Date Cost${NC}"
cost=$($AWS ce get-cost-and-usage \
  --time-period Start=$(date +%Y-%m-01),End=$(date +%Y-%m-%d) \
  --granularity MONTHLY \
  --metrics "UnblendedCost" \
  --query 'ResultsByTime[0].Total.UnblendedCost.Amount' \
  --output text 2>/dev/null)
if [ -z "$cost" ] || [ "$cost" == "None" ]; then
  echo -e "  MTD spend: ${YELLOW}⚠ Could not fetch (check Cost Explorer permissions)${NC}"
else
  echo -e "  MTD spend: ${YELLOW}\$$cost USD${NC}"
fi

# ── 6. S3 BUCKETS + SIZES ──────────────────────────────────
echo -e "\n${BOLD}[6] S3 Buckets${NC}"

# FIXED: simpler bucket listing that actually works
buckets=$($AWS s3api list-buckets --query 'Buckets[].Name' --output text 2>/dev/null)

if [ -z "$buckets" ] || [ "$buckets" == "None" ]; then
  echo -e "  ${GREEN}✓ No buckets found${NC}"
else
  for bucket in $buckets; do
    # Get bucket region
    bucket_region=$($AWS s3api get-bucket-location \
      --bucket "$bucket" --query 'LocationConstraint' \
      --output text 2>/dev/null)
    [ "$bucket_region" == "None" ] || [ -z "$bucket_region" ] && bucket_region="us-east-1"
    
    # Get size
    size=$($AWS s3 ls "s3://$bucket" --recursive --summarize \
      --region "$bucket_region" 2>/dev/null | grep "Total Size" | awk '{print $3}')
    
    # Convert to human readable
    if [ -n "$size" ] && [ "$size" -gt 0 ] 2>/dev/null; then
      if [ "$size" -gt 1073741824 ] 2>/dev/null; then
        hsize="$(echo "scale=2; $size/1073741824" | bc) GB"
      elif [ "$size" -gt 1048576 ] 2>/dev/null; then
        hsize="$(echo "scale=2; $size/1048576" | bc) MB"
      elif [ "$size" -gt 1024 ] 2>/dev/null; then
        hsize="$(echo "scale=2; $size/1024" | bc) KB"
      else
        hsize="$size bytes"
      fi
    else
      hsize="empty / 0 bytes"
    fi
    
    echo "  → $bucket ($bucket_region): $hsize"
  done
fi

echo -e "\n${BOLD}=== Done ===${NC}"