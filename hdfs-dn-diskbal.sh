#!/usr/bin/env bash

function log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') ${*}" # >> "${LOGFILE:-/tmp/hdfs-dn-diskbal.log}"
}

function log_error() {

    echo "${*}" >/dev/stderr
}

function printHelp() {

cat <<EOF
$@

Usage: $(basename $0) 
        --hdfs-config|-c FILENAME  Specify the datanode's HDFS configuration file
                                   Default: /etc/hadoop/conf/hdfs-site.xml
        --threshold|-t PERCENTAGE  Tolerate up to this % of difference between 2 disks.
                                   Integer value. Default: 5                                   
        --force|-f                 Force running when executed as root
EOF
    
exit 2

}


# This function fill a global array with the disks
# mentioned in datanode's HDFS config file
function parseDisks() {

  [ -f "${HDFS_CONF}" ] || { log "HDFS config file ${HDFS_CONF} does not exist. Exiting."; exit 2; }
  log "Loaded datanode config file ${HDFS_CONF}"
  IFS="," read -r -a HDFS_DISKS < <(grep "<name>dfs.datanode.data.dir</name>"\
                                    "${HDFS_CONF}"\
                                    -A 1|tail -n1|sed -r "s_^[ ]*<value>(.*)</value>_\1_"|tr -d ' ')

  [ ${#HDFS_DISKS[@]} -lt 2 ] && { log "We need at least 2 disks to balance. Found only ${#HDFS_DISKS[@]} in config file.";
                                   exit 2; }
  log "Data disks to be balanced: ${HDFS_DISKS[*]}"
}

function getUsedDisk() {

  [[ "${1}" == "most" ]] && ORDER=""
  [[ "${1}" == "least" ]] && ORDER="r"

  FIELD=2
  [[ "${2}" == "size" ]] && FIELD=1

  for d in "${HDFS_DISKS[@]}"
  do
    # if no mounted disk is found, fall back to root
    df -ml --output="used,target" | grep "${d}" \
    || { df -ml --output="used,target" |grep -E "/$"|sed -r "s_/\$_${d}_"; }
  done | sort -n${ORDER} -k1 | tail -n1 \
  | awk "{print \$${FIELD}}"
}


# returns the RELATIVE path to the biggest subdir in the most used data disk
function getBiggestSubdir() {

  BIGGEST_DISK=$(getUsedDisk most)
  cd "${BIGGEST_DISK}"
  # find the biggest 1st level "subdirNN"
  find current/BP-*/current/finalized/  -mindepth 1 -maxdepth 1 -type d -print0 \
  | xargs -0 -n 8 du -d0|sort -k1 -n|tail -n1|awk '{print $2}'
}

function checkDatanodeRunning() {

  DNPID=$(pgrep -f -- "-Dproc_datanode") && \
      { log_error "Cannot do anything while datanode is running (PID: ${DNPID})";\
      exit 2; }
}

function checkRunningUser() {

  id|grep -q "uid=0" && { log "Running as root user, exiting. Use --force to override"; exit 2; }
}

# moveSubdir FROM_DISK TO_DISK
function moveSubdir() {
  
  local SOURCE_DISK
  local DEST_DISK

  SOURCE_DISK=$1
  DEST_DISK=$2

  [[ "${SOURCE_DISK}" == "${DEST_DISK}" ]] && \
    { log_error "Cannot continue, source and destination disk are the same (${SOURCE_DISK})";\
      exit 2; }

  SUBDIR=$(getBiggestSubdir)

  DEST_SUBDIR=$(dirname ${SUBDIR})
  log "Moving ${SOURCE_DISK}/${SUBDIR} to ${DEST_DISK}/${DEST_SUBDIR}"
  mkdir -p "${DEST_DISK}/${SUBDIR}" # just in case dest dir does not exist
  rsync -a --remove-source-files "${SOURCE_DISK}/${SUBDIR}" "${DEST_DISK}/${DEST_SUBDIR}"

}

function isThresholdTraspassed() {

  SMALL=$1
  BIG=$2

  (( (BIG-SMALL) * 100 / (BIG+SMALL) > BALANCE_THRESHOLD )) && return 0 || return 1
}

function balanceDisks() {

  local BIGGEST_DISK
  local SMALLEST_DISK
  local BIGGEST_DISK_SIZE
  local SMALLEST_DISK_SIZE

  BIGGEST_DISK_SIZE=$(getUsedDisk most size)
  SMALLEST_DISK_SIZE=$(getUsedDisk least size)

  while isThresholdTraspassed "$SMALLEST_DISK_SIZE" "$BIGGEST_DISK_SIZE"
  do
    BIGGEST_DISK="$(getUsedDisk most)"
    SMALLEST_DISK="$(getUsedDisk least)"
    log "${BALANCE_THRESHOLD}% threshold between ${BIGGEST_DISK} and ${SMALLEST_DISK} exceeded, balancing data."
    moveSubdir "$BIGGEST_DISK" "$SMALLEST_DISK"
    BIGGEST_DISK_SIZE="$(getUsedDisk most size)"
    SMALLEST_DISK_SIZE="$(getUsedDisk least size)"
  done
  
  log "No disks are exceeding the balance threshold."
  return 0
}

# main starts here
HDFS_CONF="/etc/hadoop/conf/hdfs-site.xml"
BALANCE_THRESHOLD=5 # in %
FORCE_RUN=0
while [ $# -gt 0 ]  
do
    case "$1" in
        --hdfs-config|-c) HDFS_CONF="$2";         shift 2;;
        --threshold|-t)   BALANCE_THRESHOLD="$2"; shift 2;;
        --force|-f)       FORCE_RUN="1";          shift 1;;
        *)                printHelp "Wrong parameter" ;;
    esac        
done

checkDatanodeRunning
[ ${FORCE_RUN} -eq 0 ] && checkRunningUser

log "Starting DataNode local disks balancing"
parseDisks
balanceDisks
log "DataNode local disks balancing finished"
