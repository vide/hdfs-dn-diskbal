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
EOF
    
exit 2

}


# This function fill a global array with the disks
# mentioned in datanode's HDFS config file
function parseDisks() {

  log "Loaded datanode config file ${HDFS_CONF}"
  IFS="," read -r -a HDFS_DISKS < <(grep "<name>dfs.datanode.data.dir</name>"\
                                    "${HDFS_CONF}"\
                                    -A 1|tail -n1|sed -r "s_^[ ]*<value>(.*)</value>_\1_"|tr -d ' ')

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
  # we are only interested in the leaves of the directory tree, hence the -S 
  # switch, because data files are only in the leaves
  du -d2 -S current/BP-*/current/finalized/|sort -n -k1 |tail -n2|head -n1|awk '{print $2}'
}

function checkDatanodeRunning() {

  DNPID=$(pgrep -f -- "-Dproc_datanode") && \
      { log_error "Cannot do anything while datanode is running (PID: ${DNPID})";\
      exit 2; }
}

# moveSubdir FROM_DISK TO_DISK
function moveSubdir() {
  
  local SOURCE_DISK
  local DEST_DISK

  checkDatanodeRunning
  SOURCE_DISK=$1
  DEST_DISK=$2

  [[ "${SOURCE_DISK}" == "${DEST_DISK}" ]] && \
    { log_error "Cannot continue, source and destination disk are the same (${SOURCE_DISK})";\
      exit 2; }

  SUBDIR=$(getBiggestSubdir)

  DEST_SUBDIR=$(dirname ${SUBDIR})
  log "Moving ${SOURCE_DISK}/${SUBDIR} to ${DEST_DISK}/${DEST_SUBDIR}"
  echo mkdir -p "${DEST_DISK}/${SUBDIR}" # just in case dest dir does not exist
  echo rsync -a --remove-source-files "${SOURCE_DISK}/${SUBDIR}" "${DEST_DISK}/${DEST_SUBDIR}"

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
    log "${BALANCE_THRESHOLD}% threshold between ${BIGGEST_DISK} and ${SMALLEST_DISK} exceed, balancing data."
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
while [ $# -gt 0 ]  
do
    case "$1" in
        --hdfs-config|-c) HDFS_CONF="$2";         shift 2;;        
        --threshold|-t)   BALANCE_THRESHOLD="$2"; shift 2;;    
        *)                printHelp "Wrong parameter" ;;
    esac        
done


log "Starting DataNode local disks balancing"
parseDisks
balanceDisks
log "DataNode local disks balancing finished"
