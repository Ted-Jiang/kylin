#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# This script is for production metadata store manipulation
# It is designed to run in hadoop CLI, both in sandbox or in real hadoop environment
#
# If you're a developer of Kylin and want to download sandbox's metadata into your dev machine,
# take a look at SandboxMetastoreCLI


source ${KYLIN_HOME:-"$(cd -P -- "$(dirname -- "$0")" && pwd -P)/../"}/bin/header.sh

if [ "$1" == "backup" ]
then

    mkdir -p ${KYLIN_HOME}/meta_backups

    _now=$(date +"%Y_%m_%d_%H_%M_%S")
    _file="${KYLIN_HOME}/meta_backups/meta_${_now}"
    echo "Starting backup to ${_file}"
    mkdir -p ${_file}

    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool download ${_file} "${@:2}"

    echo "metadata store backed up to ${_file}"

elif [ "$1" == "backup-hdfs" ]
then

    BACKUP_HOME="$2/meta_backups"

    if [ "$#" -eq 3 ]
    then
        HDFS_META_HOME=$3
    else
        HDFS_META_HOME=$(grep kylin.env.hdfs-working-dir ${KYLIN_HOME}/conf/kylin.properties | awk -F= '{print $2}')
    fi

    if [ -z "$HDFS_META_HOME" ]
    then
        quit "HDFS_META_HOME should be defined!!!"
    else
        echo "HDFS_META_HOME is $HDFS_META_HOME"
    fi

    _now=$(date +"%Y_%m_%d_%H_%M_%S")
    _file="meta_${_now}"
    _tar="${_file}.tar.gz"
    _cur_meta_dir="${BACKUP_HOME}/${_file}"
    mkdir -p ${_cur_meta_dir}

    echo "Starting backup to ${_cur_meta_dir}"
    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool download ${_cur_meta_dir}
    echo "metadata store backed up to ${_cur_meta_dir}"

    _cur_meta_tar="${BACKUP_HOME}/${_tar}"
    tar -czvf ${_cur_meta_tar} ${_cur_meta_dir}

    HDFS_BACKUP_HOME="${HDFS_META_HOME}/meta_backups"
    hadoop fs -mkdir -p ${HDFS_BACKUP_HOME}

    echo "Upload metadata ${_cur_meta_tar} to hdfs ${HDFS_BACKUP_HOME}"
    hadoop fs -copyFromLocal ${_cur_meta_tar} ${HDFS_BACKUP_HOME}
    echo "metadata store backed up to hdfs ${HDFS_BACKUP_HOME}/${_tar}"

    rm -f ${_cur_meta_tar}
    rm -rf ${_cur_meta_dir}
    echo "remove local meta ${_cur_meta_dir}"

    # keep latest n tars
    _n_keep=5

    _n_exists=`hadoop fs -ls ${HDFS_BACKUP_HOME} | wc -l`

    if ((${_n_exists} > ${_n_keep}))
    then
        _n_delete=$((${_n_exists} - ${_n_keep}))
        for _hdfs_tar in `hadoop fs -ls ${HDFS_BACKUP_HOME} | sort -k6,7 -r | awk '{print $8}' | tail -${_n_delete}`
        do
          echo "Going to delete hdfs file ${_hdfs_tar}"
          hadoop fs -rm ${_hdfs_tar}
        done
    fi

elif [ "$1" == "fetch" ]
then

    _file=$2

    _now=$(date +"%Y_%m_%d_%H_%M_%S")
    _fileDst="${KYLIN_HOME}/meta_backups/meta_${_now}"
    echo "Starting restoring $_fileDst"
    mkdir -p $_fileDst

    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool fetch $_fileDst $_file
    echo "metadata store backed up to $_fileDst"

elif [ "$1" == "restore" ]
then

    _file=$2
    echo "Starting restoring $_file"
    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool upload $_file "${@:3}"

elif [ "$1" == "list" ]
then

    _file=$2
    echo "Starting list $_file"
    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool list $_file

elif [ "$1" == "remove" ]
then

    _file=$2
    echo "Starting remove $_file"
    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool remove $_file

elif [ "$1" == "cat" ]
then

    _file=$2
    echo "Starting cat $_file"
    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool cat $_file

elif [ "$1" == "reset" ]
then

    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool reset
    
elif [ "$1" == "refresh-cube-signature" ]
then

    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.cube.cli.CubeSignatureRefresher
    
elif [ "$1" == "clean" ]
then

    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.MetadataCleanupJob  "${@:2}"

else
    echo "usage: metastore.sh backup [RESOURCE_PATH_PREFIX]"
    echo "       metastore.sh fetch DATA"
    echo "       metastore.sh reset"
    echo "       metastore.sh refresh-cube-signature"
    echo "       metastore.sh restore PATH_TO_LOCAL_META [RESOURCE_PATH_PREFIX]"
    echo "       metastore.sh list RESOURCE_PATH"
    echo "       metastore.sh cat RESOURCE_PATH"
    echo "       metastore.sh remove RESOURCE_PATH"
    echo "       metastore.sh clean [--delete true]"
    exit 1
fi
