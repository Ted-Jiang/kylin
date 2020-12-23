/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

KylinApp
  .controller('SourceMetaCtrl', function ($scope, $cacheFactory, $q, $window, $routeParams, CubeService, $modal, TableService, $route, loadingRequest, SweetAlert, tableConfig, TableModel,cubeConfig, MessageBox) {
    var $httpDefaultCache = $cacheFactory.get('$http');
    $scope.tableModel = TableModel;
    $scope.tableModel.selectedSrcDb = [];
    $scope.tableModel.selectedSrcTable = {};
    $scope.window = 0.68 * $window.innerHeight;
    $scope.tableConfig = tableConfig;
    $scope.isCalculate = true;
    $scope.selectedTsPattern = '';
    $scope.selfDefinedTsPattern = false;

    $scope.state = {
      filterAttr: 'id', filterReverse: false, reverseColumn: 'id',
      dimensionFilter: '', measureFilter: ''
    };

    function innerSort(a, b) {
      var nameA = a.name.toLowerCase(), nameB = b.name.toLowerCase();
      if (nameA < nameB) //sort string ascending
        return -1;
      if (nameA > nameB)
        return 1;
      return 0; //default return value (no sorting)
    };

    $scope.aceSrcTbLoaded = function (forceLoad) {
      //stop request when project invalid
      if (!$scope.projectModel.getSelectedProject()) {
        TableModel.init();
        return;
      }
      if (forceLoad) {
        $httpDefaultCache.removeAll();
      }
      $scope.loading = true;
      TableModel.aceSrcTbLoaded(forceLoad).then(function () {
        $scope.loading = false;
      });
    };

    $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
//         will load table when enter this page,null or not
      $scope.aceSrcTbLoaded();
    }, function (resp) {
      SweetAlert.swal('Oops...', resp, 'error');
    });


    $scope.showSelected = function (obj) {
      if (obj.uuid) {
        $scope.tableModel.selectedSrcTable = obj;
      }
      else if (obj.datatype) {
        $scope.tableModel.selectedSrcTable.selectedSrcColumn = obj;
      }
    };

    $scope.aceSrcTbChanged = function () {
      $scope.tableModel.selectedSrcDb = [];
      $scope.tableModel.selectedSrcTable = {};
      $scope.aceSrcTbLoaded(true);
    };


    $scope.openModal = function () {
      if(!$scope.projectModel.selectedProject){
        SweetAlert.swal('Oops...', "Please select a project.", 'info');
        return;
      }
      $modal.open({
        templateUrl: 'addHiveTable.html',
        controller: ModalInstanceCtrl,
        backdrop : 'static',
        resolve: {
          tableNames: function () {
            return $scope.tableNames;
          },
          projectName: function () {
            return $scope.projectModel.selectedProject;
          },
          isCalculate: function () {
            return $scope.isCalculate;
          },
          scope: function () {
            return $scope;
          }
        }
      });
    };

    $scope.openReloadModal = function () {
      $modal.open({
        templateUrl: 'reloadTable.html',
        controller: ModalInstanceCtrl,
        backdrop : 'static',
        resolve: {
          tableNames: function () {
            return $scope.tableModel.selectedSrcTable.database + '.'+ $scope.tableModel.selectedSrcTable.name;
          },
          projectName: function () {
            return $scope.projectModel.selectedProject;
          },
          isCalculate: function () {
            return $scope.isCalculate;
          },
          scope: function () {
            return $scope;
          }
        }
      });
    };

    $scope.calCardinality = function (tableName) {
      SweetAlert.swal({
        title: "",
        text: "Are you sure to recalculate column cardinality?",
        showCancelButton: true,
        confirmButtonColor: '#DD6B55',
        confirmButtonText: "Yes",
        cancelButtonText: "No",
        closeOnConfirm: true
      }, function (isConfirm) {
        if (isConfirm) {
          if (!$scope.projectModel.selectedProject) {
            SweetAlert.swal('', 'Please select a project.', 'info');
            return;
          }
          loadingRequest.show();
          TableService.genCardinality({tableName: tableName, pro: $scope.projectModel.selectedProject}, {}, function () {
            loadingRequest.hide();
            MessageBox.successNotify('Cardinality job has been submitted successfully. Please wait a while to get the numbers.');
          }, function (e) {
            loadingRequest.hide();
            if (e.data && e.data.exception) {
              var message = e.data.exception;
              var msg = !!(message) ? message : 'Failed to take action.';
              SweetAlert.swal('Oops...', msg, 'error');
            } else {
              SweetAlert.swal('Oops...', "Failed to take action.", 'error');
            }
          });
        }
      });
    };

    $scope.openTreeModal = function () {
      if(!$scope.projectModel.selectedProject){
        SweetAlert.swal('Oops...', "Please select a project.", 'info');
        return;
      }

      $modal.open({
        templateUrl: 'addHiveTableFromTree.html',
        controller: ModalInstanceCtrl,
        resolve: {
          tableNames: function () {
            return $scope.tableNames;
          },
          projectName:function(){
            return  $scope.projectModel.selectedProject;
          },
          isCalculate: function () {
            return $scope.isCalculate;
          },
          scope: function () {
            return $scope;
          }
        }
      });
    };

    $scope.reloadTable = function (tableName, isCalculate){
      var delay = $q.defer();
      loadingRequest.show();
      TableService.loadHiveTable({tableName: tableName, action: $scope.projectModel.selectedProject}, {calculate: isCalculate}, function (result) {
        var loadTableInfo = "";
        angular.forEach(result['result.loaded'], function (table) {
          loadTableInfo += "\n" + table;
        })
        var unloadedTableInfo = "";
        angular.forEach(result['result.unloaded'], function (table) {
          unloadedTableInfo += "\n" + table;
        })
        if (result['result.unloaded'].length != 0 && result['result.loaded'].length == 0) {
          SweetAlert.swal('Failed!', 'Failed to load following table(s): ' + unloadedTableInfo, 'error');
        }
        if (result['result.loaded'].length != 0 && result['result.unloaded'].length == 0) {
          MessageBox.successNotify('The following table(s) have been successfully loaded: ' + loadTableInfo);
        }
        if (result['result.loaded'].length != 0 && result['result.unloaded'].length != 0) {
          SweetAlert.swal('Partial loaded!', 'The following table(s) have been successfully loaded: ' + loadTableInfo + "\n\n Failed to load following table(s):" + unloadedTableInfo, 'warning');
        }
        loadingRequest.hide();
        delay.resolve("");
      }, function (e) {
        if (e.data && e.data.exception) {
          var message = e.data.exception;
          var msg = !!(message) ? message : 'Failed to take action.';
          SweetAlert.swal('Oops...', msg, 'error');
        } else {
          SweetAlert.swal('Oops...', "Failed to take action.", 'error');
        }
        loadingRequest.hide();
      })
       return delay.promise;
    }


    $scope.unloadTable = function (tableName) {
      SweetAlert.swal({
            title: "",
            text: "Are you sure to unload this table?",
            showCancelButton: true,
            confirmButtonColor: '#DD6B55',
            confirmButtonText: "Yes",
            cancelButtonText: "No",
            closeOnConfirm: true
      }, function (isConfirm) {
        if (isConfirm) {
          if (!$scope.projectModel.selectedProject) {
            SweetAlert.swal('', 'Please select a project.', 'info');
            return;
          }
          loadingRequest.show();
          TableService.unLoadHiveTable({tableName: tableName, action: $scope.projectModel.selectedProject}, {}, function (result) {
            var removedTableInfo = "";
            angular.forEach(result['result.unload.success'], function (table) {
              removedTableInfo += "\n" + table;
            })
            var unRemovedTableInfo = "";
            angular.forEach(result['result.unload.fail'], function (table) {
              unRemovedTableInfo += "\n" + table;
            })
            if (result['result.unload.fail'].length != 0 && result['result.unload.success'].length == 0) {
              SweetAlert.swal('Failed!', 'Failed to unload following table(s): ' + unRemovedTableInfo, 'error');
            }
            if (result['result.unload.success'].length != 0 && result['result.unload.fail'].length == 0) {
              MessageBox.successNotify('The following table(s) have been successfully unloaded: ' + removedTableInfo);
            }
            if (result['result.unload.success'].length != 0 && result['result.unload.fail'].length != 0) {
              SweetAlert.swal('Partial unloaded!', 'The following table(s) have been successfully unloaded: ' + removedTableInfo + "\n\n Failed to unload following table(s):" + unRemovedTableInfo, 'warning');
            }
            loadingRequest.hide();
            $scope.aceSrcTbLoaded(true);
          }, function (e) {
            if (e.data && e.data.exception) {
              var message = e.data.exception;
              var msg = !!(message) ? message : 'Failed to take action.';
              SweetAlert.swal('Oops...', msg, 'error');
            } else {
              SweetAlert.swal('Oops...', "Failed to take action.", 'error');
            }
            loadingRequest.hide();
          })
        }
      })
    }

    var ModalInstanceCtrl = function ($scope, $location, $modalInstance, tableNames, MessageService, projectName, isCalculate, scope, kylinConfig) {
      $scope.tableNames = "";
      $scope.selectTable = tableNames;
      $scope.projectName = projectName;
      $scope.isCalculate = {
        val: true
      }

      $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
      };

      $scope.kylinConfig = kylinConfig;


      $scope.treeOptions = {multiSelection: true};
      $scope.selectedNodes = [];
      $scope.hiveLimit =  kylinConfig.getHiveLimit();
      $scope.sourceType =  kylinConfig.getSourceType();
      if ($scope.sourceType !== '0') {
        $scope.isCalculate.val = false
      }

      $scope.loadHive = function () {
        if($scope.hiveLoaded)
          return;
        TableService.showHiveDatabases({project: $scope.projectName}, function (databases) {
          $scope.dbNum = databases.length;
          if (databases.length > 0) {
            $scope.hiveMap = {};
            for (var i = 0; i < databases.length; i++) {
              var dbName = databases[i];
              var hiveData = {"dbname":dbName,"tables":[],"expanded":false};
              $scope.hive.push(hiveData);
              $scope.hiveMap[dbName] = i;
            }
          }
          $scope.hiveLoaded = true;
          $scope.showMoreDatabases();
        }, function (e) {
          if (e.data && e.data.exception) {
            var message = e.data.exception;
            var msg = !!(message) ? message : 'Failed to take action.';
            SweetAlert.swal('Oops...', msg, 'error');
          } else {
            SweetAlert.swal('Oops...', "Failed to take action.", 'error');
          }
          $scope.hiveLoaded = true;
        });
      }

      $scope.showMoreTables = function(hiveTables, node){
        var shownTimes = parseInt(node.children.length / $scope.hiveLimit);
        var from = $scope.hiveLimit * shownTimes;
        var to = 0;
        var hasMore = false;
        if(from + $scope.hiveLimit > hiveTables.length) {
          to = hiveTables.length - 1;
        } else {
          to = from + $scope.hiveLimit - 1;
          hasMore = true;
        }
        if(!angular.isUndefined(node.children[from])){
          node.children.pop();
        }

        for(var idx = from; idx <= to; idx++){
          node.children.push({"label":node.label+'.'+hiveTables[idx],"id":idx-from+1,"children":[]});
        }

        if(hasMore){
          var loading = {"label":"","id":65535,"children":[]};
          node.children.push(loading);
        }
      }

      $scope.showAllTables = function(hiveTables, node){
        var shownTimes = parseInt(node.children.length / $scope.hiveLimit);
        var from = $scope.hiveLimit * shownTimes;
        var to = hiveTables.length - 1;
        if(!angular.isUndefined(node.children[from])){
          node.children.pop();
        }
        for(var idx = from; idx <= to; idx++){
          node.children.push({"label":node.label+'.'+hiveTables[idx],"id":idx-from+1,"children":[]});
        }
      }

      $scope.showMoreDatabases = function(){
        var shownTimes = parseInt($scope.treedata.length / $scope.hiveLimit);
        var from = $scope.hiveLimit * shownTimes;
        var to = 0;
        var hasMore = false;
        if(from + $scope.hiveLimit > $scope.hive.length) {
          to = $scope.hive.length - 1;
        } else {
          to = from + $scope.hiveLimit - 1;
          hasMore = true;
        }
        if(!angular.isUndefined($scope.treedata[from])){
          $scope.treedata.pop();
        }

        for(var idx = from; idx <= to; idx++){
          var children = [];
          var loading = {"label":"","id":0,"children":[]};
          children.push(loading);
          $scope.treedata.push({"label":$scope.hive[idx].dbname,"id":idx+1,"children":children,"expanded":false});
        }

        if(hasMore){
          var loading = {"label":"","id":65535,"children":[0]};
          $scope.treedata.push(loading);
        }
      }

      $scope.showAllDatabases = function(){
        var shownTimes = parseInt($scope.treedata.length / $scope.hiveLimit);
        var from = $scope.hiveLimit * shownTimes;
        var to = $scope.hive.length - 1;

        if(!angular.isUndefined($scope.treedata[from])){
          $scope.treedata.pop();
        }

        for(var idx = from; idx <= to; idx++){
          var children = [];
          var loading = {"label":"","id":0,"children":[]};
          children.push(loading);
          $scope.treedata.push({"label":$scope.hive[idx].dbname,"id":idx+1,"children":children,"expanded":false});
        }
      }

      $scope.showMoreClicked = function($parentNode){
        if($parentNode == null){
          $scope.showMoreDatabases();
        } else {
          $scope.showMoreTables($scope.hive[$scope.hiveMap[$parentNode.label]].tables,$parentNode);
        }
      }

      $scope.showAllClicked = function($parentNode){
        if($parentNode == null){
          $scope.showAllDatabases();
        } else {
          $scope.showAllTables($scope.hive[$scope.hiveMap[$parentNode.label]].tables,$parentNode);
        }
      }

      $scope.showToggle = function(node) {
        if(node.expanded == false){
          TableService.showHiveTables({"database": node.label, project: $scope.projectName},function (hive_tables){
            var tables = [];
            for (var i = 0; i < hive_tables.length; i++) {
              tables.push(hive_tables[i]);
            }
            $scope.hive[$scope.hiveMap[node.label]].tables = tables;
            $scope.showMoreTables(tables,node);
            node.expanded = true;
          });
        }
      }

      $scope.showSelected = function(node) {

      }

      if(angular.isUndefined($scope.hive) || angular.isUndefined($scope.hiveLoaded) || angular.isUndefined($scope.treedata) ){
        $scope.hive = [];
        $scope.hiveLoaded = false;
        $scope.treedata = [];
        $scope.loadHive();
      }

      $scope.confirmReload = function() {
        $scope.cancel();
        scope.reloadTable($scope.selectTable, $scope.isCalculate.val).then(function() {
          scope.aceSrcTbLoaded(true);
        })
      }


      $scope.add = function () {

        if($scope.tableNames.length === 0 && $scope.selectedNodes.length > 0) {
          for(var i = 0; i <  $scope.selectedNodes.length; i++){
            if($scope.selectedNodes[i].label.indexOf(".") >= 0){
              $scope.tableNames += ($scope.selectedNodes[i].label) += ',';
            }
          }
        }

        if ($scope.tableNames.trim() === "") {
          SweetAlert.swal('', 'Please input table(s) you want to load.', 'info');
          return;
        }

        if (!$scope.projectName) {
          SweetAlert.swal('', 'Please select a project.', 'info');
          return;
        }

        $scope.cancel();
        scope.reloadTable($scope.tableNames, $scope.isCalculate.val).then(function(){
             scope.aceSrcTbLoaded(true);
           });
      }
    };

    $scope.editStreamingConfig = function(tableName){
      var modalInstance = $modal.open({
        templateUrl: 'editStreamingSource.html',
        controller: EditStreamingSourceCtrl,
        backdrop : 'static',
        resolve: {
          tableNames: function () {
            return $scope.tableNames;
          },
          projectName: function () {
            return $scope.projectModel.selectedProject;
          },
          tableName: function(){
            return tableName;
          },
          scope: function () {
            return $scope;
          }
        }
      });

      modalInstance.result.then(function () {
        $scope.$broadcast('StreamingConfigEdited');
      }, function () {
        $scope.$broadcast('StreamingConfigEdited');
      });


    }

    $scope.editStreamingConfigV2 = function(streamingConfig){
      var modalInstance = $modal.open({
        templateUrl: 'editStreamingTableV2.html',
        controller: EditStreamingSourceV2Ctrl,
        backdrop : 'static',
        resolve: {
          streamingConfig: function () {
            return streamingConfig;
          },
          projectName: function () {
            return $scope.projectModel.selectedProject;
          },
          scope: function () {
            return $scope;
          }
        }
      });

      modalInstance.result.then(function () {
        $scope.$broadcast('StreamingConfigEdited');
      }, function () {
        $scope.$broadcast('StreamingConfigEdited');
      });
    }

    //streaming model
    $scope.openStreamingSourceModal = function () {
      if(!$scope.projectModel.selectedProject){
        SweetAlert.swal('Oops...', "Please select a project.", 'info');
        return;
      }
      $modal.open({
        templateUrl: 'addStreamingSource.html',
        controller: StreamingSourceCtrl,
        backdrop : 'static',
        resolve: {
          tableNames: function () {
            return $scope.tableNames;
          },
          projectName: function () {
            return $scope.projectModel.selectedProject;
          },
          scope: function () {
            return $scope;
          }
        }
      });
    };
    function bootstrapServerValidation(bootstrapServers) {
      var flag = false;
      if (bootstrapServers && bootstrapServers.length > 0) {
        angular.forEach(bootstrapServers, function(bootstrapServer, ind) {
          if (!bootstrapServer.host || !bootstrapServer.port || bootstrapServer.host.length === 0 || bootstrapServer.port.length === 0) {
            flag = true;
          }
        });
      } else {
        flag = true;
      }
      return flag;
    };
    var EditStreamingSourceCtrl = function ($scope, $interpolate, $templateCache, tableName, $modalInstance, tableNames, MessageService, projectName, scope, tableConfig,cubeConfig,StreamingModel,StreamingService) {

      $scope.state = {
        tableName : tableName,
        mode: "edit",
        target:"kfkConfig"
      }

      $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
      };

      $scope.projectName = projectName;
      $scope.streamingMeta = StreamingModel.createStreamingConfig();
      $scope.kafkaMeta = StreamingModel.createKafkaConfig();
      $scope.updateStreamingMeta = function(val){
        $scope.streamingMeta = val;
      }
      $scope.updateKafkaMeta = function(val){
        $scope.kafkaMeta = val;
      }

      $scope.updateStreamingSchema = function(){
        StreamingService.update({}, {
          project: $scope.projectName,
          tableData:angular.toJson(""),
          streamingConfig: angular.toJson($scope.streamingMeta),
          kafkaConfig: angular.toJson($scope.kafkaMeta)
        }, function (request) {
          if (request.successful) {
            MessageBox.successNotify('Updated the streaming successfully.');
            $scope.cancel();
          } else {
            var message = request.message;
            var msg = !!(message) ? message : 'Failed to take action.';
            MessageService.sendMsg($scope.streamingResultTmpl({
              'text': msg,
              'streamingSchema': angular.toJson($scope.streamingMeta,true),
              'kfkSchema': angular.toJson($scope.kafkaMeta,true)
            }), 'error', {}, true, 'top_center');
          }
          loadingRequest.hide();
        }, function (e) {
          if (e.data && e.data.exception) {
            var message = e.data.exception;
            var msg = !!(message) ? message : 'Failed to take action.';
            MessageService.sendMsg($scope.streamingResultTmpl({
              'text': msg,
              'streamingSchema': angular.toJson($scope.streamingMeta,true),
              'kfkSchema': angular.toJson($scope.kafkaMeta,true)
            }), 'error', {}, true, 'top_center');
          } else {
            MessageService.sendMsg($scope.streamingResultTmpl({
              'text': msg,
              'streamingSchema': angular.toJson($scope.streamingMeta,true),
              'kfkSchema': angular.toJson($scope.kafkaMeta,true)
            }), 'error', {}, true, 'top_center');
          }
          //end loading
          loadingRequest.hide();

        })
      }

    }

    var EditStreamingSourceV2Ctrl = function ($scope, ResponseUtil, $modalInstance, projectName,StreamingServiceV2, streamingConfig) {
      $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
      };
      $scope.streamingConfig = streamingConfig;
      $scope.streamingConfig.properties.bootstrapServers = streamingConfig.properties['bootstrap.servers'].split(',').map(function(address){
        return {
          host: address.split(':')[0],
          port: +address.split(':')[1]
        }
      })
      $scope.addBootstrapServer = function() {
        if (!$scope.streamingConfig.properties.bootstrapServers) {
          $scope.streamingConfig.properties.bootstrapServers = [];
        }
        $scope.streamingConfig.properties.bootstrapServers.push({host: '', port: 9092});
      }
      $scope.removeBootstrapServer = function(index) {
        $scope.streamingConfig.properties.bootstrapServers.splice(index, 1);
      };
      $scope.projectName = projectName;
      $scope.updateStreamingMeta = function(val){
        $scope.streamingMeta = val;
      }
      $scope.updateKafkaMeta = function(val){
        $scope.kafkaMeta = val;
      }
      $scope.bootstrapServerValidation = bootstrapServerValidation
      $scope.updateStreamingV2Config = function(){
        loadingRequest.show();
        $scope.streamingConfig.properties['bootstrap.servers'] = $scope.streamingConfig.properties.bootstrapServers.map(function(address){
          return address.host + ':' + address.port;
        }).join(',');
        delete $scope.streamingConfig.properties.bootstrapServers;
        var updateConfig = {
          project: $scope.projectName,
          streamingConfig: JSON.stringify($scope.streamingConfig)
        }
        StreamingServiceV2.update({}, updateConfig, function (request) {
          if (request.successful) {
            MessageBox.successNotify('Updated the streaming successfully.');
            $scope.cancel();
          } else {
            ResponseUtil.handleError({
              data: {exception: request.message}
            })
          }
          loadingRequest.hide();
        }, function (e) {
          ResponseUtil.handleError(e)
          loadingRequest.hide();
        })
      }
    }
    // 推断列的类型
    function checkColumnValType(val,key){
      var defaultType;
      if(typeof val ==="number"){
          if(/id/i.test(key)&&val.toString().indexOf(".")==-1){
            defaultType="int";
          }else if(val <= 2147483647){
            if(val.toString().indexOf(".")!=-1){
              defaultType="decimal";
            }else{
              defaultType="int";
            }
          }else{
            defaultType="timestamp";
          }
      }else if(typeof val ==="string"){
          if(!isNaN((new Date(val)).getFullYear())&&typeof ((new Date(val)).getFullYear())==="number"){
            defaultType="date";
          }else{
            defaultType="varchar(256)";
          }
      }else if(Object.prototype.toString.call(val)=="[object Array]"){
          defaultType="varchar(256)";
      }else if (typeof val ==="boolean"){
          defaultType="boolean";
      }
      return defaultType;
    }
    // 打平straming表结构
    function flatStreamingJson (objRebuildFunc, flatResult) {
      return function flatObj (obj,base,comment) {
        base=base?base+"_":"";
        comment= comment?comment+"|":""
        for(var i in obj){
          if(Object.prototype.toString.call(obj[i])=="[object Object]"){
            flatObj(obj[i],base+i,comment+i);
            continue;
          }
          flatResult.push(objRebuildFunc(base+i,obj[i],comment+i));
        }
      }
    }

    var StreamingSourceCtrl = function ($scope, $location,$interpolate,$templateCache, $modalInstance, tableNames, MessageService, projectName, scope, tableConfig,cubeConfig,StreamingModel,StreamingService) {

      $scope.state={
        'mode':'edit'
      }

      $scope.streamingMeta = StreamingModel.createStreamingConfig();
      $scope.kafkaMeta = StreamingModel.createKafkaConfig();



      $scope.steps = {
        curStep:1
      };

      $scope.streamingCfg = {
        parseTsColumn:"{{}}",
        columnOptions:[]
      }

      $scope.previewStep = function(){
        $scope.steps.curStep--;
      }

      $scope.nextStep = function(){

        $scope.checkFailed = false;

        //check form
        $scope.form['setStreamingSchema'].$submitted = true;
        if(!$scope.streaming.sourceSchema||$scope.streaming.sourceSchema===""){
          $scope.checkFailed = true;
        }

        if(!$scope.table.name||$scope.table.name===""){
          $scope.checkFailed = true;
        }

        $scope.prepareNextStep();

        if(!$scope.rule.timestampColumnExist){
          $scope.checkFailed = true;
        }

        if($scope.checkFailed){
          return;
        }

        $scope.steps.curStep++;
      }

      $scope.prepareNextStep = function(){
        $scope.streamingCfg.columnOptions = [];
        $scope.rule.timestampColumnExist = false;
        angular.forEach($scope.columnList,function(column,$index){
          if (column.checked == "Y" && column.fromSource=="Y" && column.type == "timestamp") {
            $scope.streamingCfg.columnOptions.push(column.name);
            $scope.rule.timestampColumnExist = true;
          }
        })

        if($scope.streamingCfg.columnOptions.length==1){
          $scope.streamingCfg.parseTsColumn = $scope.streamingCfg.columnOptions[0];
          $scope.kafkaMeta.parserProperties = "tsColName="+$scope.streamingCfg.parseTsColumn;
        }
        if($scope.kafkaMeta.parserProperties!==''){
          $scope.state.isParserHeaderOpen = false;
        }else{
          $scope.state.isParserHeaderOpen = true;
        }
      }

      $scope.projectName = projectName;
      $scope.tableConfig = tableConfig;
      $scope.cubeConfig = cubeConfig;
      $scope.streaming = {
        sourceSchema: '',
        'parseResult': {}
      }

      $scope.table = {
        name: '',
        sourceValid:false,
        schemaChecked:false
      }

      $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
      };

      $scope.streamingOnLoad = function () {
        console.log($scope.streaming.sourceSchema);
      }

      $scope.columnList = [];

      $scope.streamingOnChange = function () {
        $scope.table.schemaChecked = true;
        try {
          $scope.streaming.parseResult = JSON.parse($scope.streaming.sourceSchema);
        } catch (error) {
          $scope.table.sourceValid = false;
          return;
        }
        $scope.table.sourceValid = true;

        //streaming table data change structure
        function createNewObj(key,val,comment){
          var obj={};
          obj.name=key;
          obj.type=checkColumnValType(val,key);
          obj.fromSource="Y";
          obj.checked="Y";
          obj.comment=comment;
          if(Object.prototype.toString.call(val)=="[object Array]"){
            obj.checked="N";
          }
          return obj;
        }
       var columnList = []
        flatStreamingJson(createNewObj, columnList)($scope.streaming.parseResult)
        var timeMeasure = $scope.cubeConfig.streamingAutoGenerateMeasure;
        for(var i = 0;i<timeMeasure.length;i++){
          var defaultCheck = 'Y';
          columnList.push({
            'name': timeMeasure[i].name,
            'checked': defaultCheck,
            'type': timeMeasure[i].type,
            'fromSource':'N'
          });
        }

        var firstCommit = false;
        if($scope.columnList.length==0){
          firstCommit = true;
        }

        if(!firstCommit){
          angular.forEach(columnList,function(item){
            for(var i=0;i<$scope.columnList.length;i++){
              if($scope.columnList[i].name==item.name){
                item.checked = $scope.columnList[i].checked;
                item.type = $scope.columnList[i].type;
                item.fromSource = $scope.columnList[i].fromSource;
                break;
              }
            }
          })
        }
        $scope.columnList = columnList;
      }


      $scope.streamingResultTmpl = function (notification) {
        // Get the static notification template.
        var tmpl = notification.type == 'success' ? 'streamingResultSuccess.html' : 'streamingResultError.html';
        return $interpolate($templateCache.get(tmpl))(notification);
      };


      $scope.form={};
      $scope.rule={
        'timestampColumnExist':false
      }

      $scope.modelMode == "addStreaming";

      $scope.syncStreamingSchema = function () {

        $scope.form['cube_streaming_form'].$submitted = true;

        if($scope.form['cube_streaming_form'].parserName.$invalid || $scope.form['cube_streaming_form'].parserProperties.$invalid) {
          $scope.state.isParserHeaderOpen = true;
        }

        if($scope.form['cube_streaming_form'].$invalid){
            return;
        }

        var columns = [];
        angular.forEach($scope.columnList,function(column,$index){
          if (column.checked == "Y") {
            var columnInstance = {
              "id": ++$index,
              "name": column.name,
              "comment": /[|]/.test(column.comment)? column.comment : "",
              "datatype": column.type
            }
            columns.push(columnInstance);
          }
        })


        $scope.tableData = {
          "name": $scope.table.name,
          "source_type":1,
          "columns": columns,
          'database':'Default'
        }


        $scope.kafkaMeta.name = $scope.table.name
        $scope.streamingMeta.name = $scope.table.name;

        SweetAlert.swal({
          title:"",
          text: 'Are you sure to save the streaming table and cluster info ?',
          showCancelButton: true,
          confirmButtonColor: '#DD6B55',
          confirmButtonText: "Yes",
          closeOnConfirm: true
        }, function (isConfirm) {
          if (isConfirm) {
            loadingRequest.show();

            if ($scope.modelMode == "editExistStreaming") {
              StreamingService.update({}, {
                project: $scope.projectName,
                tableData:angular.toJson($scope.tableData),
                streamingConfig: angular.toJson($scope.streamingMeta),
                kafkaConfig: angular.toJson($scope.kafkaMeta)
              }, function (request) {
                if (request.successful) {
                  MessageBox.successNotify('Updated the streaming successfully.');
                  $location.path("/models");
                } else {
                  var message = request.message;
                  var msg = !!(message) ? message : 'Failed to take action.';
                  MessageService.sendMsg($scope.streamingResultTmpl({
                    'text': msg,
                    'streamingSchema': angular.toJson($scope.streamingMeta,true),
                    'kfkSchema': angular.toJson($scope.kafkaMeta,true)
                  }), 'error', {}, true, 'top_center');
                }
                loadingRequest.hide();
              }, function (e) {
                if (e.data && e.data.exception) {
                  var message = e.data.exception;
                  var msg = !!(message) ? message : 'Failed to take action.';
                  MessageService.sendMsg($scope.streamingResultTmpl({
                    'text': msg,
                    'streamingSchema': angular.toJson($scope.streamingMeta,true),
                    'kfkSchema': angular.toJson($scope.kafkaMeta,true)
                  }), 'error', {}, true, 'top_center');
                } else {
                  MessageService.sendMsg($scope.streamingResultTmpl({
                    'text': msg,
                    'streamingSchema': angular.toJson($scope.streamingMeta,true),
                    'kfkSchema': angular.toJson($scope.kafkaMeta,true)
                  }), 'error', {}, true, 'top_center');
                }
                //end loading
                loadingRequest.hide();

              })
            } else {
              StreamingService.save({}, {
                project: $scope.projectName,
                tableData:angular.toJson($scope.tableData),
                streamingConfig: angular.toJson($scope.streamingMeta),
                kafkaConfig: angular.toJson($scope.kafkaMeta)
              }, function (request) {
                if (request.successful) {
                  MessageBox.successNotify('Created the streaming successfully.');
                  $scope.cancel();
                  scope.aceSrcTbLoaded(true);
                } else {
                  var message = request.message;
                  var msg = !!(message) ? message : 'Failed to take action.';
                  MessageService.sendMsg($scope.streamingResultTmpl({
                    'text': msg,
                    'streamingSchema': angular.toJson($scope.streamingMeta,true),
                    'kfkSchema': angular.toJson($scope.kafkaMeta,true)
                  }), 'error', {}, true, 'top_center');
                }
                loadingRequest.hide();
              }, function (e) {
                if (e.data && e.data.exception) {
                  var message = e.data.exception;
                  var msg = !!(message) ? message : 'Failed to take action.';

                  MessageService.sendMsg($scope.streamingResultTmpl({
                    'text': msg,
                    'streamingSchema':angular.toJson( $scope.streamingMeta,true),
                    'kfkSchema': angular.toJson($scope.kafkaMeta,true)
                  }), 'error', {}, true, 'top_center');
                } else {
                  MessageService.sendMsg($scope.streamingResultTmpl({
                    'text': msg,
                    'streamingSchema': angular.toJson($scope.streamingMeta,true),
                    'kfkSchema': angular.toJson($scope.kafkaMeta,true)
                  }), 'error', {}, true, 'top_center');
                }
                //end loading
                loadingRequest.hide();
              })
            }

          }
        });
      }

    };

     //streaming resource onboard v2
    $scope.openStreamingSourceModalV2 = function() {
      if(!$scope.projectModel.selectedProject){
        SweetAlert.swal('Oops...', "Please select a project.", 'info');
        return;
      }
      $modal.open({
        templateUrl: 'addStreamingSourceV2.html',
        controller: StreamingSourceCtrlV2,
        backdrop : 'static',
        resolve: {
          // kafka stream
          isKafka: function () {
            return {
              value: true
            }
          },
          projectName: function () {
            return $scope.projectModel.selectedProject;
          },
          scope: function () {
            return $scope;
          }
        }
      });
    };

    //rheos stream onboard
    $scope.openRheosStreamingSourceModal = function () {
      if (!$scope.projectModel.selectedProject) {
        SweetAlert.swal('Oops...', "Please select a project.", 'info');
        return;
      }
      $modal.open({
        templateUrl: 'addRheosStreamingSource.html',
        controller: StreamingSourceCtrlV2,
        backdrop: 'static',
        resolve: {
          // rheos
          isKafka: function () {
            return {
              value: false
            }
          },
          projectName: function () {
            return $scope.projectModel.selectedProject;
          },
          scope: function () {
            return $scope;
          }
        }
      });
    };

    var StreamingSourceCtrlV2 = function($scope, $modalInstance, $filter, MessageService, projectName, isKafka, scope, cubeConfig, tableConfig, StreamingModel, StreamingServiceV2) {
      $scope.tableConfig = tableConfig;
      // common
      $scope.steps = {
        curStep:1
      };

      $scope.previewStep = function() {
        $scope.steps.curStep--;
      }

      $scope.nextStep = function() {
        // clean data
        if ($scope.steps.curStep === 1) {
          $scope.streaming = {
            template: '',
            dataTypeArr: tableConfig.dataTypes,
            TSColumnArr: [],
            TSColumnMappingArr: [],
            TSPatternArr: ['MS', 'S'],
            TSParserArr: ['org.apache.kylin.stream.source.kafka.LongTimeParser', 'org.apache.kylin.stream.source.kafka.DateTimeParser'],
            TSColumnSelected: '',
            TSParser: 'org.apache.kylin.stream.source.kafka.LongTimeParser',
            TSPattern: 'MS',
            errMsg: ''
          };
          $scope.tableData = {
            source_type: $scope.tableData.source_type
          };
        }
        $scope.steps.curStep++;
      };

      $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
      };

      // streaming config
      $scope.streamingConfig = {
        name: '',
        properties: {},
        parser_info: {}
      };

      $scope.rheosConfig ={
        EventType: ['RHEOS_EVENT'],
        RheosDCNameOptions: ['rno','lvs','slc']
      };

      if (isKafka.value) {
        $scope.tableData = {
          source_type: tableConfig.streamingSourceType.kafka
        };
      } else {
        // rheos
        $scope.tableData = {
          source_type: tableConfig.streamingSourceType.rheos
        };
        $scope.streamingConfig.properties.topicDC = $scope.rheosConfig.RheosDCNameOptions[0];
        $scope.streamingConfig.properties.topicEventType = $scope.rheosConfig.EventType[0];
      }


      $scope.removeBootstrapServer = function(index) {
        $scope.streamingConfig.properties.bootstrapServers.splice(index, 1);
      };

      $scope.addBootstrapServer = function() {
        if (!$scope.streamingConfig.properties.bootstrapServers) {
          $scope.streamingConfig.properties.bootstrapServers = [];
        }
        $scope.streamingConfig.properties.bootstrapServers.push({host: '', port: '9092'});
      }

      $scope.bootstrapServerValidation = bootstrapServerValidation;

      // streaming table
      $scope.streaming = {
        template: '',
        dataTypeArr: tableConfig.dataTypes,
        TSColumnArr: [],
        TSColumnMappingArr: [],
        TSColumnSelected: '',
        TSParser: '',
        TSPattern: '',
        errMsg: '',
        lambda: false
      };

      $scope.additionalColumn = {};

      $scope.getTemplate = function() {
        var transformStreamingConfig = $scope._transformStreamingObj({project: projectName,
              tableData: $scope.tableData,
              streamingConfig: $scope.streamingConfig});
        StreamingServiceV2.getParserTemplate({streamingConfig: transformStreamingConfig.streamingConfig, sourceType: $scope.tableData.source_type}, function(template){
          if (tableConfig.streamingSourceType.kafka === $scope.tableData.source_type ||
              tableConfig.streamingSourceType.rheos === $scope.tableData.source_type) {
            $scope.streaming.template = $filter('json')(template, 4);
          }
        }, function(e) {
          if (e.data && e.data.exception) {
            var message = e.data.exception;
            var msg = !!(message) ? message : 'Failed to get template.';
            SweetAlert.swal('Oops...', msg, 'error');
          } else {
            SweetAlert.swal('Oops...', "Failed to get template.", 'error');
          }
        });
      };

      $scope.getTableData = function() {
        $scope.tableData.name = '';
        $scope.tableData.columns = [];

        $scope.streaming.errMsg = '';

        $scope.streaming.TSColumnArr = [];
        $scope.streaming.TSColumnMappingArr = [];

        // Check template is not empty
        if (!$scope.streaming.template) {
          // $scope.tableData = undefined;
          $scope.tableData = {
            source_type: $scope.tableData.source_type
          };
          $scope.streaming.errMsg = 'Please input Streaming source record to generate schema.';
          return;
        }

        // Check template is json format
        try {
          var templateObj = JSON.parse($scope.streaming.template);
        } catch (error) {
          $scope.tableData = {
            source_type: $scope.tableData.source_type
          };
          $scope.streaming.errMsg = 'Source json invalid, Please correct your schema and generate again.';
          return;
        }
        // kafka parser
        var columnsByTemplate=[]
        function createNewObj(key,val,comment){
          var obj={};
          obj.name=key;
          obj.datatype=checkColumnValType(val,key);
          obj.comment=comment;
          if (obj.datatype === 'timestamp') {
            $scope.streaming.TSColumnArr.push(key);
            $scope.streaming.TSColumnMappingArr.push(comment);
          }
          return obj;
        }
        if (tableConfig.streamingSourceType.kafka === $scope.tableData.source_type ||
            tableConfig.streamingSourceType.rheos === $scope.tableData.source_type) {
          // TODO kafka need to support json not just first layer
          flatStreamingJson(createNewObj, columnsByTemplate)(templateObj)
        }
        var columnsByAuto = [];

        // TODO change the streamingAutoGenerateMeasure format
        angular.forEach(cubeConfig.streamingAutoGenerateMeasure, function(measure, index) {
          columnsByAuto.push({
            name: measure.name,
            datatype: measure.type
          });
        });

        $scope.tableData.columns = columnsByTemplate.concat(columnsByAuto);
      };

      $scope.getColumns = function(field, parentName, columnArr) {
        var columns = columnArr;
        if (typeof field.type === 'string') {
          if (!field.fields) {
            columns.push($scope.getColumnInfo(field, ''));
          } else {
            angular.forEach(field.fields, function(subField, ind) {
              var subColumn = $scope.getColumnInfo(subField, parentName);
              if (!$scope.isDerived(subColumn)) {
                columns.push(subColumn);
              }
            });
          }
        } else if (Array.isArray(field.type)) {
          columns.push($scope.getColumnInfo(field, ''));
        } else {
          $scope.getColumns(field.type, parentName, columns);
        }
        return columns;
      };

      $scope.getColumnInfo = function(field, parentName) {
        var fieldName = field.name;
        var contentType = 'varchar(256)';
        var typeArr = field.type;
        var _type = field.type;
        if (typeof typeArr !== 'string') {
          var contentTypeArr = [];
          angular.forEach(typeArr, function(type) {
            if ('null' !== type) {
              contentTypeArr.push(type);
            }
          });
          if (contentTypeArr.length === 1) {
            _type = contentTypeArr[0];
            if (typeof _type === 'object') {
              if (_type.type) {
                _type = _type.type;
              }
            }
          }
        }

        if ('string' !== _type) {
          if (tableConfig.dataTypes.indexOf(_type) > -1) {
            contentType = _type;
          } else if ('long' === _type) {
            contentType = 'bigint';
          }
        }

        var _column = {
          name: fieldName,
          datatype: contentType,
          comment: field.doc || '',
          field_mapping: parentName ? parentName + '.' + field.name : field.name
        };
        return _column;
      };

      $scope.removeColumn = function(index) {
        $scope.tableData.columns.splice(index, 1);
      };

      $scope.addColumn = function() {
        if ($scope.additionalColumn.name && $scope.additionalColumn.datatype && $scope.additionalColumn.field_mapping) {
          $scope.additionalColumn.error = undefined;
          if (!$scope.tableData.columns) {
            $scope.tableData.columns = [];
          }
          $scope.tableData.columns.push($scope.additionalColumn);
          if ($scope.additionalColumn.datatype == 'timestamp') {
            if (!$scope.streaming.TSColumnArr) {
              $scope.streaming.TSColumnArr = [];
              $scope.streaming.TSColumnMappingArr = [];
            }
            $scope.streaming.TSColumnArr.push($scope.additionalColumn.name);
            // add mapping info when add column
            $scope.streaming.TSColumnMappingArr.push($scope.additionalColumn.field_mapping);
          }
          $scope.additionalColumn = {};
        } else {
          $scope.additionalColumn.error = 'Additional column field can not be empty!';
        }
      };

      $scope.initFieldMapping = function() {
          $scope.additionalColumn.field_mapping = $scope.additionalColumn.name;
      };

      $scope.isDerived = function(column) {
        var derived = false;
        angular.forEach(cubeConfig.streamingAutoGenerateMeasure, function(measure, index) {
          if (measure.name === column.name) {
            derived = true;
          }
        });
        return derived;
      };

      $scope.updateTSColumnOption = function(column) {
        if (column.datatype === 'timestamp') {
          if (!$scope.streaming.TSColumnArr.includes(column.name)) {
            $scope.streaming.TSColumnArr.push(column.name);
            if (!column.field_mapping) {
              $scope.streaming.TSColumnMappingArr.push(column.field_mapping);
            } else {
              $scope.streaming.TSColumnMappingArr.push(column.comment);
            }
          }
        } else {
          if ($scope.streaming.TSColumnArr.includes(column.name)) {
            $scope.streaming.TSColumnArr.splice($scope.streaming.TSColumnArr.findIndex(function(opt) {return opt === column.name}), 1);
            $scope.streaming.TSColumnMappingArr.splice($scope.streaming.TSColumnArr.findIndex(function(opt) {return opt === column.name}), 1);
            if (column.name === $scope.streaming.TSColumnSelected) {
              $scope.streaming.TSColumnSelected = '';
            }
          }
        }
      };

      $scope.updateDateTimeParserOption = function(parser) {
        if (parser === $scope.streaming.TSParserArr[0]) {
          $scope.streaming.TSPatternArr = [];
          $scope.streaming.TSPatternArr.push('MS');
          $scope.streaming.TSPatternArr.push('S');
          $scope.streaming.TSPattern = 'MS';
        } else if (parser === $scope.streaming.TSParserArr[1]) {
          $scope.streaming.TSPatternArr = [];
          TableService.getSupportedDatetimePatterns({}, function (patterns) {
            $scope.streaming.TSPatternArr = patterns;
            $scope.streaming.TSPatternArr.push('--- Other ---');
            $scope.streaming.TSPattern = 'yyyy-MM-dd HH:mm:ss.SSS';
          }, function (e) {
            return;
          });
        }
      };

      $scope.updateTsPatternOption = function(pattern) {
        if (pattern === '--- Other ---') {
          $scope.selfDefinedTsPattern = true;
          $scope.streaming.TSPattern = '';
        } else {
          $scope.selfDefinedTsPattern = pattern;
          $scope.selfDefinedTsPattern = false;
        }
      };

      $scope.saveStreamingSource = function() {
        $scope.streaming.errMsg = '';

        if (!$scope.validateTableName()) {
          $scope.streaming.errMsg = 'Table name is invalid, please typing correct table name.';
          return;
        }

        // table column validation
        if ($scope.tableData.columns.length === 0) {
          $scope.streaming.errMsg = 'Table columns is empty, please add template to create it.';
          return;
        }

        var allColumnsAreDerived = true;
        angular.forEach($scope.tableData.columns, function(column) {
          if (!$scope.isDerived(column)) {
            allColumnsAreDerived = false;
          }
        });

        if (allColumnsAreDerived) {
          $scope.streaming.errMsg = 'All columns are derived, please add template to create it again.';
          return;
        }


        var streamingSourceConfigStr = '';

        // kafka config validation
        if (!$scope.streaming.TSColumnSelected) {
          $scope.streaming.errMsg = 'TSColumn is empty, please choose \'timestamp\' type column for TSColumn.';
          return;
        }
        // Set ts column
        $scope.streamingConfig.parser_info.ts_col_name = $scope.streaming.TSColumnSelected;
        $scope.streamingConfig.parser_info.ts_parser = $scope.streaming.TSParser;
        $scope.streamingConfig.parser_info.ts_pattern = $scope.streaming.TSPattern;
        $scope.streamingConfig.parser_info.field_mapping = {};
        $scope.tableData.columns.forEach(function(col) {
          if (col.comment) {
            $scope.streamingConfig.parser_info.field_mapping[col.name] = col.comment.replace(/\|/g, '.') || ''
          }
        })
        // rheos streaming
        if ($scope.tableConfig.streamingSourceType.rheos === $scope.tableData.source_type) {
          // rheos type streaming
          var index = $scope.streaming.TSColumnArr.findIndex(function (opt) {
            return opt === $scope.streaming.TSColumnSelected
          });
          $scope.streamingConfig.parser_info.field_mapping[$scope.streaming.TSColumnSelected] = $scope.streaming.TSColumnMappingArr[index].replace(/\|/g, '.');
        }
        SweetAlert.swal({
          title: '',
          text: 'Are you sure to save the streaming table?',
          showCancelButton: true,
          confirmButtonColor: '#DD6B55',
          confirmButtonText: "Yes",
          closeOnConfirm: true
        }, function (isConfirm) {
          if (isConfirm) {

            $scope.streamingConfig.name = $scope.tableData.name;

            // add column id
            var colInd = 0;
            angular.forEach($scope.tableData.columns, function(column) {
              column.id = colInd;
              colInd++;
            });
            var transformStreamingConfig = angular.toJson($scope._transformStreamingObj({project: projectName,
              tableData: $scope.tableData,
              streamingConfig: $scope.streamingConfig}));
            StreamingServiceV2.save({},
              transformStreamingConfig
            , function (request) {
              if (request.successful) {
                SweetAlert.swal('', 'Created the streaming successfully.', 'success');
                $scope.cancel();
                scope.aceSrcTbLoaded(true);
              } else {
                var message = request.message;
                var msg = !!(message) ? message : 'Failed to create streaming source.';
                $scope.streaming.errMsg = msg;
              }
              loadingRequest.hide();
            }, function (e) {
              if (e.data && e.data.exception) {
                var message = e.data.exception;
                var msg = !!(message) ? message : 'Failed to create streaming source.';
                $scope.streaming.errMsg = msg;
              } else {
                $scope.streaming.errMsg = 'Failed to create streaming source.';
              }
              //end loading
              loadingRequest.hide();
            });
          }
        });

      };

      $scope.validateTableName = function() {
        var tableName = $scope.tableData.name;
        if (tableName && tableName.length > 0) {
          if (tableName.split('.').length < 3 && tableName.indexOf('.') !== 0) {
            return true;
          } else {
            return false;
          }
        }
        return false;
      };

      $scope._transformStreamingObj = function(streamingObject) {
        var streamingRequest = {};
        streamingRequest.project = streamingObject.project;
        var streamingConfig = angular.copy(streamingObject.streamingConfig);
        streamingRequest.tableData = angular.copy(streamingObject.tableData);
        if (tableConfig.streamingSourceType.kafka === streamingRequest.tableData.source_type ||
            tableConfig.streamingSourceType.rheos === streamingRequest.tableData.source_type) {
            // Set bootstrap servers
            var bootstrapServersStr = '';
            angular.forEach(streamingObject.streamingConfig.properties.bootstrapServers, function(bootstrapServer, index) {
              bootstrapServersStr += ',' + bootstrapServer.host + ':' + bootstrapServer.port;
            });
            streamingConfig.properties['bootstrap.servers'] = bootstrapServersStr.substring(1);
            delete streamingConfig.properties.bootstrapServers;
        }
        if ($scope.streaming.lambda) {
          streamingRequest.tableData.source_type = streamingRequest.tableData.source_type + 1;
        }
        streamingRequest.tableData = angular.toJson(streamingRequest.tableData);
        streamingRequest.streamingConfig = angular.toJson(streamingConfig);
        return streamingRequest;
      };

    };
  });

/*snapshot controller*/
KylinApp
  .controller('TableSnapshotCtrl', function ($scope, TableService, CubeService, uiGridConstants) {
    $scope.initSnapshots = function() {
      var tableFullName = $scope.tableModel.selectedSrcTable.database + '.' + $scope.tableModel.selectedSrcTable.name
      TableService.getSnapshots({tableName: tableFullName, pro: $scope.projectModel.selectedProject}, {}, function (data) {
        var orgData = JSON.parse(angular.toJson(data));
        angular.forEach(orgData, function(snapshot) {
          if(!!snapshot.cubesAndSegmentsUsage && snapshot.cubesAndSegmentsUsage.length > 0) {
            snapshot.usageInfo = '';
            angular.forEach(snapshot.cubesAndSegmentsUsage, function(info) {
              snapshot.usageInfo += info;
              snapshot.usageInfo += '</br>';
            });
          } else {
            snapshot.usageInfo = 'No Usage Info';
          }
        });
        $scope.tableSnapshots = orgData;
      });
    };
    $scope.$watch('tableModel.selectedSrcTable', function (newValue, oldValue) {
      if (!newValue || !newValue.name) {
        return;
      }
      $scope.initSnapshots();
    });
  });

/*Lineage controller*/
KylinApp
  .controller('TableLineageCtrl', function ($scope, TableService, CubeService, uiGridConstants) {
    $scope.$watch('tableModel.selectedSrcTable', function (newValue, oldValue) {
      if (!newValue || !newValue.name) {
        return;
      }

      //data lineage
      $scope.lineage = {
        items: [],
        matrix: [],
        config: {
          margin: {
            left: 150,
            top: 10,
            right: 150,
            bottom: 10
          },
          width: 700,
          height: 646.66,
          outerRadius: 223.33,
          innerRadius: 212.16,
          pullOutSize: 50,
          opacityDefault: 0.7,
          opacityLow: 0.02
        }
      };

      $scope.lineageGridOptions = {
        enableFiltering: true,
        treeRowHeaderAlwaysVisible: false,
        enableRowHashing: false,
        columnDefs: [
          {name: 'table',
            grouping: {
              groupPriority: 0
            },
            sort: {
              priority: 0,
              direction: 'asc'
            },
            width: '25%',
            cellTemplate:
              '<div><div ng-if="!col.grouping || col.grouping.groupPriority === undefined || col.grouping.groupPriority === null || ( row.groupHeader && col.grouping.groupPriority === row.treeLevel )" class="ui-grid-cell-contents" title="TOOLTIP">{{COL_FIELD CUSTOM_FILTERS}}</div></div>'},
          { displayName: 'cube', name: 'name', width: '25%' },
          { name: 'model', width: '18%' },
          { name: 'project', width: '12%'},
          { name: 'owner', width: '10%'},
          { name: 'status'}
        ],
        onRegisterApi: function(gridApi) {
          $scope.lineageGridApi = gridApi;
          $scope.lineageGridApi.core.on.filterChanged($scope, function() {
            var grid = this.grid;
            var isfilterclear = true;
            angular.forEach(grid.columns, function( col ) {
              if(col.filters[0].term){
                isfilterclear = false;
              }
            });
            if(isfilterclear) {
              $scope.lineageGridApi.grid.columns[1].filters[0] = {
                condition: uiGridConstants.filter.STARTS_WITH
              };
              $scope.lineageGridApi.grid.columns[2].filters[0] = {
                condition: uiGridConstants.filter.STARTS_WITH
              };
            }
          });
        }
      };

      $scope.initLineage = function() {
        d3.select('#lineageChart').select('svg').remove();
        var tableFullName = $scope.tableModel.selectedSrcTable.database + '.' + $scope.tableModel.selectedSrcTable.name
        TableService.lineageCubes({tableName: tableFullName}, {}, function (data) {
          $scope.lineageData = [data.toJSON()];
          $scope.lineageData = _.sortBy($scope.lineageData, 'name').reverse();
          $scope.transformChartData($scope.lineageData);
          $scope.draw();
          $scope.transformGridData($scope.lineageData);
        });
      };

      $scope.transformGridData = function(orgData) {
        $scope.lineageGridOptions.data = [];
        // delete $$hashKey otherwise, the array will not correct
        orgData = JSON.parse(angular.toJson(orgData));
        angular.forEach(orgData, function(table){
          angular.forEach(table.cubes, function(cube) {
            cube.table = table.name.split('.')[1];
            $scope.lineageGridOptions.data.push(cube);
          });
        });
      };

      $scope.transformChartData = function(orgData) {
        $scope.lineage.matrix = [];
        $scope.lineage.items = [];
        var tables = [];
        var cubes = [];
        angular.forEach(orgData, function(table) {
          tables.push(table.name);
          table.cubes.forEach(function(cube) {
            cubes.push(cube);
          });
        });

        cubes = _.uniq(cubes, false, function(cube){ return cube.name; });

        $scope.lineage.items = cubes.concat('').concat(tables).concat('');

        $scope.lineage.config.emptyPerc = 0.4;
        if (tables.length === 1 && cubes.length ===1) {
          $scope.lineage.config.respondents = 1;
          $scope.lineage.config.emptyStroke =$scope.lineage.config.respondents * $scope.lineage.config.emptyPerc;
        } else {
          $scope.lineage.config.respondents = 0;
          angular.forEach(orgData, function(table) {
            $scope.lineage.config.respondents += table.cubes.length;
          });
          $scope.lineage.config.emptyStroke = Math.round($scope.lineage.config.respondents * $scope.lineage.config.emptyPerc);
        }
        $scope.lineage.config.cubeCount = cubes.length;
        $scope.lineage.config.tableCount = tables.length;

        angular.forEach($scope.lineage.items, function(name, ind){
          var matrix = [];
          if (ind < cubes.length) {
            for (var i = 0; i < cubes.length; i++) {
              matrix.push(0);
            }
            matrix.push(0);
            for (var i = 0; i < tables.length; i++) {
              var cubeRelated = false;
              $scope.lineageData[i].cubes.forEach(function(cube) {
                if (cubes[ind].name === cube.name)
                  cubeRelated = true;
              });
              if (cubeRelated) {
                matrix.push(1);
              } else {
                matrix.push(0);
              }
            }
            matrix.push(0);
          } else if (ind === cubes.length) {
            for (var i = 0; i < $scope.lineage.items.length-1; i++) {
              matrix.push(0);
            }
            matrix.push($scope.lineage.config.emptyStroke);
          } else if (ind < $scope.lineage.items.length - 1) {
            for (var i = 0; i < cubes.length; i++) {
              var tableRelated = false;
              $scope.lineageData[ind - cubes.length -1].cubes.forEach(function(cube){
                if (cube.name === cubes[i].name)
                  tableRelated = true;
              });
              if (tableRelated) {
                matrix.push(1);
              } else {
                matrix.push(0);
              }
            }
            matrix.push(0);
            for (var i = 0; i < tables.length; i++) {
              matrix.push(0);
            }
            matrix.push(0);
          } else if (ind === $scope.lineage.items.length -1) {
            for (var i = 0; i < cubes.length; i++) {
              matrix.push(0);
            }
            matrix.push($scope.lineage.config.emptyStroke);
            for (var i = 0; i < tables.length; i++) {
              matrix.push(0);
            }
            matrix.push(0);
          }
          $scope.lineage.matrix.push(matrix);
        });
      };

      $scope.draw = function() {
        var svg = d3.select('#lineageChart').append('svg')
          .attr('width', ($scope.lineage.config.width + $scope.lineage.config.margin.left + $scope.lineage.config.margin.right))
          .attr('height', ($scope.lineage.config.height + $scope.lineage.config.margin.top + $scope.lineage.config.margin.bottom));

        var wrapper = svg.append('g').attr('class', 'chordWrapper')
          .attr('transform', 'translate(' + ($scope.lineage.config.width / 2 + $scope.lineage.config.margin.left) + ',' + ($scope.lineage.config.height / 2 + $scope.lineage.config.margin.top) + ')');


        var chord = customChordLayout().padding(.02).sortChords(d3.ascending).matrix($scope.lineage.matrix);

        $scope.lineage.config.offset = (2 * Math.PI) * ($scope.lineage.config.emptyStroke/($scope.lineage.config.respondents + $scope.lineage.config.emptyStroke))/4;


        // title
        var titleWrapper = svg.append('g').attr('class', 'lineage-title'), titleOffset = 40;

        titleWrapper.append('text')
          .attr('class', 'lineage-title left')
          .style('font-size', '16px')
          .attr('x', ($scope.lineage.config.width/2 + $scope.lineage.config.margin.left - $scope.lineage.config.outerRadius - $scope.lineage.config.margin.left))
          .attr('y', titleOffset)
          .text('Tables');
        titleWrapper.append('line')
          .attr('class','lineage-title-line left')
          .attr('x1', ($scope.lineage.config.width/2 + $scope.lineage.config.margin.left - $scope.lineage.config.outerRadius)*0.9 - $scope.lineage.config.margin.left)
          .attr('x2', ($scope.lineage.config.width/2 + $scope.lineage.config.margin.left - $scope.lineage.config.outerRadius)*1.1 - $scope.lineage.config.margin.left)
          .attr('y1', titleOffset+8)
          .attr('y2', titleOffset+8);
        //Title top right
        titleWrapper.append('text')
          .attr('class', 'lineage-title lineage-right')
          .style('font-size', '16px')
          .attr('x', ($scope.lineage.config.width/2 + $scope.lineage.config.margin.left + $scope.lineage.config.outerRadius + $scope.lineage.config.margin.left))
          .attr('y', titleOffset)
          .text('Cubes');
        titleWrapper.append('line')
          .attr('class','lineage-title-line right')
          .attr('x1', ($scope.lineage.config.width/2 +$scope.lineage.config. margin.left - $scope.lineage.config.outerRadius)*0.9 + 2*($scope.lineage.config.outerRadius) + $scope.lineage.config.margin.left)
          .attr('x2', ($scope.lineage.config.width/2 + $scope.lineage.config.margin.left - $scope.lineage.config.outerRadius)*1.1 + 2*($scope.lineage.config.outerRadius) + $scope.lineage.config.margin.left)
          .attr('y1', titleOffset+8)
          .attr('y2', titleOffset+8);

        var defs = wrapper.append('defs');
        var linearGradient = defs.append('linearGradient')
          .attr('id', 'animatedGradient')
          .attr('x1', '0%')
          .attr('y1', '0%')
          .attr('x2', '100%')
          .attr('y2', '0')
          .attr('spreadMethod', 'reflect');

        linearGradient.append('animate')
          .attr('attributeName', 'x1')
          .attr('values', '0%;100%')
          .attr('dur', '7s')
          .attr('repeatCount', 'indefinite');

        linearGradient.append('animate')
          .attr('attributeName', 'x2')
          .attr('values', '100%;200%')
          .attr('dur', '7s')
          .attr('repeatCount', 'indefinite');

        linearGradient.append('stop')
          .attr('offset', '5%')
          .attr('stop-color', '#E8E8E8');
        linearGradient.append('stop')
          .attr('offset', '45%')
          .attr('stop-color', '#A3A3A3');
        linearGradient.append('stop')
          .attr('offset', '55%')
          .attr('stop-color', '#A3A3A3');
        linearGradient.append('stop')
          .attr('offset', '95%')
          .attr('stop-color', '#E8E8E8');

        // Define the div for the tooltip
        if (!document.getElementById('lineage-tooltip')){
          var tooltips = d3.select('body').append('div')
            .attr('id', 'lineage-tooltip')
            .style('opacity', 0);
        } else {
          var tooltips = d3.select('#lineage-tooltip');
        }

        var arc = d3.svg.arc()
          .innerRadius($scope.lineage.config.innerRadius)
          .outerRadius($scope.lineage.config.outerRadius)
          .startAngle(startAngle)
          .endAngle(endAngle);

        var path = stretchedChord()
          .radius($scope.lineage.config.innerRadius)
          .startAngle(startAngle)
          .endAngle(endAngle)
          .pullOutSize($scope.lineage.config.pullOutSize);

        var g = wrapper.selectAll('g.group')
          .data(chord.groups)
          .enter().append('g')
          .attr('class', 'group')
          .on('mouseover', function(d, i) {
            d3.select(this).style('cursor', 'pointer');
            fade($scope.lineage.config.opacityLow, d, i, svg);
            tooltips.transition()
              .duration(200)
              .style('opacity', .9);
            var htmlContent = '';
            if (d.index < $scope.lineage.config.cubeCount) {
              var cube = $scope.lineage.items[d.index];
              htmlContent += cube.name + '</br>';
              htmlContent += 'Status: ' + cube.status + '</br>';
              htmlContent += 'Owner: ' + cube.owner + '</br>';
              htmlContent += 'Project: ' + cube.project + '</br>';
              htmlContent += 'Model: ' + cube.model;
              tooltips.attr('class', 'lineage-cube-tooltip');
            } else if (d.index > $scope.lineage.config.cubeCount) {
              htmlContent = $scope.lineage.items[d.index];
              tooltips.attr('class', 'lineage-table-tooltip');
            }
            tooltips.html(htmlContent)
              .style('left', (d3.event.pageX + 'px'))
              .style('top', (d3.event.pageY + 'px'));
          })
          .on('mouseout', function(d, i) {
            d3.select(this).style('cursor', 'default');
            fade($scope.lineage.config.opacityDefault, d, i, svg);
            tooltips.transition()
              .duration(500)
              .style('opacity', 0);
          });

        g.append('path')
          .style('stroke', function(d,i) {
            return ($scope.lineage.items[i] === '' ? 'none' : '#00A1DE');
          })
          .style('fill', function(d,i) {
            return ($scope.lineage.items[i] === '' ? 'none' : '#00A1DE');
          })
          .style('pointer-events', function(d,i) {
            return ($scope.lineage.items[i] === '' ? 'none' : 'auto');
          })
          .attr('d', arc)
          .attr('transform', function(d, i) {
            d.pullOutSize = $scope.lineage.config.pullOutSize * ( i > $scope.lineage.config.cubeCount ? -1 : 1);
            return 'translate(' + d.pullOutSize + ',' + 0 + ')';
          });

        g.append('text')
          .each(function(d) {
            d.angle = ((d.startAngle + d.endAngle) / 2) + $scope.lineage.config.offset;
          })
          .attr('dy', '.15em')
          .attr('class', 'titles')
          .attr('text-anchor', function(d) {
            return d.angle > Math.PI ? 'end' : null;
          })
          .attr('transform', function(d,i) {
            var c = arc.centroid(d);
            return 'translate(' + (c[0] + d.pullOutSize) + ',' + c[1] + ')'
              + 'rotate(' + (d.angle * 180 / Math.PI - 90) + ')'
              + 'translate(' + 15 + ',0)'
              + (d.angle > Math.PI ? 'rotate(180)' : '')
          })
          .text(function(d,i) {
            if (i < $scope.lineage.config.cubeCount) {
              var cubeName = $scope.lineage.items[i].name;
              return cubeName.length > 20 ? cubeName.substring(0, 20) + '...' : cubeName;
            } else if (i > $scope.lineage.config.cubeCount && i < $scope.lineage.items.length -1){
              var tableName = $scope.lineage.items[i].split('.')[1];
              return tableName.length > 20 ? tableName.substring(0, 20) + '...' : tableName;
            }
          });

        var chords = wrapper.selectAll('path.chord')
          .data(chord.chords)
          .enter().append('path')
          .attr('class', 'chord')
          .style('stroke', 'none')
          .style('fill', 'url(#animatedGradient)')
          .style('opacity', function(d) {
            return ($scope.lineage.items[d.source.index] === '' ? 0 : $scope.lineage.config.opacityDefault);
          })
          .style('pointer-events', function(d,i) {
            return ($scope.lineage.items[d.source.index] === '' ? 'none' : 'auto');
          })
          .attr('d', path);

        var clicks = 0;
        var timer = null;

        g.on('click', function(d, i){
          clicks++;
          if (clicks === 1) {
            timer = setTimeout(function() {
              if (i < $scope.lineage.config.cubeCount) {
                $scope.lineageData = [];
                getTablesByCube($scope.lineage.items[i].name);
              } else if (i > $scope.lineage.config.cubeCount) {
                getCubesByTable($scope.lineage.items[i], true);
              }
              clicks = 0;
            }, 500);
          } else {
            clearTimeout(timer);
            clicks = 0;
          }
        });

        g.on('dblclick', function(d, i) {
          if (i < $scope.lineage.config.cubeCount) {
            getTablesByCube($scope.lineage.items[i].name);
          } else if (i > $scope.lineage.config.cubeCount) {
            getCubesByTable($scope.lineage.items[i]);
          }
        });

        chords.append('title')
          .text(function(d, i) {
            var sourceName = '';
            if (d.target.index < $scope.lineage.config.cubeCount) {
              sourceName = $scope.lineage.items[d.target.index].name;
            } else if (d.target.index > $scope.lineage.config.cubeCount) {
              sourceName = $scope.lineage.items[d.target.index];
            }
            var targetName = '';
            if (d.source.index < $scope.lineage.config.cubeCount) {
              targetName = $scope.lineage.items[d.source.index].name;
            } else if (d.source.index > $scope.lineage.config.cubeCount) {
              targetName = $scope.lineage.items[d.source.index];
            }
            return [sourceName, ' => ', targetName].join('');
          });
      };

      function getTablesByCube(cubeName) {
        var cube = _.find($scope.lineage.items.slice(0, $scope.lineage.config.cubeCount), function(item) {
          return item.name === cubeName;
        });
        TableService.lineageTables({cubeName: cubeName}, {}, function (data) {
          angular.forEach(angular.fromJson(data), function(tableName) {
            var tableExist = false;
            angular.forEach($scope.lineageData, function(table, ind) {
              if (table.name === tableName) {
                tableExist = true;
                var cubeExist = _.find(table.cubes, function(item) {
                  return item.name === cubeName;
                });
                if (!cubeExist) {
                  $scope.lineageData[ind].cubes.push(cube);
                }
              }
            });
            if (!tableExist) {
              $scope.lineageData.push({
                name: tableName,
                cubes: [
                  cube
                ]
              });
            }
          });
          d3.select('#lineageChart').select('svg').remove();
          $scope.lineageData = _.sortBy($scope.lineageData, 'name').reverse();
          $scope.transformChartData($scope.lineageData);
          $scope.draw();
          $scope.transformGridData($scope.lineageData);
          addGridFilter('cube', cubeName);
        });
      };

      function getCubesByTable(tableName, only) {
        TableService.lineageCubes({tableName: tableName}, {}, function(data){
          var tableInfo = data.toJSON();
          var tableExist = false;
          angular.forEach($scope.lineageData, function(table, ind) {
            if (tableInfo.name === table.name) {
              $scope.lineageData.splice(ind, 1, tableInfo);
              tableExist = true;
            }
          });
          if (!tableExist) {
            $scope.lineageData.push(talbeInfo);
          }
          if (only) {
            var selectTable = _.find($scope.lineageData, function(item) {
              return item.name === tableName;
            });
            $scope.lineageData = [selectTable];
          }
          d3.select('#lineageChart').select('svg').remove();
          $scope.lineageData = _.sortBy($scope.lineageData, 'name').reverse();
          $scope.transformChartData($scope.lineageData);
          $scope.draw();
          $scope.transformGridData($scope.lineageData);
          addGridFilter('table', tableName.split('.')[1]);
        });
      };

      function addGridFilter(type, content) {
        if (type === 'table') {
          $scope.lineageGridApi.grid.columns[1].filters[0] = {
            condition: uiGridConstants.filter.EXACT,
            term: content
          };
          $scope.lineageGridApi.grid.columns[2].filters[0] = {
            condition: uiGridConstants.filter.EXACT,
            term: ''
          };
        } else if (type === 'cube') {
          $scope.lineageGridApi.grid.columns[1].filters[0] = {
            condition: uiGridConstants.filter.EXACT,
            term: ''
          };
          $scope.lineageGridApi.grid.columns[2].filters[0] = {
            condition: uiGridConstants.filter.EXACT,
            term: content
          };
        }
        $scope.lineageGridApi.grid.refresh();
      };

      function startAngle(d) {
        return d.startAngle + $scope.lineage.config.offset;
      };

      function endAngle(d) {
        return d.endAngle + $scope.lineage.config.offset;
      };

      function fade(opacity, d, i, svg) {
        svg.selectAll('path.chord')
          .filter(function(d) {
            return d.source.index !== i && d.target.index !== i && $scope.lineage.items[d.source.index] !== "";
          })
          .transition('fadeOnArc')
          .style('opacity', opacity);
      };

      $scope.initLineage();
    });
  });

/*Avoid watch method call twice*/
KylinApp
  .controller('StreamConfigDisplayCtrl', function ($scope, StreamingServiceV2, tableConfig) {
    $scope.$watch('tableModel.selectedSrcTable', function (newValue, oldValue) {
      if (!newValue) {
        return;
      }
      if (_.values(tableConfig.streamingSourceType).indexOf($scope.tableModel.selectedSrcTable.source_type) > -1) {
        var table = $scope.tableModel.selectedSrcTable;
        var streamingName = table.database+"."+table.name;
        StreamingServiceV2.getConfig({table:streamingName}, function (configs) {
          $scope.currentStreamingConfig = configs[0];
        });
      }
    });
  });
