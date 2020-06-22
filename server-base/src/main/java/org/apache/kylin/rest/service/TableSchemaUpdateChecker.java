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

package org.apache.kylin.rest.service;

import static java.lang.String.format;
import static org.apache.kylin.shaded.com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.MailService;
import org.apache.kylin.common.util.MailTemplateProvider;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.job.util.MailNotificationUtil;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.ModelDimensionDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.base.Predicate;
import org.apache.kylin.shaded.com.google.common.collect.ImmutableList;
import org.apache.kylin.shaded.com.google.common.collect.Iterables;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableSchemaUpdateChecker {
    private static final Logger logger = LoggerFactory.getLogger(TableSchemaUpdateChecker.class);

    private final TableMetadataManager metadataManager;
    private final CubeManager cubeManager;
    private final DataModelManager dataModelManager;

    public static class CheckResult {
        private final String tableName;
        private final boolean valid;
        private final String reason;
        private final Set<CubeInstance> impactedCubes;

        private CheckResult(String tableName, boolean valid, String reason) {
            this(tableName, valid, reason, Sets.<CubeInstance> newHashSet());
        }

        private CheckResult(String tableName, boolean valid, String reason, Set<CubeInstance> impactedCubes) {
            this.tableName = tableName;
            this.valid = valid;
            this.reason = reason;
            this.impactedCubes = impactedCubes;
        }

        public void raiseExceptionWhenInvalid() {
            raiseExceptionAndNotifyWhenInvalid(false);
        }

        public void raiseExceptionAndNotifyWhenInvalid() {
            raiseExceptionAndNotifyWhenInvalid(true);
        }

        private void raiseExceptionAndNotifyWhenInvalid(boolean ifNotify) {
            if (!valid) {
                if (ifNotify) {
                    notifyInvalid(tableName, reason, impactedCubes);
                }
                throw new IllegalArgumentException(reason);
            }
        }

        static CheckResult validOnFirstLoad(String tableName) {
            return new CheckResult(tableName, true,
                    format(Locale.ROOT, "Table '%s' hasn't been loaded before", tableName));
        }

        static CheckResult validOnCompatibleSchema(String tableName) {
            return new CheckResult(tableName, true,
                    format(Locale.ROOT, "Table '%s' is compatible with all existing cubes", tableName));
        }

        static CheckResult invalidOnFetchSchema(String tableName, Exception e) {
            return new CheckResult(tableName, false,
                    format(Locale.ROOT, "Failed to fetch metadata of '%s': %s", tableName, e.getMessage()));
        }

        static CheckResult invalidOnIncompatibleSchema(String tableName, List<String> reasons,
                Set<CubeInstance> impactedCubes) {
            StringBuilder buf = new StringBuilder();
            for (String reason : reasons) {
                buf.append("- ").append(reason).append("\n");
            }

            return new CheckResult(tableName, false,
                    format(Locale.ROOT,
                            "Found %d issue(s) with '%s':%n%s Please disable and " + "purge related " + "cube(s) first",
                            reasons.size(), tableName, buf.toString()),
                    impactedCubes);
        }
    }

    TableSchemaUpdateChecker(TableMetadataManager metadataManager, CubeManager cubeManager,
            DataModelManager dataModelManager) {
        this.metadataManager = checkNotNull(metadataManager, "metadataManager is null");
        this.cubeManager = checkNotNull(cubeManager, "cubeManager is null");
        this.dataModelManager = checkNotNull(dataModelManager, "dataModelManager is null");
    }

    private List<CubeInstance> findCubeByTable(final TableDesc table) {
        Iterable<CubeInstance> relatedCubes = Iterables.filter(cubeManager.listAllCubes(),
                new Predicate<CubeInstance>() {
                    @Override
                    public boolean apply(@Nullable CubeInstance cube) {
                        if (cube == null || cube.allowBrokenDescriptor()) {
                            return false;
                        }
                        DataModelDesc model = cube.getModel();
                        if (model == null)
                            return false;
                        return model.containsTable(table);
                    }
                });

        return ImmutableList.copyOf(relatedCubes);
    }

    private boolean isColumnCompatible(ColumnDesc column, ColumnDesc newCol) {
        if (!column.getName().equalsIgnoreCase(newCol.getName())) {
            return false;
        }

        if (column.getType().isIntegerFamily()) {
            // OLAPTable.listSourceColumns converts some integer columns to bigint,
            // therefore strict type comparison won't work.
            // changing from one integer type to another should be fine.
            return newCol.getType().isIntegerFamily();
        } else if (column.getType().isNumberFamily()) {
            // Both are float/double should be fine.
            return newCol.getType().isNumberFamily();
        } else if ((column.getType().isStringFamily() && newCol.getType().isDateTimeFamily())
                && metadataManager.getConfig().isAbleChangeStringToDateTime()) {
            // String can be converted to Date or Time
            return true;
        } else {
            // only compare base type name, changing precision or scale should be fine
            return column.getTypeName().equals(newCol.getTypeName());
        }
    }

    /**
     * check whether all columns used in `cube` has compatible schema in current hive schema denoted by `fieldsMap`.
     * @param cube cube to check, must use `table` in its model
     * @param origTable kylin's table metadata
     * @param newTable current hive schema of `table`
     * @return columns in origTable that can't be found in newTable
     */
    private List<String> checkAllColumnsInCube(CubeInstance cube, TableDesc origTable, TableDesc newTable) {
        Set<ColumnDesc> usedColumns = Sets.newHashSet();
        for (TblColRef col : cube.getAllColumns()) {
            usedColumns.add(col.getColumnDesc());
        }

        List<String> violateColumns = Lists.newArrayList();
        for (ColumnDesc column : origTable.getColumns()) {
            if (!column.isComputedColumn() && usedColumns.contains(column)) {
                ColumnDesc newCol = newTable.findColumnByName(column.getName());
                if (newCol == null || !isColumnCompatible(column, newCol)) {
                    violateColumns.add(column.getName());
                }
            }
        }
        return violateColumns;
    }

    /**
     * check whether all columns in `table` are still in `fields` and have the same index as before.
     *
     * @param origTable kylin's table metadata
     * @param newTable current table metadata in hive
     * @return true if only new columns are appended in hive, false otherwise
     */
    private boolean checkAllColumnsInTableDesc(TableDesc origTable, TableDesc newTable) {
        if (origTable.getColumnCount() > newTable.getColumnCount()) {
            return false;
        }

        ColumnDesc[] columns = origTable.getColumns();
        for (int i = 0; i < columns.length; i++) {
            if (!isColumnCompatible(columns[i], newTable.getColumns()[i])) {
                return false;
            }
        }
        return true;
    }

    public CheckResult allowReload(TableDesc newTableDesc, String prj) {
        final String fullTableName = newTableDesc.getIdentity();

        TableDesc existing = metadataManager.getTableDesc(fullTableName, prj);
        if (existing == null) {
            return CheckResult.validOnFirstLoad(fullTableName);
        }
        List<String> issues = Lists.newArrayList();

        for (DataModelDesc usedModel : findModelByTable(newTableDesc, prj)) {
            checkValidationInModel(newTableDesc, issues, usedModel);
        }

        Set<CubeInstance> impactedCubes = Sets.newHashSet();
        for (CubeInstance cube : findCubeByTable(newTableDesc)) {
            boolean ifImpacted = checkValidationInCube(newTableDesc, issues, cube);
            if (ifImpacted) {
                impactedCubes.add(cube);
            }
        }

        if (issues.isEmpty()) {
            return CheckResult.validOnCompatibleSchema(fullTableName);
        }
        return CheckResult.invalidOnIncompatibleSchema(fullTableName, issues, impactedCubes);
    }

    private Iterable<? extends DataModelDesc> findModelByTable(TableDesc newTableDesc, String prj) {
        List<DataModelDesc> usedModels = Lists.newArrayList();
        List<String> modelNames = dataModelManager.getModelsUsingTable(newTableDesc, prj);
        modelNames.stream().map(mn -> dataModelManager.getDataModelDesc(mn)).filter(m -> null != m)
                .forEach(m -> usedModels.add(m));

        return usedModels;
    }

    private boolean checkValidationInCube(TableDesc newTableDesc, List<String> issues, CubeInstance cube) {
        boolean ifImpacted = false;

        final String fullTableName = newTableDesc.getIdentity();
        String modelName = cube.getModel().getName();
        // if user reloads a fact table used by cube, then all used columns must match current schema
        if (cube.getModel().isFactTable(fullTableName)) {
            TableDesc factTable = cube.getModel().findFirstTable(fullTableName).getTableDesc();
            List<String> violateColumns = checkAllColumnsInCube(cube, factTable, newTableDesc);
            if (!violateColumns.isEmpty()) {
                issues.add(format(Locale.ROOT, "Column %s used in cube[%s] and model[%s], but changed " + "in hive",
                        violateColumns, cube.getName(), modelName));
                ifImpacted = true;
            }
        }

        // if user reloads a lookup table used by cube, only append column(s) are allowed, all existing columns
        // must be the same (except compatible type changes)
        if (cube.getModel().isLookupTable(fullTableName)) {
            TableDesc lookupTable = cube.getModel().findFirstTable(fullTableName).getTableDesc();
            if (!checkAllColumnsInTableDesc(lookupTable, newTableDesc)) {
                issues.add(format(Locale.ROOT,
                        "Table '%s' is used as Lookup Table in cube[%s] and model[%s], but " + "changed in "
                                + "hive, only append operation are supported on hive table as lookup table",
                        lookupTable.getIdentity(), cube.getName(), modelName));
                ifImpacted = true;
            }
        }

        return ifImpacted;
    }

    private void checkValidationInModel(TableDesc newTableDesc, List<String> issues, DataModelDesc usedModel) {
        List<String> violateColumns = Lists.newArrayList();

        final String fullTableName = newTableDesc.getIdentity();
        if (usedModel.isFactTable(fullTableName)) {
            TableDesc factTable = usedModel.findFirstTable(fullTableName).getTableDesc();
            violateColumns.addAll(checkAllColumnsInFactTable(usedModel, factTable, newTableDesc));
        }

        if (usedModel.isLookupTable(fullTableName)) {
            violateColumns.addAll(checkAllColumnsInLookupTable(usedModel, newTableDesc));
        }

        if (!violateColumns.isEmpty()) {
            issues.add(format(Locale.ROOT, "Column %s used in model[%s], but not exist " + "in hive", violateColumns,
                    usedModel.getName()));
        }
    }

    // columns in usedModel should not be deleted in new table 
    private List<String> checkAllColumnsInLookupTable(DataModelDesc usedModel, TableDesc newTableDesc) {
        List<String> violateColumns = Lists.newArrayList();

        Set<String> newColumns = Sets.newHashSet(newTableDesc.getColumns()).stream()
                .map(c -> c.getName().toUpperCase(Locale.ROOT)).collect(Collectors.toSet());
        for (ModelDimensionDesc dim : usedModel.getDimensions()) {
            if (dim.getTable().equalsIgnoreCase(newTableDesc.getIdentity())) {
                Set<String> oldColumns = Sets.newHashSet(dim.getColumns()).stream().map(c -> c.toUpperCase(Locale.ROOT))
                        .collect(Collectors.toSet());
                oldColumns.removeAll(newColumns);
                violateColumns.addAll(oldColumns);
            }
        }

        return violateColumns;
    }

    private List<String> checkAllColumnsInFactTable(DataModelDesc usedModel, TableDesc factTable,
            TableDesc newTableDesc) {
        List<String> violateColumns = Lists.newArrayList();

        for (ColumnDesc column : findUsedColumnsInFactTable(usedModel, factTable)) {
            if (!column.isComputedColumn()) {
                ColumnDesc newCol = newTableDesc.findColumnByName(column.getName());
                if (newCol == null) {
                    violateColumns.add(column.getName());
                }
            }
        }
        return violateColumns;
    }

    // get table name from column full name
    private String getTableName(String columnName) {
        int lastIndexOfDot = columnName.lastIndexOf('.');
        String tableName = null;
        if (lastIndexOfDot >= 0) {
            tableName = columnName.substring(0, lastIndexOfDot);
        } else {
            return null;
        }
        // maybe contain db name
        lastIndexOfDot = tableName.lastIndexOf('.');
        if (lastIndexOfDot >= 0) {
            tableName = tableName.substring(lastIndexOfDot + 1);
        }
        return tableName;
    }

    private ColumnDesc mustGetColumnDesc(TableDesc factTable, String columnName) {
        ColumnDesc columnDesc = factTable.findColumnByName(columnName);
        Preconditions.checkNotNull(columnDesc, format(Locale.ROOT, "Can't find column %s in current fact table %s.",
                columnName, factTable.getIdentity()));
        return columnDesc;
    }

    private Set<ColumnDesc> findUsedColumnsInFactTable(DataModelDesc usedModel, TableDesc factTable) {
        Set<ColumnDesc> usedColumns = Sets.newHashSet();
        // column in dimension
        for (ModelDimensionDesc dim : usedModel.getDimensions()) {
            if (dim.getTable().equalsIgnoreCase(factTable.getName())) {
                for (String col : dim.getColumns()) {
                    usedColumns.add(mustGetColumnDesc(factTable, col));
                }
            }
        }

        // column in measure
        for (String columnInMeasure : usedModel.getMetrics()) {
            if (factTable.getName().equalsIgnoreCase(getTableName(columnInMeasure))) {
                usedColumns.add(mustGetColumnDesc(factTable, columnInMeasure));
            }
        }

        return usedColumns;
    }

    public CheckResult allowMigrate(TableDesc newTableDesc, TableDesc hiveTableDesc) throws Exception {
        final String fullTableName = newTableDesc.getIdentity();

        List<String> issues = Lists.newArrayList();
        checkAllColumnsInHiveTableDesc(hiveTableDesc, newTableDesc, issues);
        if (issues.isEmpty()) {
            return new CheckResult(fullTableName, true,
                    format(Locale.ROOT, "Table '%s' is compatible with existing hive table", fullTableName));
        } else {
            return new CheckResult(fullTableName, false, format(Locale.ROOT,
                    "Table '%s' is incompatible with existing hive table due to '%s'", fullTableName, issues));
        }
    }

    private void checkAllColumnsInHiveTableDesc(TableDesc hiveTable, TableDesc newTable, List<String> issues) {
        ColumnDesc[] hiveTableCols = hiveTable.getColumns();
        ColumnDesc[] newTableCols = newTable.getColumns();

        if (hiveTableCols.length < newTableCols.length) {
            Set<String> colNamesNew = Lists.newArrayList(newTableCols).stream().map(input -> input.getName())
                    .collect(Collectors.toSet());
            Set<String> colNamesHive = Lists.newArrayList(hiveTableCols).stream().map(input -> input.getName())
                    .collect(Collectors.toSet());
            colNamesNew.removeAll(colNamesHive);
            issues.add(format(Locale.ROOT, "columns %s are not existing in hive table", colNamesNew));
            return;
        }

        Map<String, ColumnDesc> hiveColMap = Lists.newArrayList(hiveTableCols).stream()
                .collect(Collectors.toMap(input -> input.getName().toUpperCase(Locale.ROOT), input -> input));
        Map<String, ColumnDesc> newColMap = Lists.newArrayList(newTableCols).stream()
                .collect(Collectors.toMap(input -> input.getName().toUpperCase(Locale.ROOT), input -> input));

        List<String> violateColumns = Lists.newArrayList();
        for (String colName : newColMap.keySet()) {
            ColumnDesc hiveCol = hiveColMap.get(colName);
            if (hiveCol == null) {
                issues.add(format(Locale.ROOT, "column %s is not existing in hive table", colName));
                continue;
            }
            if (!isColumnCompatible(hiveCol, newColMap.get(colName))) {
                violateColumns.add(colName);
            }
        }
        if (!violateColumns.isEmpty()) {
            issues.add(format(Locale.ROOT, "Columns %s are incompatible " + "in hive", violateColumns));
        }
    }

    private static void notifyInvalid(String tableName, String reason, Set<CubeInstance> impactedCubes) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        Set<String> users = Sets.newHashSet();
        for (CubeInstance cube : impactedCubes) {
            users.addAll(cube.getDescriptor().getNotifyList());
        }
        final String[] adminDls = kylinConfig.getAdminDls();
        if (adminDls != null) {
            users.addAll(Lists.newArrayList(adminDls));
        }

        logger.debug("send notification to owner of {} impacted cubes for table {}", impactedCubes.size(), tableName);
        Pair<String, String> mail = formatNotifications(tableName, impactedCubes, reason);
        new MailService(kylinConfig).sendMail(Lists.newArrayList(users), mail.getFirst(), mail.getSecond());
    }

    private static Pair<String, String> formatNotifications(String tableName, Set<CubeInstance> cubes,
            String issueDetails) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        ProjectManager projectManager = ProjectManager.getInstance(kylinConfig);

        Map<String, Object> root = Maps.newHashMap();
        root.put("job_name", "Source Data Schema Change");
        root.put("env_name", kylinConfig.getDeployEnv());
        root.put("change_details", Matcher.quoteReplacement(StringUtil.noBlank(issueDetails, "no change info")));
        root.put("source_type", "HIVE");
        root.put("source_data", tableName);

        List<Map> impactInfoList = Lists.newArrayList();
        for (CubeInstance cube : cubes) {
            Map<String, Object> impactInfoMap = Maps.newHashMap();
            impactInfoMap.put("project_name", projectManager.getProjectOfModel(cube.getModel().getName()));
            impactInfoMap.put("cube_name", cube.getName());
            impactInfoMap.put("cube_owner", cube.getOwner());
            impactInfoList.add(impactInfoMap);
        }

        root.put("impacted_info_List", impactInfoList);

        String content = MailTemplateProvider.getInstance().buildMailContent("HIVE_SCHEMA_CHANGED", root);
        String title = MailNotificationUtil.getMailTitle("TABLE SCHEMA", "CHANGED", kylinConfig.getDeployEnv());
        return Pair.newPair(title, content);
    }
}
