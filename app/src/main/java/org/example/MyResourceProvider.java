package org.example;

import java.util.List;

import com.google.zetasql.SimpleColumn;
import com.google.zetasql.SimpleTable;
import com.google.zetasql.TypeFactory;
import com.google.zetasql.ZetaSQLType;
import com.google.zetasql.toolkit.catalog.FunctionInfo;
import com.google.zetasql.toolkit.catalog.ProcedureInfo;
import com.google.zetasql.toolkit.catalog.TVFInfo;
import com.google.zetasql.toolkit.catalog.bigquery.BigQueryResourceProvider;

public class MyResourceProvider implements BigQueryResourceProvider {
    @Override
    public List<SimpleTable> getTables(String projectId, List<String> tableReferences) {
        var tableName = "wikipedia";
        List<SimpleColumn> columns = List.of(
            new SimpleColumn(tableName, "title", TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING)),
            new SimpleColumn(tableName, "comment", TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING))

        );
        SimpleTable table = new SimpleTable(tableName, columns);
        return List.of(table);
    }

    @Override
    public List<SimpleTable> getAllTablesInDataset(String projectId, String datasetName) {
        System.out.println("Called here !!!!");
        System.out.println(datasetName);
        return this.getTables(projectId, null);
    }

    @Override
    public List<SimpleTable> getAllTablesInProject(String projectId) {
        return this.getTables(projectId, null);
    }

    @Override
    public List<FunctionInfo> getFunctions(String projectId, List<String> functionReferences) {
        return List.of();
    }

    @Override
    public List<FunctionInfo> getAllFunctionsInDataset(String projectId, String datasetName) {
        return List.of();
    }

    @Override
    public List<FunctionInfo> getAllFunctionsInProject(String projectId) {
        return List.of();
    }

    @Override
    public List<TVFInfo> getTVFs(String projectId, List<String> functionReferences) {
        return List.of();
    }

    @Override
    public List<TVFInfo> getAllTVFsInDataset(String projectId, String datasetName) {
        return List.of();
    }

    @Override
    public List<TVFInfo> getAllTVFsInProject(String projectId) {
        return List.of();
    }

    @Override
    public List<ProcedureInfo> getProcedures(String projectId, List<String> procedureReferences) {
        return List.of();
    }

    @Override
    public List<ProcedureInfo> getAllProceduresInDataset(String projectId, String datasetName) {
        return List.of();
    }

    @Override
    public List<ProcedureInfo> getAllProceduresInProject(String projectId) {
        return List.of();
    }

}
