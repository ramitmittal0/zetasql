/*
 * Copyright 2023 Google LLC All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example;

import com.google.zetasql.Analyzer;
import com.google.zetasql.AnalyzerOptions;
import com.google.zetasql.LanguageOptions;
import com.google.zetasql.Parser;
import com.google.zetasql.ZetaSQLOptions;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedCreateTableAsSelectStmt;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedStatement;
import com.google.zetasql.toolkit.AnalyzedStatement;
import com.google.zetasql.toolkit.ZetaSQLToolkitAnalyzer;
import com.google.zetasql.toolkit.catalog.bigquery.BigQueryCatalog;
import com.google.zetasql.toolkit.options.BigQueryLanguageOptions;
import com.google.zetasql.toolkit.tools.lineage.ColumnEntity;
import com.google.zetasql.toolkit.tools.lineage.ColumnLineage;
import com.google.zetasql.toolkit.tools.lineage.ColumnLineageExtractor;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

public class App {

  private static void outputLineage(String query, Set<ColumnLineage> lineageEntries) {
    System.out.println("\nQuery:");
    System.out.println(query);
    System.out.println("\nLineage:");
    lineageEntries.forEach(
        lineage -> {
          System.out.printf("%s.%s\n", lineage.target.table, lineage.target.name);
          for (ColumnEntity parent : lineage.parents) {
            System.out.printf("\t\t<- %s.%s\n", parent.table, parent.name);
          }
        });
    System.out.println();
    System.out.println();
  }

  private static void lineageForCreateTableAsSelectStatement(
      BigQueryCatalog catalog, AnalyzerOptions options) {
    String query =
        """
create or replace temp table all_latest_ratings as (
select * from `default.samples.stg_data`
pivot(string_agg(prev_rating_value) as latest_rating, string_agg(prev_rating_name) as latest_perf_cycle for 
   cast(prev_rating_rank as string)  in ('0','1','2')))
        """;

  // LanguageOptions languageOptions = new LanguageOptions().enableMaximumLanguageFeatures();
  // languageOptions.setSupportsAllStatementKinds();

  //   var x = Parser.parseScript(query, languageOptions);
  //   var sb = new StringBuilder();
  //   x.debugStringImpl("    ", " ", sb);
  //   System.out.println(sb.toString());
    
  //   x.accept(MyVisitor);


  // Set<String> tables =
  // Analyzer.extractTableNamesFromScript(query, options).stream()
  //     .map(tablePath -> String.join(".", tablePath))
  //     .collect(Collectors.toSet());
  // System.out.println(tables);

    ZetaSQLToolkitAnalyzer analyzer = new ZetaSQLToolkitAnalyzer(options);
    Iterator<AnalyzedStatement> statementIterator = analyzer.analyzeStatements(query, catalog);
    ResolvedStatement statement = statementIterator.next().getResolvedStatement().get();


    ResolvedCreateTableAsSelectStmt createTableAsSelectStmt =
        (ResolvedCreateTableAsSelectStmt) statement;


    Set<ColumnLineage> lineageEntries =
        ColumnLineageExtractor.extractColumnLevelLineage(createTableAsSelectStmt);

    System.out.println("Extracted column lineage from CREATE TABLE AS SELECT");
    outputLineage(query, lineageEntries);
  }

  // private static void lineageForInsertStatement(
  //     BigQueryCatalog catalog, ZetaSQLToolkitAnalyzer analyzer) {
  //   String query =
  //       "INSERT INTO `bigquery-public-data.samples.wikipedia`(title, comment)\n"
  //           + "SELECT\n"
  //           + "    LOWER(upper_corpus) AS titleaaaaaa,\n"
  //           + "    UPPER(lower_word) AS comment\n"
  //           + "FROM (\n"
  //           + "    SELECT\n"
  //           + "      UPPER(corpus) AS upper_corpus,\n"
  //           + "      LOWER(word) AS lower_word\n"
  //           + "    FROM `bigquery-public-data.samples.shakespeare`\n"
  //           + "    WHERE word_count > 10\n"
  //           + "    );";

  //   Iterator<AnalyzedStatement> statementIterator = analyzer.analyzeStatements(query, catalog);

  //   ResolvedStatement statement = statementIterator.next().getResolvedStatement().get();
  //   ResolvedInsertStmt insertStmt = (ResolvedInsertStmt) statement;

  //   Set<ColumnLineage> lineageEntries =
  //       ColumnLineageExtractor.extractColumnLevelLineage(insertStmt);

  //   System.out.println("Extracted column lineage from INSERT");
  //   outputLineage(query, lineageEntries);
  // }

  // private static void lineageForUpdateStatement(
  //     BigQueryCatalog catalog, ZetaSQLToolkitAnalyzer analyzer) {
  //   String query =
  //       "UPDATE `bigquery-public-data.samples.wikipedia` W\n"
  //           + "    SET title = S.corpus, comment = S.word\n"
  //           + "FROM (SELECT corpus, UPPER(word) AS word FROM `bigquery-public-data.samples.shakespeare`) S\n"
  //           + "WHERE W.title = S.corpus;";

  //   Iterator<AnalyzedStatement> statementIterator = analyzer.analyzeStatements(query, catalog);

  //   ResolvedStatement statement = statementIterator.next().getResolvedStatement().get();
  //   ResolvedUpdateStmt updateStmt = (ResolvedUpdateStmt) statement;

  //   Set<ColumnLineage> lineageEntries =
  //       ColumnLineageExtractor.extractColumnLevelLineage(updateStmt);

  //   System.out.println("Extracted column lineage from UPDATE");
  //   outputLineage(query, lineageEntries);
  // }

  // private static void lineageForMergeStatement(
  //     BigQueryCatalog catalog, ZetaSQLToolkitAnalyzer analyzer) {
  //   String query =
  //       "MERGE `bigquery-public-data.samples.wikipedia` W\n"
  //           + "USING (SELECT corpus, UPPER(word) AS word FROM `bigquery-public-data.samples.shakespeare`) S\n"
  //           + "ON W.title = S.corpus\n"
  //           + "WHEN MATCHED THEN\n"
  //           + "    UPDATE SET comment = S.word\n"
  //           + "WHEN NOT MATCHED THEN\n"
  //           + "    INSERT(title) VALUES (UPPER(corpus));";

  //   Iterator<AnalyzedStatement> statementIterator = analyzer.analyzeStatements(query, catalog);

  //   ResolvedStatement statement = statementIterator.next().getResolvedStatement().get();
  //   ResolvedMergeStmt mergeStmt = (ResolvedMergeStmt) statement;

  //   Set<ColumnLineage> lineageEntries = ColumnLineageExtractor.extractColumnLevelLineage(mergeStmt);

  //   System.out.println("Extracted column lineage from MERGE");
  //   outputLineage(query, lineageEntries);
  // }

  public static void main(String[] args) {
    BigQueryCatalog catalog = new BigQueryCatalog("default", new MyResourceProvider());
    catalog.addAllTablesInProject("default");

    LanguageOptions languageOptions = new LanguageOptions().enableMaximumLanguageFeatures();
    languageOptions.setSupportsAllStatementKinds();

    AnalyzerOptions options = new AnalyzerOptions();
    options.setLanguageOptions(languageOptions);

    lineageForCreateTableAsSelectStatement(catalog, options);
    // System.out.println("-----------------------------------");
    // lineageForInsertStatement(catalog, analyzer);
    // System.out.println("-----------------------------------");
    // lineageForUpdateStatement(catalog, analyzer);
    // System.out.println("-----------------------------------");
    // lineageForMergeStatement(catalog, analyzer);
  }
}