package io.factry.historian.gateway;

import com.inductiveautomation.historian.common.model.options.RawQueryOptions;
import com.inductiveautomation.historian.common.model.options.ResultWriterOptions;
import com.inductiveautomation.historian.gateway.api.Historian;
import com.inductiveautomation.historian.gateway.api.paths.QualifiedPathAdapter;
import com.inductiveautomation.historian.gateway.api.query.QueryEngine;
import com.inductiveautomation.historian.gateway.interop.HistorianQueryInterface;
import com.inductiveautomation.historian.gateway.query.writing.HistoryWriter;
import com.inductiveautomation.historian.gateway.util.HistorianUtilities;
import com.inductiveautomation.ignition.common.FillMode;
import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.StreamingDatasetWriter;
import com.inductiveautomation.ignition.common.model.values.QualityCode;
import com.inductiveautomation.ignition.common.sqltags.history.AggregationMode;
import com.inductiveautomation.ignition.common.sqltags.history.BasicTagHistoryQueryParams;
import com.inductiveautomation.ignition.common.sqltags.history.ReturnFormat;
import com.inductiveautomation.ignition.common.sqltags.history.TagHistoryQueryParams;
import com.inductiveautomation.ignition.common.util.Flags;
import com.inductiveautomation.ignition.gateway.sqltags.history.query.ColumnQueryDefinition;
import com.inductiveautomation.ignition.gateway.sqltags.history.query.HistoryQueryExecutor;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * Runs a wide-format history query the way Ignition 8.3 does for queryTagHistory and the Power
 * Chart: HistorianQueryInterface builds the executor, HistoryWriter turns its points into rows.
 */
final class IgnitionHistoryQuery {

    private IgnitionHistoryQuery() {
    }

    /** The dataset Ignition builds, row by row. */
    static final class Dataset implements StreamingDatasetWriter {
        String[] columns;
        final List<Object[]> rows = new ArrayList<>();
        final List<QualityCode[]> qualities = new ArrayList<>();
        Exception error;
        boolean finished;

        @Override
        public void initialize(String[] names, Class<?>[] types, boolean qualityCodes, int expectedRows) {
            columns = names;
        }

        @Override
        public void write(Object[] row, QualityCode[] quality) {
            rows.add(row.clone());
            qualities.add(quality.clone());
        }

        @Override
        public void finish() {
            finished = true;
        }

        @Override
        public void finishWithError(Exception e) {
            error = e;
        }

        long time(int row) {
            return ((Date) rows.get(row)[0]).getTime();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(Arrays.toString(columns));
            for (int i = 0; i < rows.size(); i++) {
                sb.append("\n").append(Arrays.toString(rows.get(i))).append(" ").append(Arrays.toString(qualities.get(i)));
            }
            return sb.append("\nerror: ").append(error).toString();
        }
    }

    /**
     * @param returnSize -1 for a raw read; with an aggregate, the number of intervals
     * @param aggregate  null for a raw read
     * @param tags       tag paths under provider "default", also used as column names
     */
    static Dataset wide(QueryEngine engine, long start, long end, int returnSize, AggregationMode aggregate,
                        String... tags) throws Exception {
        return wide(engine, start, end, returnSize, aggregate, FillMode.NONE, tags);
    }

    /**
     * @param fill with an aggregate, the fill mode of every column. FillMode.DERIVED makes Ignition
     *             give each column its own executor and query.
     */
    static Dataset wide(QueryEngine engine, long start, long end, int returnSize, AggregationMode aggregate,
                        FillMode fill, String... tags) throws Exception {
        return wide(engine, start, end, returnSize, aggregate, fill, 0, tags);
    }

    private static Dataset wide(QueryEngine engine, long start, long end, int returnSize, AggregationMode aggregate,
                                FillMode fill, int flags, String... tags) throws Exception {
        List<QualifiedPath> paths = new ArrayList<>();
        List<ColumnQueryDefinition> defs = new ArrayList<>();
        for (String tag : tags) {
            QualifiedPath path = QualifiedPath.parseSafe("sys:gw:/prov:default:/tag:" + tag);
            paths.add(path);
            defs.add(aggregate == null
                    ? new ColumnQueryDefinition(path, tag)
                    : new ColumnQueryDefinition(path, aggregate, fill, tag));
        }
        BasicTagHistoryQueryParams params = new BasicTagHistoryQueryParams(paths, new Date(start), new Date(end),
                returnSize, aggregate, ReturnFormat.Wide, List.of(tags), List.of());
        params.setQueryFlags(new Flags(flags));
        return run(engine, params, defs);
    }

    /**
     * A raw read with Ignition's legacy query flags, as queryTagHistory sets them: 16 for
     * includeBoundingValues=True, 32 for False, 1 for no seed values.
     */
    static Dataset raw(QueryEngine engine, long start, long end, int returnSize, int flags, String... tags)
            throws Exception {
        return wide(engine, start, end, returnSize, null, FillMode.NONE, flags, tags);
    }

    /**
     * A raw wide read of whole history paths, as a pen picked from the browse tree or a script
     * names them (for example {@code histprov:Factry:/tag:default/Line/Flow}). Columns are
     * named "c0", "c1", ...
     */
    static Dataset rawPaths(QueryEngine engine, long start, long end, String... paths) throws Exception {
        List<QualifiedPath> qualified = new ArrayList<>();
        List<ColumnQueryDefinition> defs = new ArrayList<>();
        List<String> aliases = new ArrayList<>();
        for (int i = 0; i < paths.length; i++) {
            QualifiedPath path = QualifiedPath.parseSafe(paths[i]);
            qualified.add(path);
            aliases.add("c" + i);
            defs.add(new ColumnQueryDefinition(path, "c" + i));
        }
        BasicTagHistoryQueryParams params = new BasicTagHistoryQueryParams(qualified, new Date(start), new Date(end),
                -1, null, ReturnFormat.Wide, aliases, List.of());
        return run(engine, params, defs);
    }

    /**
     * A read the way system.historian.queryRawPoints runs it in 8.3.6: its options are turned into
     * legacy query parameters (HistorianUtilities.toLegacyQueryParams, as
     * HistorianManagerImpl.queryDataPoints does) and run through the same legacy path as
     * queryTagHistory.
     */
    static Dataset rawPoints(QueryEngine engine, RawQueryOptions options) throws Exception {
        TagHistoryQueryParams params = HistorianUtilities.toLegacyQueryParams(options, ResultWriterOptions.builder()
                .resultFormat(ResultWriterOptions.ResultFormat.WIDE)
                .objectType(ResultWriterOptions.ObjectType.DATASET).build());
        List<ColumnQueryDefinition> defs = new ArrayList<>();
        for (int i = 0; i < params.getPaths().size(); i++) {
            QualifiedPath path = (QualifiedPath) params.getPaths().get(i);
            List<String> aliases = params.getAliases();
            defs.add(new ColumnQueryDefinition(path, aliases != null && i < aliases.size() ? aliases.get(i)
                    : path.getLastPathComponent()));
        }
        return run(engine, params, defs);
    }

    private static Dataset run(QueryEngine engine, TagHistoryQueryParams params, List<ColumnQueryDefinition> defs)
            throws Exception {
        Dataset ds = new Dataset();
        HistoryWriter writer = new HistoryWriter("test", ds, params);
        HistoryQueryExecutor executor = new HistorianQueryInterface(historian(engine)).createQuery(defs, writer);
        writer.setQueryExecutor(executor);
        writer.execute();
        return ds;
    }

    @SuppressWarnings("unchecked")
    private static Historian<FactryHistorianSettings> historian(QueryEngine engine) {
        return (Historian<FactryHistorianSettings>) Proxy.newProxyInstance(IgnitionHistoryQuery.class.getClassLoader(),
                new Class<?>[]{Historian.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "getQueryEngine" -> Optional.of(engine);
                    case "getName" -> "Factry";
                    case "getPathAdapter" -> QualifiedPathAdapter.DEFAULT;
                    default -> null;
                });
    }
}
