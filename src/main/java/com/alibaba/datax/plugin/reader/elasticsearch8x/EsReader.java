package com.alibaba.datax.plugin.reader.elasticsearch8x;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.exception.ExceptionTracker;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.elasticsearch8x.gson.MapTypeAdapter;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.searchbox.params.SearchType;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.TimeUnit;

import ognl.Ognl;
import ognl.OgnlException;

/**
 * @author yasy2025
 * @date 2024 12-16 16:32
 */
@SuppressWarnings(value = {"unchecked"})
public class EsReader extends Reader {

    public static class Job extends Reader.Job {
        private static final Logger log = LoggerFactory.getLogger(Job.class);
        private Configuration conf = null;

        @Override
        public void prepare() {
            /*
             * 注意：此方法仅执行一次。
             * 最佳实践：如果 Job 中有需要进行数据同步之前的处理，可以在此处完成，如果没有必要则可以直接去掉。
             */
            ESClient esClient = new ESClient();
            esClient.createClient(Key.getEndpoints(conf),
                    Key.getAccessID(conf),
                    Key.getAccessKey(conf));

            String indexName = Key.getIndexName(conf);
            String typeName = Key.getTypeName(conf);
            log.info("index:[{}], type:[{}]", indexName, typeName);
            try {
                boolean isIndicesExists = esClient.indicesExists(indexName);
                if (!isIndicesExists) {
                    throw new IOException(String.format("index[%s] not exist", indexName));
                }
            } catch (Exception ex) {
                throw DataXException.asDataXException(ESReaderErrorCode.ES_INDEX_NOT_EXISTS, ex.toString());
            }
            esClient.closeRestHighLevelClient();
        }

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> configurations = new ArrayList<>();
            List<Object> search = conf.getList(Key.SEARCH_KEY, Object.class);
            for (Object query : search) {
                Configuration clone = conf.clone();
                clone.set(Key.SEARCH_KEY, query);
                configurations.add(clone);
            }
            return configurations;
        }

        @Override
        public void post() {
            super.post();
        }

        @Override
        public void destroy() {
            log.info("============elasticsearch reader job destroy=================");
        }
    }

    public static class Task extends Reader.Task {
        private static final Logger log = LoggerFactory.getLogger(Job.class);

        private Configuration conf;
        ESClient esClient = null;
        Gson gson = null;
        private String index;
        private String type;
        private SearchType searchType;
        private Map<String, Object> headers;
        private String query;
        private String scroll;
        private EsTable table;
        private String[] includes;
        private String[] excludes;
        private int size;
        private boolean containsId;
        private long timeout;

        @Override
        public void prepare() {
            esClient.createClient(Key.getEndpoints(conf),
                    Key.getAccessID(conf),
                    Key.getAccessKey(conf));
        }

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
            this.esClient = new ESClient();
            this.gson = new GsonBuilder().registerTypeAdapterFactory(MapTypeAdapter.FACTORY).create();
            this.index = Key.getIndexName(conf);
            this.type = Key.getTypeName(conf);
            this.searchType = Key.getSearchType(conf);
            this.headers = Key.getHeaders(conf);
            this.query = Key.getQuery(conf);
            this.scroll = Key.getScroll(conf);
            this.includes = Key.getIncludes(conf);
            this.excludes = Key.getExcludes(conf);
            this.size = Key.getSize(conf);
            this.containsId = Key.getContainsId(conf);
            this.timeout = Key.getTimeout(conf);
            this.table = Key.getTable(conf);

            if (table == null || table.getColumn() == null || table.getColumn().isEmpty()) {
                throw DataXException.asDataXException(ESReaderErrorCode.COLUMN_CANT_BE_EMPTY, "请检查job的elasticsearchreader插件下parameter是否配置了table参数");
            }
        }

        @Override
        public void startRead(RecordSender recordSender) {
            PerfTrace.getInstance().addTaskDetails(super.getTaskId(), index);
            //search
            PerfRecord queryPerfRecord = new PerfRecord(super.getTaskGroupId(), super.getTaskId(), PerfRecord.PHASE.SQL_QUERY);
            SearchRequest searchRequest = new SearchRequest(index);
            SearchSourceBuilder sourceBuilder = jsonToSearchSourceBuilder(query);
            sourceBuilder.trackTotalHits(true);
            sourceBuilder.fetchSource(includes, excludes);
            sourceBuilder.size(size);
            sourceBuilder.timeout(new TimeValue(timeout, TimeUnit.MILLISECONDS));
            // 使用searchAfter需要指定排序规则
            searchRequest.searchType(searchType.toString());
            searchRequest.source(sourceBuilder);

            SearchResponse searchResponse;

            queryPerfRecord.start();
            Object[] sortValues = null;
            try {
                searchResponse = esClient.getClient().search(searchRequest, RequestOptions.DEFAULT);
                TotalHits totalHits = searchResponse.getHits().getTotalHits();
                int total = (int) totalHits.value;
                log.info("search total：{}, size: {} ", total, sourceBuilder.size());
                if (total == 0) {
                    return;
                }
                SearchHit[] searchHits = searchResponse.getHits().getHits();

                queryPerfRecord.start();
                this.transportRecords(recordSender, searchHits);
                queryPerfRecord.end();

                if (total <= sourceBuilder.size()) {
                    return;
                }

                SearchHit last = searchHits[searchHits.length - 1];
                sortValues = last.getSortValues();
                log.info("??????????searchAfter is：{} ", Arrays.toString(sortValues));
            } catch (Exception e) {
                throw DataXException.asDataXException(ESReaderErrorCode.ES_SEARCH_ERROR, e);
            }
            queryPerfRecord.end();

            try {
                while (true) {
                    // 使用searchAfter循环
                    queryPerfRecord.start();
                    log.info("searchAfter is：{} ", Arrays.toString(sortValues));
                    sourceBuilder.searchAfter(sortValues);
                    searchResponse = esClient.getClient().search(searchRequest, RequestOptions.DEFAULT);
                    SearchHit[] searchHits = searchResponse.getHits().getHits();
                    queryPerfRecord.end();

                    queryPerfRecord.start();
                    this.transportRecords(recordSender, searchHits);
                    queryPerfRecord.end();

                    if (searchHits.length == 0) {
                        break;
                    }
                    // 重新复值searchAfter
                    SearchHit last = searchHits[searchHits.length - 1];
                    sortValues = last.getSortValues();
                }
            } catch (IOException e) {
                throw DataXException.asDataXException(ESReaderErrorCode.ES_SEARCH_ERROR, e);
            }
        }

        private void setDefaultValue(List<EsField> column, Map<String, Object> data) {
            for (EsField field : column) {
                if (field.hasChild()) {
                    setDefaultValue(field.getChild(), data);
                } else {
                    data.putIfAbsent(field.getFinalName(table.getNameCase()), null);
                }
            }
        }


        private SearchSourceBuilder jsonToSearchSourceBuilder(String query) {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            if (StringUtils.isNotBlank(query)) {
                log.info("search condition is : {} ", query);
                SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
                try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(new NamedXContentRegistry(searchModule.getNamedXContents()), null, query)) {
                    searchSourceBuilder.parseXContent(parser);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return searchSourceBuilder;
        }

        private void getPathSource(List<Map<String, Object>> result, Map<String, Object> source, List<EsField> column, Map<String, Object> parent) {
            if (source.isEmpty()) {
                return;
            }
            for (EsField esField : column) {
                if (!esField.hasChild()) {
                    parent.put(esField.getFinalName(table.getNameCase()), source.getOrDefault(esField.getName(), esField.getValue()));
                }
            }
            for (EsField esField : column) {
                if (!esField.hasChild()) {
                    continue;
                }
                Object value = source.get(esField.getName());
                if (value instanceof Map) {
                    getPathSource(result, (Map<String, Object>) value, esField.getChild(), parent);
                } else if (value instanceof List) {
                    List<Map<String, Object>> valueList = (List<Map<String, Object>>) value;
                    if (valueList.isEmpty()) {
                        continue;
                    }
                    List<Map<String, Object>> joinResults = new ArrayList<>();
                    ArrayList<Map<String, Object>> copyResult = new ArrayList<>(result);
                    result.clear();
                    for (Map<String, Object> joinParent : copyResult) {
                        for (Map<String, Object> item : valueList) {
                            HashMap<String, Object> childData = new LinkedHashMap<>(joinParent);
                            joinResults.add(childData);
                            getPathSource(joinResults, item, esField.getChild(), childData);
                            result.addAll(joinResults);
                            joinResults.clear();
                        }
                    }
                    copyResult.clear();
                }
            }
        }

        private Object getOgnlValue(Object expression, Map<String, Object> root, Object defaultValue) {
            try {
                if (!(expression instanceof String)) {
                    return defaultValue;
                }
                Object value = Ognl.getValue(expression.toString(), root);
                if (value == null) {
                    return defaultValue;
                }
                return value;
            } catch (OgnlException e) {
                return defaultValue;
            }
        }

        private boolean filter(String filter, String deleteFilterKey, Map<String, Object> record) {
            if (StringUtils.isNotBlank(deleteFilterKey)) {
                record.remove(deleteFilterKey);
            }
            if (StringUtils.isBlank(filter)) {
                return true;
            }
            return (Boolean) getOgnlValue(filter, record, Boolean.TRUE);
        }

        private boolean transportRecords(RecordSender recordSender, SearchHit[] searchHits) {
            if (searchHits == null && searchHits.length == 0) {
                return false;
            }
            List<String> sources = Lists.newArrayList();
            for (SearchHit hit : searchHits) {
                sources.add(hit.getSourceAsString());
            }
//            Map<String, Object> recordMap = new LinkedHashMap<>();
            List<Map<String, Object>> recordMaps = new ArrayList<>();

            for (SearchHit hit : searchHits) {
                List<EsField> column = table.getColumn();
//                for (int i = 0; i < column.size(); i++) {
//                    System.out.println(column.get(i).getName());
//                }
                if (column == null || column.isEmpty()) {
                    continue;
                }

//                if (containsId) {
//                    recordMap.put("_id", hit.getId());
//                }
//                Map<String, Object> parent = JSON.parseObject(hit.getSourceAsString(), LinkedHashMap.class);

                Map<String, Object> parent = new LinkedHashMap<>((int) (column.size() * 1.5));
                setDefaultValue(table.getColumn(), parent);

//                System.out.println(hit.getSourceAsString());

                setDefaultValue(table.getColumn(), parent);
//                System.out.println(parent.keySet());
//                System.out.println(parent.values());

//                recordMaps.putAll(parent);
                recordMaps.add(parent);
//                this.transportOneRecord(recordSender, recordMap);
                getPathSource(recordMaps, gson.fromJson(hit.getSourceAsString(), Map.class), column, parent);
//                System.out.println(parent.values());

                this.transportOneRecord(table, recordSender, recordMaps);
                recordMaps.clear();
            }
            return sources.size() > 0;
        }

//        private void transportOneRecord(RecordSender recordSender, Map<String, Object> recordMap) {
        private void transportOneRecord(EsTable table, RecordSender recordSender, List<Map<String, Object>> recordMaps) {
            for (Map<String, Object> o : recordMaps) {
                boolean allow = filter(table.getFilter(), table.getDeleteFilterKey(), o);
                if (allow && o.entrySet().stream().anyMatch(x -> x.getValue() != null)) {
                    Record record = buildRecord(recordSender, o);
                    /*
                    这里的buildrecord是核心问题
                     */
//                    System.out.println(record);
                    recordSender.sendToWriter(record);
                }
            }
        }

        private Record buildRecord(RecordSender recordSender, Map<String, Object> source) {
            Record record = recordSender.createRecord();
            boolean hasDirty = false;
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Object> entry : source.entrySet()) {
                try {
                    Object o = source.get(entry.getKey());
                    record.addColumn(getColumn(o));
                } catch (Exception e) {
                    hasDirty = true;
                    sb.append(ExceptionTracker.trace(e));
                }
            }
            if (hasDirty) {
                getTaskPluginCollector().collectDirtyRecord(record, sb.toString());
            }
            return record;
        }

        private Column getColumn(Object value) {
            Column col;
            if (value == null) {
                col = new StringColumn();
            } else if (value instanceof String) {
                col = new StringColumn((String) value);
            } else if (value instanceof Integer) {
                col = new LongColumn(((Integer) value).longValue());
            } else if (value instanceof Long) {
                col = new LongColumn((Long) value);
            } else if (value instanceof Byte) {
                col = new LongColumn(((Byte) value).longValue());
            } else if (value instanceof Short) {
                col = new LongColumn(((Short) value).longValue());
            } else if (value instanceof Double) {
                col = new DoubleColumn(BigDecimal.valueOf((Double) value));
            } else if (value instanceof Float) {
                col = new DoubleColumn(BigDecimal.valueOf(((Float) value).doubleValue()));
            } else if (value instanceof Date) {
                col = new DateColumn((Date) value);
            } else if (value instanceof Boolean) {
                col = new BoolColumn((Boolean) value);
            } else if (value instanceof byte[]) {
                col = new BytesColumn((byte[]) value);
            } else if (value instanceof List) {
                col = new StringColumn(JSON.toJSONString(value));
            } else if (value instanceof Map) {
                col = new StringColumn(JSON.toJSONString(value));
            } else if (value instanceof Array) {
                col = new StringColumn(JSON.toJSONString(value));
            } else {
                throw DataXException.asDataXException(ESReaderErrorCode.UNKNOWN_DATA_TYPE, "type:" + value.getClass().getName());
            }
            return col;
        }

        @Override
        public void post() {
            super.post();
        }

        @Override
        public void destroy() {
            log.info("============elasticsearch reader taskGroup[{}] taskId[{}] destroy=================", super.getTaskGroupId(), super.getTaskId());
            esClient.closeRestHighLevelClient();
        }
    }
}
