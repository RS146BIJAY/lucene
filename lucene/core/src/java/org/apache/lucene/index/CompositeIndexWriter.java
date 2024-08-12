/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.index;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.*;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Version;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.lucene.index.IndexWriter.WRITE_LOCK_NAME;

/**
 * If there are batch document deletes or batch document updates or delete by query, sequence number must be updated once??
 *
 */
public class CompositeIndexWriter implements Closeable, Accountable {

    private final Map<String, IndexWriter> criteriaIndexWriterMapping;
    private final List<IndexWriterConfig> configList;
    private final CriteriaBasedCompositeDirectory multitenantDirectory;


    public CompositeIndexWriter(CriteriaBasedCompositeDirectory multitenantDirectory, List<IndexWriterConfig> iwcList) throws IOException {
        this.multitenantDirectory = multitenantDirectory;
        criteriaIndexWriterMapping = new HashMap<>();
        int i = 0;
        for (String criteria : multitenantDirectory.getCriteriaList()) {
            Directory dir = multitenantDirectory.getDirectory(criteria);
            IndexWriter iw = new IndexWriter(dir, iwcList.get(i));
            criteriaIndexWriterMapping.put(criteria, iw);
            ++i;
        }

        configList = iwcList;
    }

    public List<IndexWriter> getIndexWriters() {
        return new ArrayList<>(criteriaIndexWriterMapping.values());
    }

    public IndexWriter getIndexWriterByCriteria(String criteria) {
        return criteriaIndexWriterMapping.get(criteria);
    }

    // Should we close all IndexWriter when any IndexWriter are closed?
    @Override
    public void close() throws IOException {
        SegmentInfos compositeInfos = new SegmentInfos(configList.get(0).createdVersionMajor);
        for (Map.Entry<String, IndexWriter> indexWriterEntry: criteriaIndexWriterMapping.entrySet()) {
            IndexWriter indexWriter = indexWriterEntry.getValue();
//            String criteria = indexWriterEntry.getKey();
            indexWriter.close();
//            SegmentInfos childSegmentInfos = indexWriter.getSegmentInfos();
//            for (int i = 0; i < indexWriter.getSegmentCount(); i++) {
//                SegmentCommitInfo currentCommitInfo = childSegmentInfos.info(i);
//                SegmentInfo currentInfo = currentCommitInfo.info;
//                compositeInfos.add(new SegmentCommitInfo(new SegmentInfo(currentInfo.dir, currentInfo.getVersion(),
//                        currentInfo.getMinVersion(), criteria + "_" +currentInfo.name, currentInfo.maxDoc(),
//                        currentInfo.getUseCompoundFile(), currentInfo.getHasBlocks(), currentInfo.getCodec(),
//                        currentInfo.getDiagnostics(), currentInfo.getId(), currentInfo.getAttributes(),
//                        currentInfo.getIndexSort()), currentCommitInfo.getDelCount(), currentCommitInfo.getSoftDelCount(),
//                        currentCommitInfo.getDelGen(), currentCommitInfo.getFieldInfosGen(),
//                        currentCommitInfo.getDocValuesGen(), currentCommitInfo.getId()));
//            }
        }
//
//        compositeInfos.commit(multitenantDirectory);
    }

    /**
     * Returns the commit user data iterable previously set with {@link #setLiveCommitData(Iterable)},
     * or null if nothing has been set yet.
     */
    public final synchronized Iterable<Map.Entry<String, String>> getLiveCommitData() {
        final Map<String, String> userData = new HashMap<>();
        for (IndexWriter writer: criteriaIndexWriterMapping.values()) {
            writer.getLiveCommitData().forEach(e -> userData.put(e.getKey(), e.getValue()));
        }

        return userData.entrySet();
    }

    public synchronized void deleteUnusedFiles() throws IOException {
        criteriaIndexWriterMapping.values().forEach(writer -> {
            try {
                writer.deleteUnusedFiles();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public final long getFlushingBytes() {
        long flushingBytes = 0L;
        for (IndexWriter writer: criteriaIndexWriterMapping.values()) {
            flushingBytes += writer.getFlushingBytes();
        }

        return flushingBytes;
    }

    public synchronized void rollback() throws IOException {
        criteriaIndexWriterMapping.values().forEach(writer -> {
            try {
                writer.rollback();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public long ramBytesUsed() {
        long ramBytes = 0L;
        for (IndexWriter writer: criteriaIndexWriterMapping.values()) {
            ramBytes += writer.getFlushingBytes();
        }

        return ramBytes;
    }

    // Need to check if we need to iterate all index writer for tragic exceptions or iterate only on those which is used in indexing.
    public Throwable getTragicException() {
        for (IndexWriter writer: criteriaIndexWriterMapping.values()) {
            if (writer.getTragicException() != null) {
                return writer.getTragicException();
            }
        }

        return null;
    }

    public long getPendingNumDocs() {
        long numDocs = 0L;
        for (IndexWriter writer: criteriaIndexWriterMapping.values()) {
            numDocs += writer.getPendingNumDocs();
        }

        return numDocs;
    }

    // Most use case is whether IndexWriter is closed or not.
    public boolean isOpen() {
        for (IndexWriter writer: criteriaIndexWriterMapping.values()) {
            if (writer.isOpen()) {
                return true;
            }
        }

        return false;
    }

    // Commit files at global file.
    public final long commit() throws IOException {
        long commitCount = 0L;

        for (IndexWriter writer: criteriaIndexWriterMapping.values()) {
            commitCount += Math.max(0, writer.commit());
        }

        if (commitCount == 0) {
            return -1;
        }

        return commitCount;
    }

    public final boolean hasUncommittedChanges() {
        for (IndexWriter writer: criteriaIndexWriterMapping.values()) {
            if (writer.hasUncommittedChanges()) {
                return true;
            }
        }

        return false;
    }

    public final void maybeMerge() throws IOException {
        for (IndexWriter writer: criteriaIndexWriterMapping.values()) {
            writer.maybeMerge();
        }
    }

    public void forceMerge(int maxNumSegments) throws IOException {
        for (IndexWriter writer: criteriaIndexWriterMapping.values()) {
            writer.forceMerge(maxNumSegments);
        }
    }

    public void forceMerge(int maxNumSegments, boolean doWait) throws IOException {
        for (IndexWriter writer: criteriaIndexWriterMapping.values()) {
            writer.forceMerge(maxNumSegments, doWait);
        }
    }

    public void forceMergeDeletes(boolean doWait) throws IOException {
        for (IndexWriter writer: criteriaIndexWriterMapping.values()) {
            writer.forceMergeDeletes(doWait);
        }
    }

    public List<IndexWriterConfig> getConfig() {
        return configList;
    }

    public boolean hasPendingMerges() {
        for (IndexWriter writer: criteriaIndexWriterMapping.values()) {
            if (writer.hasPendingMerges()) {
                return true;
            }
        }

        return false;
    }

    // Validate if setting live commit data for individual writer will cause any issue.
    public final synchronized void setLiveCommitData(
            Iterable<Map.Entry<String, String>> commitUserData) {
        for (IndexWriter writer: criteriaIndexWriterMapping.values()) {
            writer.setLiveCommitData(commitUserData);
        }
    }

    public long addDocument(Iterable<? extends IndexableField> doc) throws IOException {
        return getIndexWriterByCriteria(getGroupingCriteriaForDoc(List.of(doc))).addDocument(doc);
    }

    public long addDocuments(Iterable<? extends Iterable<? extends IndexableField>> docs)
            throws IOException {
        return getIndexWriterByCriteria(getGroupingCriteriaForDoc(docs)).addDocuments(docs);
    }

    public long softUpdateDocument(
            Term term, Iterable<? extends IndexableField> doc, Field... softDeletes) throws IOException {
        return getIndexWriterByCriteria(getGroupingCriteriaForDoc(List.of(doc))).softUpdateDocument(term, doc, softDeletes);
    }

    public long softUpdateDocuments(
            Term term, Iterable<? extends Iterable<? extends IndexableField>> docs, Field... softDeletes)
            throws IOException {
        return getIndexWriterByCriteria(getGroupingCriteriaForDoc(docs)).softUpdateDocuments(term, docs, softDeletes);
    }

    private String getGroupingCriteriaForDoc(final Iterable<? extends Iterable<? extends IndexableField>> docs) {
        Iterator<? extends IndexableField> docIt = docs.iterator().next().iterator();
        while (docIt.hasNext()) {
            IndexableField field = docIt.next();
            if (field.stringValue() != null && field.name().equals("status")) {
                return field.stringValue();
            }
        }

        return "0";
    }

    public synchronized IndexWriter.DocStats getDocStats() {
        int maxDoc = 0, numDocs = 0;
        for (IndexWriter writer: criteriaIndexWriterMapping.values()) {
            IndexWriter.DocStats docStats = writer.getDocStats();
            System.out.println("MaxDoc: " + maxDoc + " NumDocs: " + numDocs);
            maxDoc += docStats.maxDoc;
            numDocs += docStats.numDocs;
        }

        return new IndexWriter.DocStats(maxDoc, numDocs);
    }

    public final void flush() throws IOException {
        for (IndexWriter writer: criteriaIndexWriterMapping.values()) {
            writer.flush();
        }
    }

    final void flush(boolean triggerMerge, boolean applyAllDeletes) throws IOException {
        for (IndexWriter writer: criteriaIndexWriterMapping.values()) {
            writer.flush(triggerMerge, applyAllDeletes);
        }
    }

    final synchronized int getSegmentCount() {
        int segmentCount = 0;
        for (IndexWriter writer: criteriaIndexWriterMapping.values()) {
            segmentCount += writer.getSegmentCount();
        }

        return segmentCount;
    }

    final int getFlushCount() {
        int flushCount = 0;
        for (IndexWriter writer: criteriaIndexWriterMapping.values()) {
            flushCount += writer.getFlushCount();
        }

        return flushCount;
    }

    protected boolean isEnableTestPoints() {
        return false;
    }

    public long deleteDocuments(Query... queries) throws IOException {
        long seqNoMax = 0;
        for (IndexWriter writer: criteriaIndexWriterMapping.values()) {
            seqNoMax = Math.max(seqNoMax, writer.deleteDocuments(queries));
        }

        return seqNoMax;
    }

    public long deleteDocuments(Term... terms) throws IOException {
        long seqNoMax = 0;
        for (IndexWriter writer: criteriaIndexWriterMapping.values()) {
            seqNoMax = Math.max(seqNoMax, writer.deleteDocuments(terms));
        }

        return seqNoMax;
    }

    protected void doAfterFlush() throws IOException {
        for (IndexWriter writer: criteriaIndexWriterMapping.values()) {
            writer.doAfterFlush();
        }
    }

    protected void doBeforeFlush() throws IOException {
        for (IndexWriter writer: criteriaIndexWriterMapping.values()) {
            writer.doBeforeFlush();
        }
    }

    // TODO: Merge this
    public long addIndexes(CodecReader... readers) throws IOException {
        long seqNumber = 0;
        for (IndexWriter writer: criteriaIndexWriterMapping.values()) {
            seqNumber = writer.addIndexes(readers);
        }

        return seqNumber;
    }

    public MultiDirectoryReader getReader(boolean applyAllDeletes, boolean writeAllDeletes) throws IOException {
        List<DirectoryReader> readers = new ArrayList<>();
        List<SegmentInfos> infos = new ArrayList<>();
        for (IndexWriter writer: criteriaIndexWriterMapping.values()) {
            readers.add(writer.getReader(applyAllDeletes, writeAllDeletes));
            infos.add(writer.getSegmentInfos());
        }

        return new MultiDirectoryReader(multitenantDirectory, readers.toArray(new DirectoryReader[0]),
                true, this, infos, writeAllDeletes);
    }

    public List<SegmentInfos> getSegmentInfosList() {
        final List<SegmentInfos> segmentInfosList = new ArrayList<>();
        for (IndexWriter writer: criteriaIndexWriterMapping.values()) {
            segmentInfosList.add(writer.getSegmentInfos());
        }

        return segmentInfosList;
    }
}

