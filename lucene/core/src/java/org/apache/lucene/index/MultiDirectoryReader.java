package org.apache.lucene.index;

import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

// TODO: How to handle scenario when few reader is null
public class MultiDirectoryReader extends BaseCompositeReader<DirectoryReader> {

    private final boolean closeSubReaders;
    private final List<DirectoryReader> subReaders;
    /** The index directory. */
    private final CriteriaBasedCompositeDirectory directory;
    private final CompositeIndexWriter compositeIndexWriter;
    private final boolean writeAllDeletes;
    private final List<SegmentInfos> segmentInfosList;

    /**
     * ?? Do we need directory sorter as well?
     * Constructs a {@code BaseCompositeReader} on the given subReaders.
     *
     * @param subReaders       the wrapped sub-readers. This array is returned by {@link
     *                         #getSequentialSubReaders} and used to resolve the correct subreader for docID-based
     *                         methods. <b>Please note:</b> This array is <b>not</b> cloned and not protected for
     *                         modification, the subclass is responsible to do this.
     */
    protected MultiDirectoryReader(CriteriaBasedCompositeDirectory multitenantDirectory,
                                   DirectoryReader[] subReaders, boolean closeSubReaders, CompositeIndexWriter writer,
                                   final List<SegmentInfos> sisList, boolean writeAllDeletes) throws IOException {
        super(subReaders, null);
        this.closeSubReaders = closeSubReaders;
        this.subReaders = Collections.unmodifiableList(Arrays.asList(subReaders));
        this.directory = multitenantDirectory;
        this.compositeIndexWriter = writer;
        this.segmentInfosList = sisList;
        this.writeAllDeletes = writeAllDeletes;
    }

    public static MultiDirectoryReader open(final CompositeIndexWriter writer) throws IOException {
        return open(writer, true, false);
    }

    public static MultiDirectoryReader open(
            final CompositeIndexWriter writer, boolean applyAllDeletes, boolean writeAllDeletes)
            throws IOException {
        return writer.getReader(applyAllDeletes, writeAllDeletes);
    }

    public static MultiDirectoryReader open(CriteriaBasedCompositeDirectory multitenantDirectory,
                                            List<SegmentInfos> infos, List<? extends DirectoryReader> oldReaders,
                                            Comparator<LeafReader> subReadersSorter) throws IOException {
        DirectoryReader[] subReaders = createSubReaders(multitenantDirectory, infos, oldReaders, subReadersSorter);
        return new MultiDirectoryReader(multitenantDirectory, subReaders, true, null, infos,false);
    }

    public static MultiDirectoryReader open(CriteriaBasedCompositeDirectory multitenantDirectory, List<IndexCommit> commits,
                                            boolean isExtendedCompatibility, int minimumSupportedVersion) throws IOException {
        final List<DirectoryReader> readerList = new ArrayList<>();
        final List<SegmentInfos> segmentInfosList = new ArrayList<>();
        for (IndexCommit commit : commits) {
            DirectoryReader reader;
            if (isExtendedCompatibility) {
                reader = DirectoryReader.open(commit, minimumSupportedVersion, null);
            } else {
                reader = DirectoryReader.open(commit);
            }

            readerList.add(reader);
            segmentInfosList.add(((StandardDirectoryReader) reader).getSegmentInfos());
        }

        return new MultiDirectoryReader(multitenantDirectory, readerList.toArray(new DirectoryReader[0]),
                true, null, segmentInfosList, false);
    }

    public static MultiDirectoryReader openIfChanged(MultiDirectoryReader oldReader) throws IOException {
        final MultiDirectoryReader newReader = oldReader.doOpenIfChanged();
        assert newReader != oldReader;
        return newReader;
    }

    private static DirectoryReader[] createSubReaders(CriteriaBasedCompositeDirectory multitenantDirectory,
                                                      List<SegmentInfos> infosList, List<? extends DirectoryReader> oldReaders,
                                                      Comparator<LeafReader> subReadersSorter) throws IOException {
        Directory[] directoryList = multitenantDirectory.listSubDirectories();
        assert directoryList.length == oldReaders.size();
        assert directoryList.length == infosList.size();
        List<DirectoryReader> subReaders = new ArrayList<>(oldReaders.size());

        for (int i = 0; i < directoryList.length; i++) {
            Directory directory = directoryList[i];
            SegmentInfos segmentInfos = infosList.get(i);
            DirectoryReader oldReader = oldReaders.get(i);
            final List<LeafReader> subs = new ArrayList<>();
            for (LeafReaderContext ctx : oldReader.leaves()) {
                subs.add(ctx.reader());
            }
            subReaders.add(StandardDirectoryReader.open(directory, segmentInfos, subs, subReadersSorter));
        }

        return subReaders.toArray(new DirectoryReader[0]);
    }

    @Override
    protected void doClose() throws IOException {
        IOException ioe = null;
        for (final IndexReader r : getSequentialSubReaders()) {
            try {
                if (closeSubReaders) {
                    r.close();
                } else {
                    r.decRef();
                }
            } catch (IOException e) {
                if (ioe == null) ioe = e;
            }
        }
        // throw the first exception
        if (ioe != null) throw ioe;
    }

    protected MultiDirectoryReader doOpenIfChanged() throws IOException {
        return doOpenIfChanged( null);
    }

    // TODO Handle the scenario when writer is null.
    protected MultiDirectoryReader doOpenIfChanged(List<IndexCommit> commits) throws IOException {
        List<Directory> directoryList = Arrays.asList(directory.listSubDirectories());
        assert directoryList.size() == commits.size();
        ensureOpen();
        return doOpenFromWriter(commits);

        // TODO If we were obtained by writer.getReader(), re-ask the
        // writer to get a new reader.
//        if (compositeIndexWriter != null) {
//
//        } else {
//            return doOpenNoWriter(commits);
//        }
    }

    // TODO Handle scenario on how to create reader when individual commit is null (See StandardDirectoryReader).
    private MultiDirectoryReader doOpenFromWriter(List<IndexCommit> commits) throws IOException {
        final List<DirectoryReader> newSubReaders = new ArrayList<>();
        final List<SegmentInfos> segmentInfosList = new ArrayList<>();
        if (commits != null) {
            assert subReaders.size() == commits.size();
            for (int i = 0; i < subReaders.size(); i++) {
                final DirectoryReader newSubReader = DirectoryReader.openIfChanged(subReaders.get(i), commits.get(i));
                segmentInfosList.add(((StandardDirectoryReader)newSubReader).getSegmentInfos());
                newSubReaders.add(newSubReader);
            }

            return new MultiDirectoryReader(directory, newSubReaders.toArray(new DirectoryReader[0]), true,
                    compositeIndexWriter, segmentInfosList, writeAllDeletes);
        } else {
            // TODO
//            if (writer.nrtIsCurrent(segmentInfos)) {
//                return null;
//            }

            if (isNrtCurrent()) {
                return null;
            }

            for (IndexWriter writer: compositeIndexWriter.getIndexWriters()) {
                DirectoryReader reader = writer.getReader(true, writeAllDeletes);
                newSubReaders.add(reader);
            }

//            if (reader.getVersion() == segmentInfos.getVersion()) {
//                reader.decRef();
//                return null;
//            }

            if (isVersionSame(newSubReaders)) {
                return null;
            }

            return new MultiDirectoryReader(directory, newSubReaders.toArray(new DirectoryReader[0]), true,
                    compositeIndexWriter, compositeIndexWriter.getSegmentInfosList(), writeAllDeletes);
        }

    }

    private boolean isVersionSame(List<DirectoryReader> readerList) {
        for (int i = 0; i < readerList.size(); i++) {
            DirectoryReader reader = readerList.get(i);
            SegmentInfos segmentInfos = segmentInfosList.get(i);
            if (reader.getVersion() != segmentInfos.getVersion()) {
                return false;
            }
        }

        return true;
    }

    private boolean isNrtCurrent() {
        final List<IndexWriter> writerList = compositeIndexWriter.getIndexWriters();
        assert writerList.size() == segmentInfosList.size();
        for (int i = 0; i < writerList.size(); i++) {
            if (!writerList.get(i).nrtIsCurrent(segmentInfosList.get(i))) {
                return false;
            }
        }

        return true;
    }

    protected MultiDirectoryReader doOpenIfChanged(CompositeIndexWriter writer, boolean applyAllDeletes)
            throws IOException {
        ensureOpen();
        // TODO
//        if (writer == this.writer && applyAllDeletes == this.applyAllDeletes) {
//            return doOpenFromWriter(null);
//        } else {
            return writer.getReader(applyAllDeletes, writeAllDeletes);
//        }
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        if (getSequentialSubReaders().size() == 1) {
            return getSequentialSubReaders().get(0).getReaderCacheHelper();
        }
        return null;
    }

    public CriteriaBasedCompositeDirectory directory() {
        return directory;
    }

    public CompositeIndexWriter getCompositeIndexWriter() {
        return compositeIndexWriter;
    }

    public boolean isWriteAllDeletes() {
        return writeAllDeletes;
    }

    public List<SegmentInfos> getSegmentInfosList() {
        return segmentInfosList;
    }

    public List<DirectoryReader> getSubReaders() {
        return subReaders;
    }
}
