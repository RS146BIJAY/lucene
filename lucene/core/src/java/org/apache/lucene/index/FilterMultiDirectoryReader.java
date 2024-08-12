package org.apache.lucene.index;

import java.io.IOException;
import java.util.List;

/**
 * A FilterDirectoryReader wraps another MultiDirectoryReader, allowing implementations to transform or
 * extend it.
 *
 * <p>Subclasses should implement doWrapDirectoryReader to return an instance of the subclass.
 *
 * <p>If the subclass wants to wrap the MultiDirectoryReader's subreaders, it should also implement a
 * SubReaderWrapper subclass, and pass an instance to its super constructor.
 */
public abstract class FilterMultiDirectoryReader extends MultiDirectoryReader {

    /** The filtered DirectoryReader */
    protected final MultiDirectoryReader in;

    public FilterMultiDirectoryReader(MultiDirectoryReader in, SubMultiReaderWrapper wrapper) throws IOException {
        super(in.directory(), wrapper.wrap(in.getSequentialSubReaders()), true,
                in.getCompositeIndexWriter(), in.getSegmentInfosList(), in.isWriteAllDeletes());
        this.in = in;
    }

    /**
     * Get the wrapped instance by <code>reader</code> as long as this reader is an instance of {@link
     * FilterDirectoryReader}.
     */
    public static MultiDirectoryReader unwrap(MultiDirectoryReader reader) {
        while (reader instanceof FilterMultiDirectoryReader) {
            reader = ((FilterMultiDirectoryReader) reader).getDelegate();
        }
        return reader;
    }

    protected abstract MultiDirectoryReader doWrapDirectoryReader(MultiDirectoryReader in) throws IOException;

    private MultiDirectoryReader wrapDirectoryReader(MultiDirectoryReader in) throws IOException {
        return in == null ? null : doWrapDirectoryReader(in);
    }

    // Handle null scenario
//    @Override
//    protected final MultiDirectoryReader doOpenIfChanged() throws IOException {
//        return wrapDirectoryReader(in.doOpenIfChanged());
//    }

    @Override
    protected final MultiDirectoryReader doOpenIfChanged(CompositeIndexWriter writer, boolean applyAllDeletes)
            throws IOException {
        return wrapDirectoryReader(in.doOpenIfChanged(writer, applyAllDeletes));
    }

    @Override
    protected final MultiDirectoryReader doOpenIfChanged(List<IndexCommit> commit) throws IOException {
        return wrapDirectoryReader(in.doOpenIfChanged(commit));
    }

//    @Override
//    public long getVersion() {
//        return in.getVersion();
//    }
//
//    @Override
//    public boolean isCurrent() throws IOException {
//        return in.isCurrent();
//    }

    /** Returns the wrapped {@link DirectoryReader}. */
    public MultiDirectoryReader getDelegate() {
        return in;
    }

    public abstract static class SubMultiReaderWrapper {

        /**
         * Wraps a list of LeafReaders
         *
         * @return an array of wrapped LeafReaders. The returned array might contain less elements
         *     compared to the given reader list if an entire reader is filtered out.
         */
        protected DirectoryReader[] wrap(List<? extends DirectoryReader> readers) {
            DirectoryReader[] wrapped = new DirectoryReader[readers.size()];
            int i = 0;
            for (DirectoryReader reader : readers) {
                DirectoryReader wrap = wrap(reader);
                assert wrap != null;
                wrapped[i++] = wrap;
            }
            return wrapped;
        }

        /** Constructor */
        public SubMultiReaderWrapper() {}

        /**
         * Wrap one of the parent DirectoryReader's subreaders
         *
         * @param reader the subreader to wrap
         * @return a wrapped/filtered LeafReader
         */
        public abstract DirectoryReader wrap(DirectoryReader reader);
    }

    /**
     * A DelegatingCacheHelper is a CacheHelper specialization for implementing long-lived caching
     * behaviour for FilterDirectoryReader subclasses. It uses a unique CacheKey for the purpose of
     * implementing the onClose listener delegation for the reader.
     */
    protected static class DelegatingCacheHelper implements CacheHelper {
        private final CacheHelper delegate;
        private final CacheKey cacheKey = new CacheKey();

        /**
         * Create a new DelegatingCacheHelper that delegates the cache onClose listener to another
         * CacheHelper, but with its own unique CacheKey.
         *
         * @param delegate the CacheHelper to delegate the close listener to
         */
        protected DelegatingCacheHelper(CacheHelper delegate) {
            this.delegate = delegate;
        }

        @Override
        public CacheKey getKey() {
            return cacheKey;
        }

        @Override
        public void addClosedListener(ClosedListener listener) {
            // here we wrap the listener and call it with our cache key
            // this is important since this key will be used to cache the reader and otherwise we won't
            // free caches etc.
            delegate.addClosedListener(unused -> listener.onClose(cacheKey));
        }
    }
}
