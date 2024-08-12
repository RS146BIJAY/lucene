package org.apache.lucene.index;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SoftDeletesMultiDirectoryReaderWrapper extends FilterMultiDirectoryReader {
    private final String field;
    private final CacheHelper readerCacheHelper;

    public SoftDeletesMultiDirectoryReaderWrapper(MultiDirectoryReader in, String field) throws IOException {
        this(in, new SoftDeletesSubMultiReaderWrapper(Collections.emptyMap(), field));
    }

    private SoftDeletesMultiDirectoryReaderWrapper(MultiDirectoryReader in, SoftDeletesSubMultiReaderWrapper wrapper) throws IOException {
        super(in, wrapper);
        this.field = wrapper.field;
        readerCacheHelper =
                in.getReaderCacheHelper() == null
                        ? null
                        : new FilterMultiDirectoryReader.DelegatingCacheHelper(in.getReaderCacheHelper());
    }

    @Override
    protected MultiDirectoryReader doWrapDirectoryReader(MultiDirectoryReader in) throws IOException {
        Map<CacheKey, DirectoryReader> readerCache = new HashMap<>();
        for (DirectoryReader reader: getSequentialSubReaders()) {
            if (reader instanceof SoftDeletesDirectoryReaderWrapper && reader.getReaderCacheHelper() != null) {
                readerCache.put(reader.getReaderCacheHelper().getKey(), reader);
            }
        }

        return new SoftDeletesMultiDirectoryReaderWrapper(
                in, new SoftDeletesSubMultiReaderWrapper(readerCache, field));
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return readerCacheHelper;
    }

    private static class SoftDeletesSubMultiReaderWrapper extends SubMultiReaderWrapper {
        private final Map<CacheKey, DirectoryReader> mapping;
        private final String field;

        public SoftDeletesSubMultiReaderWrapper(Map<CacheKey, DirectoryReader> oldReadersCache, String field) {
            Objects.requireNonNull(field, "Field must not be null");
            assert oldReadersCache != null;
            this.mapping = oldReadersCache;
            this.field = field;
        }

        @Override
        protected DirectoryReader[] wrap(List<? extends DirectoryReader> readers) {
            List<DirectoryReader> wrapped = new ArrayList<>(readers.size());
            for (DirectoryReader reader : readers) {
                DirectoryReader wrap = wrap(reader);
                assert wrap != null;
                if (wrap.numDocs() != 0) {
                    wrapped.add(wrap);
                }
            }
            return wrapped.toArray(new DirectoryReader[0]);
        }

        @Override
        public DirectoryReader wrap(DirectoryReader reader) {
            CacheHelper readerCacheHelper = reader.getReaderCacheHelper();
            if (readerCacheHelper != null && mapping.containsKey(readerCacheHelper.getKey())) {
                // if the reader cache helper didn't change and we have it in the cache don't bother
                // creating a new one
                return mapping.get(readerCacheHelper.getKey());
            }
            try {
                return new SoftDeletesDirectoryReaderWrapper(reader, field);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
