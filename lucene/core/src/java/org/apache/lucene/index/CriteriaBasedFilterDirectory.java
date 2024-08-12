package org.apache.lucene.index;

import org.apache.lucene.store.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

public class CriteriaBasedFilterDirectory extends FilterDirectory {
    private final String prefix;
    private final Directory multiTenantDirectory;

    /**
     * Sole constructor, typically called from sub-classes.
     *
     * @param multiTenantDirectory underlying multitenant directory common for all the groups
     */
    public CriteriaBasedFilterDirectory(Directory multiTenantDirectory, String prefix) {
        super(multiTenantDirectory);
        this.prefix = prefix;
        this.multiTenantDirectory = multiTenantDirectory;
    }

    @Override
    public String[] listAll() throws IOException {
        return Arrays.stream(multiTenantDirectory.listAll()).filter(f -> f.startsWith(prefix))
                .map(fileName -> fileName.replaceFirst("^" + prefix + "_", "")).toArray(String[]::new);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        assert name.startsWith(prefix);
        multiTenantDirectory.deleteFile(name);
    }

    @Override
    public long fileLength(String name) throws IOException {
        assert name.startsWith(prefix);
        return multiTenantDirectory.fileLength(name);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        assert name.startsWith(prefix);
        return multiTenantDirectory.createOutput(name, context);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
            throws IOException {
        assert prefix.startsWith(this.prefix);
        return multiTenantDirectory.createTempOutput(prefix, suffix, context);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        multiTenantDirectory.sync(names.stream().filter(f -> f.startsWith(prefix)).collect(Collectors.toList()));
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        assert source.startsWith(prefix);

        if (!dest.startsWith(prefix)) {
            dest = this.prefix.concat("_").concat(dest);
        }

        multiTenantDirectory.rename(source, dest);
    }

    @Override
    public Lock obtainLock(String name) throws IOException {
        if (!name.startsWith(prefix)) {
            name = prefix.concat("_").concat(name);
        }

        return multiTenantDirectory.obtainLock(name);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        assert name.startsWith(prefix);
        return multiTenantDirectory.openInput(name, context);
    }
}
