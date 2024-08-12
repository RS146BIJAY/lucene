package org.apache.lucene.index;

import org.apache.lucene.store.*;

import java.io.IOException;
import java.util.*;

public class CriteriaBasedCompositeDirectory extends FilterDirectory {

    private final Map<String, Directory> criteriaDirectoryMapping;
    private final Directory multiTenantDirectory;

    /**
     * Sole constructor, typically called from sub-classes.
     *
     * @param in
     */
    public CriteriaBasedCompositeDirectory(Directory in, Map<String, Directory> criteriaDirectoryMapping) {
        super(in);
        this.multiTenantDirectory = in;
        this.criteriaDirectoryMapping = criteriaDirectoryMapping;
    }

    public Directory getDirectory(String criteria) {
        return criteriaDirectoryMapping.get(criteria);
    }

    public Set<String> getCriteriaList() {
        return criteriaDirectoryMapping.keySet();
    }

    @Override
    public void deleteFile(String name) throws IOException {
        String criteria = name.split("_")[0];
        getDirectory(criteria).deleteFile(name);
    }

    // Fix this.
    @Override
    public String[] listAll() throws IOException {
        List<String> filesList = new ArrayList<>();
        for (Map.Entry<String, Directory> filterDirectoryEntry: criteriaDirectoryMapping.entrySet()) {
            String prefix = filterDirectoryEntry.getKey();
            Directory filterDirectory = filterDirectoryEntry.getValue();
            for (String fileName : filterDirectory.listAll()) {
                filesList.add(prefix + "_" + fileName);
            }
        }

        return filesList.toArray(new String[0]);
    }

    // Merge this
    @Override
    public ChecksumIndexInput openChecksumInput(String name, IOContext context) throws IOException {
        String criteria = name.split("_")[0];
        return getDirectory(criteria).openChecksumInput(name, context);
    }

    // TODO: Select on the basis of filter name.
    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        String criteria = name.split("_")[0];
        return getDirectory(criteria).openInput(name, context);
    }

    // TODO: Merge this
    // TODO: Select on the basis of filter name.
    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        String criteria = name.split("_")[0];
        return getDirectory(criteria).createOutput(name, context);
    }

    // TODO: Select on the basis of filter name.
    @Override
    public long fileLength(String name) throws IOException {
        String criteria = name.split("_")[0];
        return getDirectory(criteria).fileLength(name);
    }

    @Override
    public void close() throws IOException {
        for (Directory filterDirectory: criteriaDirectoryMapping.values()) {
            filterDirectory.close();
        }

        multiTenantDirectory.close();
    }

    // Attach prefix name.
    public Directory[] listSubDirectories() {
        return this.criteriaDirectoryMapping.values().toArray(new Directory[0]);
    }
}

