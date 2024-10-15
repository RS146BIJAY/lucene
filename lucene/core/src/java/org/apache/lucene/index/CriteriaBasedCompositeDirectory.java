package org.apache.lucene.index;

import org.apache.lucene.store.*;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

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

    // TODO: Handling references of parent IndexWriter for deleting files of child IndexWriter
    //  (As of now not removing file in parent delete call). For eg: If a dec ref is called on parent IndexWriter and
    //  there are no active references of a file by parent IndexWriter to child IndexWriter, should we delete it?
    @Override
    public void deleteFile(String name) throws IOException {
//        if (name.contains("$")) {
//            String criteria = name.split("\\$")[0];
//            System.out.println("Deleting file from directory " + getDirectory(criteria) + " with name " + name);
//            getDirectory(criteria).deleteFile(name.replace(criteria + "$", ""));
//        } else {
//            System.out.println("Deleting file from directory " + multiTenantDirectory + " with name " + name);
//            multiTenantDirectory.deleteFile(name);
//        }

        // For time being let child IndexWriter take care of deleting files inside it. Parent IndexWriter should only care
        // about deleting files within parent directory.
        if (!name.contains("$")) {
            multiTenantDirectory.deleteFile(name);
        }
    }

    // Fix this.
    @Override
    public String[] listAll() throws IOException {
//        List<String> filesList = new ArrayList<>();
//        for (Map.Entry<String, Directory> filterDirectoryEntry: criteriaDirectoryMapping.entrySet()) {
//            String prefix = filterDirectoryEntry.getKey();
//            Directory filterDirectory = filterDirectoryEntry.getValue();
//            for (String fileName : filterDirectory.listAll()) {
//                filesList.add(prefix + "_" + fileName);
//            }
//        }

        // Exclude group level folder names which is same as criteria
        Set<String> criteriaList = getCriteriaList();
        String[] filesList = Arrays.stream(multiTenantDirectory.listAll()).filter(fileName -> !criteriaList.contains(fileName))
                .toArray(String[]::new);

        System.out.println("Parent Directory " + multiTenantDirectory + " list files: " + Arrays.toString(filesList));
        return filesList;
    }

    @Override
    public ChecksumIndexInput openChecksumInput(String name, IOContext context) throws IOException {
        if (name.contains("$")) {
            String criteria = name.split("\\$")[0];
            return getDirectory(criteria).openChecksumInput(name.replace(criteria + "$", ""), context);
        } else {
            return multiTenantDirectory.openChecksumInput(name, context);
        }
    }

    // TODO: Select on the basis of filter name.
    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        if (name.contains("$")) {
            String criteria = name.split("\\$")[0];
            return getDirectory(criteria).openInput(name.replace(criteria + "$", ""), context);
        } else {
            return multiTenantDirectory.openInput(name, context);
        }
    }

    // TODO: Merge this
    // TODO: Select on the basis of filter name.
    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        if (name.contains("$")) {
            String criteria = name.split("\\$")[0];
            return getDirectory(criteria).createOutput(name.replace(criteria + "$", ""), context);
        } else {
            return multiTenantDirectory.createOutput(name, context);
        }
    }

    // TODO: Select on the basis of filter name.
    @Override
    public long fileLength(String name) throws IOException {
        if (name.contains("$")) {
            String criteria = name.split("\\$")[0];
            return getDirectory(criteria).fileLength(name.replace(criteria + "$", ""));
        } else {
            return multiTenantDirectory.fileLength(name);
        }
    }

    @Override
    public void close() throws IOException {
        for (Directory filterDirectory: criteriaDirectoryMapping.values()) {
            filterDirectory.close();
        }

        super.close();
    }

    // Attach prefix name.
    public Directory[] listSubDirectories() {
        return this.criteriaDirectoryMapping.values().toArray(new Directory[0]);
    }
}
