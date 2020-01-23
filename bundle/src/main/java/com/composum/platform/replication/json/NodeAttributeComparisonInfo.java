package com.composum.platform.replication.json;

import com.composum.platform.commons.util.ExceptionUtil;
import com.composum.sling.core.util.SlingResourceUtil;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.sling.api.resource.Resource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static javax.jcr.PropertyType.BINARY;
import static javax.jcr.PropertyType.BOOLEAN;
import static javax.jcr.PropertyType.DATE;
import static javax.jcr.PropertyType.DECIMAL;
import static javax.jcr.PropertyType.DOUBLE;
import static javax.jcr.PropertyType.LONG;
import static javax.jcr.PropertyType.NAME;
import static javax.jcr.PropertyType.PATH;
import static javax.jcr.PropertyType.REFERENCE;
import static javax.jcr.PropertyType.STRING;
import static javax.jcr.PropertyType.URI;
import static javax.jcr.PropertyType.WEAKREFERENCE;

/**
 * JSON-(de-)serializable description of a nodes attributes, meant for the comparison of the attributes of parent
 * nodes (not for transmitting the attributes - it's not necessary to write attributes back from this representation).
 * We do not support binary attributes - these are meant to be kept in versionables.
 */
@SuppressWarnings("UnstableApiUsage")
public class NodeAttributeComparisonInfo {

    /** The maximum string length that is used directly without hashing it. */
    private static final int MAXSTRINGLEN = 64;

    /** The absolute path to the node. */
    public String path;

    /**
     * Maps the property names of not protected properties to their String value, or hashes of their value if it's a
     * long string or a data structure. Protected properties are not mentioned, since they cannot be changed, anyway.
     */
    public Map<String, String> propertyHashes;

    /** Only for GSON - use {@link #of(Resource, String)}. */
    @Deprecated
    public NodeAttributeComparisonInfo() {
        // empty
    }

    /**
     * Creates the information about one node. This uses JCR since that's the easiest way to exclude protected
     * attributes without having to enumerate all protected attribute names.
     *
     * @param resource   the resource for which we have to compute the attribute info
     * @param pathOffset if given, we "subtract" this from the beginning of the path
     * @throws IllegalArgumentException if the path of the resource does not start with the given pathOffset
     */
    @Nonnull
    public static NodeAttributeComparisonInfo of(@Nonnull Resource resource, @Nullable String pathOffset)
            throws RepositoryException, IOException, IllegalArgumentException {
        HashFunction hash = Hashing.sipHash24();
        NodeAttributeComparisonInfo result = new NodeAttributeComparisonInfo();
        if (StringUtils.isBlank(pathOffset)) {
            result.path = resource.getPath();
        } else {
            if (!SlingResourceUtil.isSameOrDescendant(pathOffset, resource.getPath()) || !pathOffset.startsWith("/")) {
                throw new IllegalArgumentException("Not a subpath of " + pathOffset + " : " + resource.getPath());
            }
            result.path = "/" + SlingResourceUtil.relativePath(pathOffset, resource.getPath());
        }
        result.propertyHashes = new TreeMap<>();
        Node node = Objects.requireNonNull(resource.adaptTo(Node.class));
        for (PropertyIterator it = node.getProperties(); it.hasNext(); ) {
            Property prop = it.nextProperty();
            if (!prop.getDefinition().isProtected()) {
                result.propertyHashes.put(prop.getName(), stringRep(prop, hash));
            }
        }
        return result;
    }

    /**
     * Creates a String representation of the property that is limited in size (long / complicated properties are
     * hashed). The representation is meant to be of small size, but make it extremely unlikely that different values
     * have the same representation. It is prefixed with the property type.
     */
    protected static String stringRep(Property prop, HashFunction hash) throws RepositoryException, IOException {
        StringBuilder buf = new StringBuilder();
        buf.append(prop.getType()).append(":");
        if (prop.isMultiple()) {
            Value[] values = prop.getValues();
            if (valueOrderingIsUnimportant(prop.getName())) {
                Arrays.sort(values, Comparator.comparing(ExceptionUtil.sneakExceptions(Value::getString)));
            }
            Hasher hasher = hash.newHasher();
            for (Value value : values) {
                String valueRep = valueRep(value, prop.getType(), hash);
                hasher.putUnencodedChars(valueRep);
            }
            buf.append(encode(hasher.hash()));
        } else {
            String valueRep = valueRep(prop.getValue(), prop.getType(), hash);
            if (valueRep.length() > MAXSTRINGLEN) {
                valueRep = encode(hash.hashUnencodedChars(valueRep));
            }
            buf.append(valueRep);
        }
        return buf.toString();
    }

    /**
     * A string representation of the property - generated depending on the type. Given the type, it should be
     * unique to the value.
     */
    protected static String valueRep(@Nonnull Value value, int type, HashFunction hash) throws RepositoryException, IOException {
        switch (type) {
            case DATE:
                Calendar date = value.getDate();
                return date != null ? String.valueOf(date.getTimeInMillis()) : "";
            case BINARY:
                Binary binary = value.getBinary();
                Hasher hasher = hash.newHasher();
                byte[] buf = new byte[8192];
                try (InputStream stream = binary != null ? binary.getStream() : null) {
                    if (stream != null) {
                        int len;
                        while ((len = stream.read(buf)) > 0) {
                            hasher.putBytes(buf, 0, len);
                        }
                    }
                } finally {
                    if (binary != null) { binary.dispose(); }
                }
                return encode(hasher.hash());
            case DOUBLE:
                return String.valueOf(value.getDouble());
            case DECIMAL:
                return String.valueOf(value.getDecimal());
            case LONG:
                return String.valueOf(value.getLong());
            case BOOLEAN:
                return String.valueOf(value.getBoolean());
            case STRING:
            case NAME:
            case PATH:
            case REFERENCE:
            case WEAKREFERENCE:
            case URI:
                return value.getString();
            default: // impossible
                throw new IllegalArgumentException("Unknown type " + type);
        }
    }

    protected static boolean valueOrderingIsUnimportant(String propertyName) {
        return JcrConstants.JCR_MIXINTYPES.equals(propertyName);
    }

    protected static String encode(HashCode hash) {
        return hash.toString();
    }

    /** Compares path and attributes. */
    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        NodeAttributeComparisonInfo that = (NodeAttributeComparisonInfo) o;
        return Objects.equals(path, that.path) &&
                Objects.equals(propertyHashes, that.propertyHashes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, propertyHashes);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NodeAttributeComparisonInfo{");
        sb.append("path='").append(path).append('\'');
        sb.append(", propertyHashes=").append(propertyHashes);
        sb.append('}');
        return sb.toString();
    }
}
