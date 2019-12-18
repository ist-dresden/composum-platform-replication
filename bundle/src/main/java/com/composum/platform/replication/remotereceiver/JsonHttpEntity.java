package com.composum.platform.replication.remotereceiver;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.http.HttpEntity;
import org.apache.http.entity.AbstractHttpEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/** An {@link HttpEntity} that serializes an object on the fly and writes it to the request. */
public class JsonHttpEntity<T> extends AbstractHttpEntity implements HttpEntity {

    private static final Logger LOG = LoggerFactory.getLogger(JsonHttpEntity.class);

    private final T object;
    private final Gson gson;

    /** @param object the object to serialize */
    public JsonHttpEntity(@Nullable T object, @Nullable Gson gson) {
        setContentEncoding(StandardCharsets.UTF_8.name());
        setContentType("application/json");
        this.object = object;
        this.gson = gson != null ? gson : new GsonBuilder().create();
    }

    @Override
    public void writeTo(OutputStream outstream) throws IOException {
        Charset cs;
        try (Writer writer = new OutputStreamWriter(outstream, StandardCharsets.UTF_8)) {
            gson.toJson(object, writer);
        }
    }

    @Override
    public boolean isRepeatable() {
        return true;
    }

    @Override
    public long getContentLength() {
        return -1;
    }

    @Override
    public boolean isStreaming() {
        return false;
    }

    /** Not implemented. */
    @Override
    public InputStream getContent() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("JsonHttpEntity only supports writeTo(OutputStream).");
    }

}
