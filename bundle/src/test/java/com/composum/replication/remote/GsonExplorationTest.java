package com.composum.replication.remote;

import com.composum.sling.platform.testing.testutil.ErrorCollectorAlwaysPrintingFailures;
import com.google.common.collect.Range;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Iterator;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/** Some checks how GSON works. */
public class GsonExplorationTest {

    @Rule
    public final ErrorCollectorAlwaysPrintingFailures ec = new ErrorCollectorAlwaysPrintingFailures();

    @Test
    public void checkSerialization() {
        Gson gson = new GsonBuilder().create();
        ec.checkThat(gson.toJson(15), is("15"));
        ec.checkThat(gson.toJson(Arrays.asList(12,13,14)), is("[12,13,14]"));
        ec.checkThat(gson.toJson(
                new AbstractList<Integer>() {
                    @Override
                    public int size() {
                        return 4;
                    }

                    @Override
                    public Integer get(int index) {
                        return index + 17;
                    }
                }
        ), is("null")); // Ouch!
    }

    @Test
    public void serializationWithAdapter() {
        Gson gson = new GsonBuilder().create();
        StreamingCollection col = new StreamingCollection();
        col.values = Arrays.asList(1,2,3,4,5);
        ec.checkThat(gson.toJson(col), is("{\"values\":[1,2,3,4,5]}"));
    }

    public static class StreamingCollection {
        @JsonAdapter(IterableAdapter.class)
        Iterable<Integer> values;
    }

    private static class IterableAdapter extends TypeAdapter<Iterable<Integer>> {

        @Override
        public void write(JsonWriter out, Iterable<Integer> value) throws IOException {
            out.beginArray();
            for (Integer val: value) {
                out.value(val);
            }
            out.endArray();
        }

        @Override
        public Iterable<Integer> read(JsonReader in) throws IOException {
            throw new UnsupportedOperationException("Not implemented yet: IterableAdapter.read");
        }
    }
}
