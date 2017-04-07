package snippets.json;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by rnair on 3/27/17.
 */
public class ObjectMapperTest {


    @Test(expected = IOException.class)
    public void eror_when_fieldName_not_provided() throws Exception {
        String testString = "{\"x\":\"value1\"}";
        TestClass obj = new ObjectMapper().readValue(testString, TestClass.class);

    }

    @Test
    public void when_field_public() throws Exception {
        String testString = "{\"x\":\"value1\"}";
        TestClass2 obj = new ObjectMapper().readValue(testString, TestClass2.class);

    }

    @Test
    public void with_public_getters_setters() throws IOException {
        String testString = "{\"x\":\"value1\"}";
        TestClass3 obj = new ObjectMapper().readValue(testString, TestClass3.class);
    }

    @Test
    public void deserializing_test() throws JsonProcessingException{
        TestClass4 t4 = new TestClass4();
        String obj = new ObjectMapper().writeValueAsString(t4);
    }

    static class TestClass {
        private String x;
    }

    static class TestClass2 {
        public String x;
    }

    static class TestClass3 {
        private String x;

        public String getX() {
            return x;
        }

    }

    static class TestClass4 {
        @JsonProperty("x")
        private String x;
    }
}
