package software.amazon.dynamo.streamsadapter.util;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Base64.Decoder;
import software.amazon.awssdk.services.dynamodb.model.Record;

public class KDSRecordObjectMapper extends ObjectMapper {

  private static final Decoder decoder = Base64.getDecoder();

  public KDSRecordObjectMapper() {
    super();
    SimpleModule module = new SimpleModule();
    module.addDeserializer(ByteBuffer.class, new ByteBufferDeserializer());

    this.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
    this.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    this.configure(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS, false);
    this.registerModule(module);
    this.findAndRegisterModules();
  }

  private static class ByteBufferDeserializer extends JsonDeserializer<ByteBuffer> {
    @Override
    public ByteBuffer deserialize(JsonParser jsonParser,
        DeserializationContext deserializationContext) throws IOException, JacksonException {
      // for binary data types, must decode base64 value otherwise we would need to handle downstream
      // for BinarySets (BS) currentName will be null
      String currentName = jsonParser.getCurrentName();
      if (currentName != null && currentName.equals("B")) {
        return ByteBuffer.wrap(decoder.decode(jsonParser.getBinaryValue()));
      } else {
        return ByteBuffer.wrap(jsonParser.getBinaryValue());
      }
    }
  }

  public Record readValue(byte[] src) throws IOException {
    return readValue(src, Record.serializableBuilderClass()).build();
  }
}