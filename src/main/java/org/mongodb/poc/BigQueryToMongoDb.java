
package org.mongodb.poc;

import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.ReadOptions.TableReadOptions;
import com.google.cloud.bigquery.storage.v1beta1.Storage.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1beta1.TableReferenceProto;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.bson.Document;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import com.google.api.gax.rpc.InvalidArgumentException;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.avro.Schema;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericData;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.avro.util.Utf8;





public class BigQueryToMongoDb {

    /* Logger for class. */
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryToMongoDb.class);
    
    public interface Options extends PipelineOptions {

	//MongoDB
	String getMongoDbUri();
	void setMongoDbUri(String getMongoDbUri);
	String getDatabase();
	void setDatabase(String database);
	String getCollection();
	void setCollection(String collection);

	//BigQuery
	String getInputTableSpec();
	void setInputTableSpec(String inputTableSpec);
    }


    /** Factory to create BigQueryStorageClients. */
    static class BigQueryStorageClientFactory {

	/**
	 * Creates BigQueryStorage client for use in extracting table schema.
	 *
	 * @return BigQueryStorageClient
	 */
	static BigQueryStorageClient create() {
	    try {
		return BigQueryStorageClient.create();
	    } catch (IOException e) {
		LOG.error("Error connecting to BigQueryStorage API: " + e.getMessage());
		throw new RuntimeException(e);
	    }
	}
    }

    /** Factory to create ReadSessions. */
    static class ReadSessionFactory {

	/**
	 * Creates ReadSession for schema extraction.
	 *
	 * @param client BigQueryStorage client used to create ReadSession.
	 * @param tableString String that represents table to export from.
	 * @param tableReadOptions TableReadOptions that specify any fields in the table to filter on.
	 * @return session ReadSession object that contains the schema for the export.
	 */
	static ReadSession create(
				  BigQueryStorageClient client, String tableString, TableReadOptions tableReadOptions) {
	    TableReference tableReference = BigQueryHelpers.parseTableSpec(tableString);
	    String parentProjectId = "projects/" + tableReference.getProjectId();
	
	    TableReferenceProto.TableReference storageTableRef =
		TableReferenceProto.TableReference.newBuilder()
		.setProjectId(tableReference.getProjectId())
		.setDatasetId(tableReference.getDatasetId())
		.setTableId(tableReference.getTableId())
		.build();
	
	    CreateReadSessionRequest.Builder builder =
		CreateReadSessionRequest.newBuilder()
		.setParent(parentProjectId)
		.setReadOptions(tableReadOptions)
		.setTableReference(storageTableRef);
	    try {
		return client.createReadSession(builder.build());
	    } catch (InvalidArgumentException iae) {
		LOG.error("Error creating ReadSession: " + iae.getMessage());
		throw new RuntimeException(iae);
	    }
	}
    }


    /**
     * The {@link BigQueryToParquet#getTableSchema(ReadSession)} method gets Avro schema for table
     * using from the {@link ReadSession} object.
     *
     * @param session ReadSession that contains schema for table, filtered by fields if any.
     * @return avroSchema Avro schema for table. If fields are provided then schema will only contain
     *     those fields.
     */
    private static Schema getTableSchema(ReadSession session) {
	Schema avroSchema;

	avroSchema = new Schema.Parser().parse(session.getAvroSchema().getSchema());
	LOG.info("Schema for export is: " + avroSchema.toString());

	return avroSchema;
    }

    public static void main(String[] args) {
	Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
	run(options);
    }
    
    public static boolean run(Options options) {
	Pipeline pipeline = Pipeline.create(options);

	TableReadOptions.Builder builder = TableReadOptions.newBuilder();
	TableReadOptions tableReadOptions = builder.build();
	BigQueryStorageClient client = BigQueryStorageClientFactory.create();
	ReadSession session =
	    ReadSessionFactory.create(client, options.getInputTableSpec(), tableReadOptions);

	// Extract schema from ReadSession
	Schema schema = getTableSchema(session);
	client.close();
	
	pipeline
	    .apply(BigQueryIO.read(SchemaAndRecord::getRecord)
		   .from(options.getInputTableSpec())
		   .withTemplateCompatibility()
		   .withMethod(Method.DIRECT_READ)
		   .withCoder(AvroCoder.of(schema))
		   )
	    .apply(
		   "bigQueryDataset",
		   ParDo.of(
			    new DoFn<GenericRecord, Document>() {
				@ProcessElement
				public void process(ProcessContext c) {
				    GenericRecord row = c.element();
				    Document doc = new Document();
				    for (Field field : row.getSchema().getFields()) {
					String key = field.name();
					Object value = row.get(key);
					if (!key.equals("_id")) {
					    doc.append(key, processValue(value, field.schema()));
					}
				    }
				    c.output(doc);
				}
			    }))
	    .apply(
		   MongoDbIO.write()
		   .withUri(options.getMongoDbUri())
		   .withDatabase(options.getDatabase())
		   .withCollection(options.getCollection()));
	pipeline.run();
	return true;
    }

    private static Object processValue(Object value, Schema fieldType) {
	if (value instanceof Record)
	    return processStruct((Record) value);
	else if (value instanceof GenericData.Array)
	    return processArray((GenericData.Array) value);						
	else if (value instanceof Utf8)
	    return value.toString();
	else if (is_timestamp(fieldType))
	    return value == null ? null : new Date(((Long) value) / 1000);
	else
	    return value;
    }

    private static Document processStruct(Record input) {
	Document doc = new Document();
	for (Field field : input.getSchema().getFields()) {
	    String key = field.name();
	    Object value = input.get(key);
	    doc.append(key, processValue(value, field.schema()));
	}
	return doc;
    }

    
    private static ArrayList processArray(GenericData.Array input) {
	ArrayList arr = new ArrayList();
	for(Object value : input) {
	    arr.add(processValue(value, input.getSchema().getElementType()));
	};
	return arr;
    }

    private static boolean is_timestamp(Schema fieldType) {
	boolean res = false;
	Schema.Type type = fieldType.getType();
	if (type == Schema.Type.LONG) {
	    LogicalType logicalType = fieldType.getLogicalType();
	    res = logicalType != null && "timestamp-micros".equals(logicalType.getName());
	} else if(type == Schema.Type.UNION) {
	    for(Schema s: fieldType.getTypes()) {
		LogicalType lt = s.getLogicalType();
		if(lt != null && "timestamp-micros".equals(lt.getName()))
		   res = true;
	    }
	}
	return res;
    }
    
    
}
