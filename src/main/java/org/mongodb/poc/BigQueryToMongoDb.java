
package org.mongodb.poc;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.bson.Document;
import java.util.List;
import java.util.ArrayList;;

public class BigQueryToMongoDb {

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

    public static void main(String[] args) {
	Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
	run(options);
    }
    
    public static boolean run(Options options) {
	Pipeline pipeline = Pipeline.create(options);
	
	pipeline
	    .apply(BigQueryIO.readTableRows().withoutValidation().from(options.getInputTableSpec()))
	    .apply(
		   "bigQueryDataset",
		   ParDo.of(
			    new DoFn<TableRow, Document>() {
				@ProcessElement
				public void process(ProcessContext c) {
				    TableRow row = c.element();
				    Document doc = new Document();
				    row.forEach(
						(key, value) -> {
						    if (!key.equals("_id")) {
							if (value instanceof TableRow)
							    doc.append(key, processStruct((TableRow)value));		
			    				else if(value instanceof ArrayList) 
							    doc.append(key, processArray((ArrayList)value));
							else
							    doc.append(key, value);
						    }
						});
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
    
    private static Document processStruct(TableRow input) {
	Document doc = new Document();
	input.forEach(
		      (key, value) -> {		
			  if (value instanceof TableRow)
			      doc.append(key, processStruct((TableRow)value));		
			  else if(value instanceof ArrayList) 
			      doc.append(key, processArray((ArrayList)value));
			  else
			      doc.append(key, value);
		      });
	return doc;
    }


    private static ArrayList processArray(List input) {
	ArrayList arr = new ArrayList();
	input.forEach( value -> {		
		if (value instanceof TableRow)
		    arr.add(processStruct((TableRow)value));		
		else if(value instanceof ArrayList) 
		    arr.add(processArray((ArrayList)value));
		else
		    arr.add(value);
	    });
	return arr;
    }
    
}
