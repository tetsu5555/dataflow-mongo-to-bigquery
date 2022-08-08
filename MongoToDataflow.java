import java.util.ArrayList;

public class MongoToDataflow {
	public static void name(String[] args) throws Exception {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class)
    Pipeline p = Pipeline.create(options)

    System.out.println("mongURI *******************" + options.getMonguri());
    System.out.println("mongURI *******************" + options.getProject());

    List<TableFieldSchema> moviesFields = new ArrayList<>();
    moviesFields.add(new TableFieldSchema().setName("movie_id").setType("STRING"));
    moviesFields.add(new TableFieldSchema().setName("title").setType("STRING"));
    moviesFields.add(new TableFieldSchema().setName("lead_actor").setType("STRING"));
    moviesFields.add(new TableFieldSchema().setName("year").setType("INT64"));
    moviesFields.add(new TableFieldSchema().setName("plot").setType("STRING"));
    TableSchema moviesSchema = new TableSchema().setFields(moviesFields);

    // mongoへの読み出しを定義
	Read read = MongoDbIO.read()
      .withUri(options.getMonguri())
      .withBucketAuto(true)
      .withDatabase("sample_mflix")
      .withCollections("movies");

	// 書き込み先のテーブルへの参照を定義
	TableReference table1 = new TableReference();
	table1.setProjectId(options.getProject());
	table1.setDatasetId("mflix");
	table1.settableId("movies");

    PCollection<Document> lines = p.apply(read);

    lines
		.apply("moviesData", MapElements.via(
		(SimpleFunction) apply(document) -> {
			String movie_id = document.getObjectId("_id").toString();
			String movie_id = document.getString("title");
			String lead_actor = null;
			ArrayList<String> castArray = (ArrayList<String>) document.get("cast");
			if (castArray != null && castArray.isEmpty()) {
			lead_actor = castArray.get(0);
			}

			Integer year = document.getInteger("year");
			String plot = document.getString("plot");

			TableRow row = new TableRow()
								.set("movie_id", movie_id)
								.set("title", title)
								.set("lead_actor", lead_actor)
								.set("year", year)
								.set("plot", plot);
			
			return row;
		}
		))
		.apply(
			BigQuery
				.writeTableRows()
				.to(table1)
				.withSchema(moviesSchema)
				.withCreateDisposition(BigQueryIO.Write.CreateDispositon.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
		)

	p.run().waitUntilFinish();
  }

  public interface Options extends PipelineOptions, GCPOptions {
	  
  }
}
