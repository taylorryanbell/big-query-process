#
# BigQuery Process
# Taylor Bell
# 2022.1.5
#

# imports
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery


class ChangeData(beam.DoFn):
    def process(self, element):
        # print(element)
        # print(type(element))

        # print(element["name"])
        # element["name"] = (element["name"] + " New")
        # print(element["name"])
        # yield element

        yield element


def run():

    opt = PipelineOptions(
        temp_location="gs://york-trb/tmp/",
        project="york-cdf-start",
        region="us-central1",
        staging_location="gs://york-trb/staging",
        job_name="taylor-bell-process2",
        save_main_session=True
    )


    schema = {
        'fields': [
            {'name': 'order_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]
    }

    new_schema = {
        'fields': [
            {'name': 'order_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'team', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'}
        ]
    }

    out_table1 = bigquery.TableReference(
        projectId="york-cdf-start",
        datasetId="bigquerypython_out",
        tableId="bqtable1-out"
    )
    table1 = "york-cdf-start.bigquerypython.bqtable1"
    table2 = "york-cdf-start.bigquerypython.bqtable4"

    table2 = bigquery.TableReference()

    with beam.Pipeline(runner="DataflowRunner", options=opt) as pipeline:
        # read in BigQuery Tables
        data1 = pipeline | "ReadFromBigQuery1" >> beam.io.ReadFromBigQuery(
            query="SELECT table1.name, table2.last_name FROM `york-cdf-start.bigquerypython.bqtable1` as table1 " \
                  "JOIN `york-cdf-start.bigquerypython.bqtable4` as table2 ON table1.order_id = table2.order_id",
            use_standard_sql=True
        ) | "Print" >> beam.Map(print)




        # data2 = pipeline | "ReadFromBigQuery2" >> beam.io.ReadFromBigQuery(
        #     gcs_location="gs://york-trb/tmp",
        #     table="york-cdf-start:bigquerypython.bqtable2"
        # )
        # data3 = pipeline | "ReadFromBigQuery3" >> beam.io.ReadFromBigQuery(
        #     gcs_location="gs://york-trb/tmp",
        #     table="york-cdf-start:bigquerypython.bqtable3"
        # )

        # combined = (([
        #     data1, data2
        # ])
        #             | "combine" >> beam.CoGroupByKey()
        #             | beam.Map(print)
        #             )

        # perform transforms
        # changed = data1 | "PrintType" >> beam.ParDo(ChangeData())

        # write to bigquery
        # changed | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
        #     table1,
        #     schema=schema,
        #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        #     custom_gcs_temp_location="gs://york-trb/tmp"
        # )

        pass


if __name__ == '__main__':
    run()
    pass
