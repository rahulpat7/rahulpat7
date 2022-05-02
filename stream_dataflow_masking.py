import apache_beam as beam
from apache_beam.io import ReadFromPubSub, PubsubMessage
from apache_beam.io import WriteToPubSub
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions, TestOptions
import argparse
import io
import datetime
import logging
import json
import google.cloud.dlp
from apache_beam.io.gcp.internal.clients import bigquery
import re

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(name)s : %(message)s", level=logging.INFO
)
logging = logging.getLogger(__name__)


class jsonRead:

    def __init__(self):
        logging.info("Initializing jsonreadwrite")

    def deserialize(record):
        try:
            logging.info("Reading the records from PubSub........")
            dict_record = json.loads(record)
            logging.info("-----------------------------------------------------------")
            logging.info("Input Record: {}".format(dict_record))
            return dict_record
        
        except Exception as e:
            logging.info("Exception in jsonRead : {}".format(e))

class JobOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--input", required=True, help="pubsub input topic")
        

class TransformerDoFn(beam.DoFn):
    
    import datetime
    def __init__(self):
        logging.info("Initializing")

    def process(self, record):
        try:
            logging.info("-----------------------------------------------------------")
            logging.info("Input Record: {}".format(record))
            record["LoadTimestamp"] = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            logging.info("Output Record: {}".format(record))
            logging.info("-----------------------------------------------------------")
            return [record]
        except Exception as e:
            logging.error("Got Exceptons {}", format(e), exc_info=True)
            return [record]

class DLPMasking:

    def __init__(self):
       logging.info("Initializing dlp masking class")

    def __masking__(record):
        try:
            final_dict = record
            for k, v in final_dict.items():
                logging.info("------NEW KEY VALUE---------")
                logging.info("checking PII for value : {}".format(v))
                out = DLPMasking.inspect_element(v)

                if out:
                    logging.info("new value : {}".format(out))
                    final_dict[k] = out
                    logging.info("Masked data : {}".format(final_dict))
                elif out == None and k == 'CustomerName':
                    logging.info("Manual masking in progress")
                    final_dict[k] = re.sub("([A-z])+","****", v)
                    logging.info("Masked data : {}".format(final_dict))
                elif out == None and k == 'CustomerDOB':
                    logging.info("Manual masking in progress for date of birth")
                    final_dict[k] = re.sub(r'(\d+-\d+-\d+)', v.split("-")[0], v)
                    logging.info("Masked data : {}".format(final_dict))
                elif out == None and k == 'CustomerEmail':
                    logging.info("Manual masking in progress for email address")
                    final_dict[k] = "********@" + v.split("@")[1]
                    logging.info("Masked data : {}".format(final_dict))
                else:
                    logging.info("No Finding after inspecting the element, hence not an PII value")

            logging.info("The new dictionary: {}".format(final_dict))
            logging.info("Final Dictionary to be loaded : {}".format(final_dict))
            return final_dict
            
        except Exception as e:
            logging.error("Got Exceptons under Masking {}", format(e), exc_info=True)
            #return [record]

    def inspect_element(content, info_type=None):
        try:
            import google.cloud.dlp
            from google.cloud import dlp_v2
            # Instantiate a client.
            dlp_client = dlp_v2.DlpServiceClient()
    
            #project_id
            project_id = 'gcp_project_id'
    
            # Construct the item to inspect.
            item = {"value": content}
    
            # The info types to search for in the content. Required.
            info_types = [{"name": "PERSON_NAME"}, {"name": "DATE_OF_BIRTH"}, {"name": "EMAIL_ADDRESS"}]
    
            # The minimum likelihood to constitute a match. Optional.
            min_likelihood = google.cloud.dlp_v2.Likelihood.LIKELY
    
            # The maximum number of findings to report (0 = server maximum). Optional.
            max_findings = 0
    
            # Whether to include the matching string in the results. Optional.
            include_quote = True
    
            # Construct the configuration dictionary. Keys which are None may
            # optionally be omitted entirely.
            inspect_config = {
                "info_types": info_types,
                "min_likelihood": min_likelihood,
                "include_quote": include_quote,
                "limits": {"max_findings_per_request": max_findings},
            }
    
            # Convert the project id into a full resource id.
            parent = f"projects/{project_id}"
            logging.info("project name : {}".format(parent))
    
            # Call the API.
            response = dlp_client.inspect_content(
                    #parent, inspect_config, item
                    request={"parent":parent, "inspect_config": inspect_config, "item": item}
            )
    
            # Print out the results.
            if response.result.findings:
                for finding in response.result.findings:
                    try:
                        logging.info("Quote: {}".format(finding.quote))
                    except AttributeError:
                        pass
                    logging.info("Info type: {}".format(finding.info_type.name))
                    # Convert likelihood value to string respresentation.
                    likelihood = finding.likelihood
                    logging.info("Likelihood: {}".format(likelihood))
                    
                    if finding.info_type.name == 'PERSON_NAME':
                        logging.info("person name is PII : {}".format(finding.info_type.name))
                        replacement_str = '********'
                        
                        #Masking starts
                        out = DLPMasking.deidentify_with_replace(project_id, finding.quote, ['PERSON_NAME'], replacement_str)
                        return out
                        
                    elif finding.info_type.name == 'EMAIL_ADDRESS':
                        logging.info("email address is pii : {} ".format(content))
                        logging.info("dtype of content : {}".format(content))
                        replacement_str = '*******' + str(content).split("@")[1]
                        logging.info("replace with string : {}".format(replacement_str))
                        
                        #Masking starts
                        out = DLPMasking.deidentify_with_replace(project_id, finding.quote, ['EMAIL_ADDRESS'], replacement_str)
                        logging.info("final output : {}".format(out))
                        return out

                    elif finding.info_type.name == 'DATE_OF_BIRTH':
                        masking_character = 'YYYY-MM-DD'
                        #Masking starts
                        out = DLPMasking.deidentify_with_replace(project_id, finding.quote, ['DATE_OF_BIRTH'], masking_character)
                        return out

            else:
                logging.info("No findings after inspection.")
                return None
                
        except Exception as e:
            logging.info('Exception in inspect element : {}'.format(e))
            
    def deidentify_with_replace(project, input_str, info_types, replacement_str="************"):

        """Uses the Data Loss Prevention API to deidentify sensitive data in a
        string by replacing matched input values with a value you specify.
        Args:
            project: The Google Cloud project id to use as a parent resource.
            input_str: The string to deidentify (will be treated as text).
            info_types: A list of strings representing info types to look for.
            replacement_str: The string to replace all values that match given
                info types.
        Returns:
            None; the response from the API is printed to the terminal.
        """
        try:
            import google.cloud.dlp
            from google.cloud import dlp_v2

            # Instantiate a client
            dlp = dlp_v2.DlpServiceClient()

            # Convert the project id into a full resource id.
            parent = f"projects/{project}"
            logging.info("******************Inside deidentify with replace***********************")
            # Construct inspect configuration dictionary
            inspect_config = {"info_types": [{"name": info_type} for info_type in info_types]}
            logging.info("Inspect config is {}".format(inspect_config))

            # Construct deidentify configuration dictionary
            deidentify_config = {
                "info_type_transformations": {
                    "transformations": [
                        {
                            "primitive_transformation": {
                                "replace_config": {
                                    "new_value": {"string_value": replacement_str}
                                }
                            }
                        }
                    ]
                }
            }
            logging.info("deidentify config is : {}".format(deidentify_config))
            # Construct item
            item = {"value": input_str}
            logging.info("Item :", item)

            # Call the API
            response = dlp.deidentify_content(
                request={"parent": parent,"deidentify_config": deidentify_config,"inspect_config": inspect_config,"item": item}
                #parent, deidentify_config, inspect_config, item
            )

            # Print out the results.
            logging.info(response.item.value)
            return response.item.value


        except Exception as e:
            logging.info('Exception in deidentify with replace : {}'.format(e))
            
def run(argv=None, save_main_session=True):

    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    job_options = pipeline_options.view_as(JobOptions)

    gcp_options = job_options.view_as(GoogleCloudOptions)
    gcp_options.project = 'gcp_project_id'
    gcp_options.temp_location = 'gs://cloud-hackathon-gcp/dataflow/tmp'
    gcp_options.region = 'europe-west2'
    gcp_options.staging_location = 'gs://cloud-hackathon-gcp/dataflow/df-stage'

    p = beam.Pipeline(options=pipeline_options)

    logging.info("------------------Pipeline Creation------------------------")
    logging.info("----------Dataflow Streaming For Masking PII Data----------")
    logging.info("-----------------------------------------------------------")

    source = ReadFromPubSub(subscription=str(job_options.input))
    
    table_spec = bigquery.TableReference(
    projectId='gcp_project_id',
    datasetId='hackathon_gcp_bq',
    tableId='loan_data_masked_new')

    table_schema = "TransactionID:STRING, CustomerID:STRING, CustomerName:STRING, CustomerDOB:STRING, CustAccountBalance:STRING, LoanAmount:STRING, CustomerEmail:STRING, CustLocation:STRING, TotalAmountPaid:STRING, LoanDuration:STRING, LoanRate:STRING, LoanSanctDate:STRING, LastCycleDate:STRING, IssuerBank:STRING, LoanEndDate:STRING, RemainingLoan:STRING, LoadTimestamp:STRING"
    
    sink = beam.io.WriteToBigQuery(table_spec,schema=table_schema,write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    
    lines = (
        p
        | "read from pubsub" >> source
        | "Deserialize JSON" >> beam.Map(lambda record: jsonRead.deserialize(record))
        | "Append a load_timestamp column" >> (beam.ParDo(TransformerDoFn()))
        | "Masking the PII values" >> beam.Map(lambda record: DLPMasking.__masking__(record))
        | "Writing to BigQuery" >> sink
    )

    result = p.run()
    result.wait_until_finish()


if __name__ == "__main__":
    run()
    
    
