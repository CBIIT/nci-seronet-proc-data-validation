import time
import pandas as pd
import sqlalchemy as sd
import os
import re
import yaml
import datetime
import warnings
from dateutil.parser import parse
import dateutil
import boto3
import hashlib
import urllib3
import json
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import email.utils
import smtplib
#from get_data_to_check_v2 import get_summary_file
#import db_loader_ref_pannels
# import db_loader_vac_resp
#import db_loader_v4
#from bio_repo_map import bio_repo_map

#from connect_to_sql_db import connect_to_sql_db
from File_Submission_Object_v2 import Submission_Object
import Validation_Rules_v2 as vald_rules
#########################################################################################hhh####
#  import templates abd CBC codes directly from box
#  connect to S3 client and return handles for future use
def lambda_handler(event, context):
    start_time = time.time()
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    print("## Running Set Up Functions")
    file_sep, s3_client, s3_resource, Support_Files, validation_date = set_up_function(bucket)
    #study_type = "Refrence_Pannel"
    study_type = "Vaccine_Response"
    template_df, dbname = get_template_data(s3_client, study_type, bucket)
    print("Initialization took %.2f seconds" % (time.time() - start_time))
    ssm = boto3.client("ssm")
    file_path = event["Records"][0]["s3"]["object"]["key"]
    cbc_name_list = {"cbc01" : ["Feinstein_CBC01", 41], "cbc02": ["UMN_CBC02", 27], "cbc03": ["ASU_CBC03", 32], "cbc04": ["Mt_Sinai_CBC04",14]}
    # Accrual_Need_To_Validate/cbc01/2023-04-20-12-59-25/submission_007_Prod_data_for_feinstein20230420_VaccinationProject_Batch9_shippingmanifest.zip/File_Validation_Results/Result_Message.txt
    file_path = file_path.replace("+", " ")         #some submissions might have spaces this line corrects the "+" replacement
    curr_cbc = file_path.split("/")[1] #should be 1 if the path is under need_to_validate function
    sub_name = file_path.split("/")[3][15:]
    site_name = cbc_name_list[curr_cbc][0]
    passed_bucket = ssm.get_parameter(Name="data_validation_passed_bucket", WithDecryption=True).get("Parameter").get("Value")
    fail_bucket = ssm.get_parameter(Name="data_validation_failed_bucket", WithDecryption=True).get("Parameter").get("Value")
    slack_pass = ssm.get_parameter(Name="success_hook_url", WithDecryption=True).get("Parameter").get("Value")
    USERNAME_SMTP = ssm.get_parameter(Name="USERNAME_SMTP", WithDecryption=True).get("Parameter").get("Value")
    PASSWORD_SMTP = ssm.get_parameter(Name="PASSWORD_SMTP", WithDecryption=True).get("Parameter").get("Value")
    error_message = Data_Validation_Main(study_type, template_df, dbname, s3_client, s3_resource, Support_Files, validation_date, file_sep, curr_cbc, file_path, bucket, ssm, passed_bucket)
    slack_fail = ssm.get_parameter(Name="failure_hook_url", WithDecryption=True).get("Parameter").get("Value")
    if len(error_message['message']) > 0:
        message_slack_fail = "Your data submission " + sub_name +" has been analyzed by the validation software.\n"
        if error_message["message_type"] == "data_validation_error":
            for i in range(0, len(error_message['message'])):
                if i != len(error_message['message']) - 1:
                    message_slack_fail = message_slack_fail + str(i + 1) + ") " + error_message['message'][i] + "\n"
                else:
                    message_slack_fail = message_slack_fail + error_message['message'][i] + "\n"
        else:
            for m in error_message['message']:
                message_slack_fail = message_slack_fail + m + "\n"
        write_to_slack(message_slack_fail, slack_fail)
        move_submission(bucket, fail_bucket, file_path, s3_client, s3_resource, site_name, curr_cbc, study_type)
        send_error_email(ssm, sub_name, list(error_message['message']), message_slack_fail)
    else:
        message_slack_success = "Your data submission " + sub_name + " has been analyzed by the validation software.\n"
        + "1) Analysis of the Zip File: Passed\n"
        + "2) Data Validation: Passed\n"
        write_to_slack(message_slack_success, slack_pass)
        move_submission(bucket, passed_bucket, file_path, s3_client, s3_resource, site_name, curr_cbc, study_type)



def Data_Validation_Main(study_type, template_df, dbname, s3_client, s3_resource, Support_Files, validation_date, file_sep, curr_cbc, file_path, bucket, ssm, passed_bucket):
    error_message = {"message_type": '', "message": []}
    if len(template_df) == 0:
        print("Study Name was not found, please correct")
        error_message['message_type'] = "study_not_found"
        error_message['message'].append("Study Name was not found, please correct")
        return error_message

    #root_dir = "C:\\Seronet_Data_Validation"  # Directory where Files will be downloaded
    ignore_validation_list = ["submission.csv", "assay.csv", "assay_target.csv", "baseline_visit_date.csv"]
    check_BSI_tables = False
    make_rec_report = False
    #upload_ref_data = False
##############################################################################################
    start_time = time.time()
    host_client = ssm.get_parameter(Name="db_host", WithDecryption=True).get("Parameter").get("Value")
    user_name = ssm.get_parameter(Name="lambda_db_username", WithDecryption=True).get("Parameter").get("Value")
    user_password =ssm.get_parameter(Name="lambda_db_password", WithDecryption=True).get("Parameter").get("Value")
    sql_tuple = connect_to_sql_db(host_client, user_name, user_password, dbname)
    print("Connection to SQL database took %.2f seconds" % (time.time() - start_time))
    '''
    if upload_ref_data is True and study_type == "Refrence_Pannel":
        #  db_loader_ref_pannels.write_panel_to_db(sql_tuple, s3_client, bucket)
        db_loader_ref_pannels.write_requests_to_db(sql_tuple, s3_client, bucket)
        db_loader_ref_pannels.make_manifests(sql_tuple, s3_client, s3_resource, bucket)
        return
    '''
    if make_rec_report is True:
        generate_rec_report(sql_tuple, s3_client, cbc_bucket)
###############################################################################################
#    if check_BSI_tables is True:
#        check_bio_repo_tables(s3_client, s3_resource, study_type)  # Create BSI report using file in S3 bucket
###############################################################################################
    cbc_bucket = passed_bucket #some files are in the pass bucket
    if study_type == "Refrence_Pannel":
        # sql_table_dict = db_loader_ref_pannels.Db_loader_main(sql_tuple, validation_date)

        sql_table_dict = get_sql_dict_ref(s3_client, cbc_bucket)
    elif study_type == "Vaccine_Response":
        sql_table_dict = get_sql_dict_vacc(s3_client, cbc_bucket)
#############################################################################################
# compares S3 destination to S3-Passed and S3-Failed to get list of submissions to work
    try:
#############################################################################################
# pulls the all assay data directly from box
        start_time = time.time()
        #assay_data, assay_target, all_qc_data, converion_file = get_box_data_v2.get_assay_data("CBC_Data")
        assay_data, assay_target, all_qc_data, converion_file = get_assay_data(s3_client, "CBC_Data", cbc_bucket)
        study_design = pd.read_sql(("Select * from Study_Design"), sql_tuple[1])
        study_design.drop("Cohort_Index", axis=1, inplace=True)
        #get_box_data_v2.get_study_design()

        print("\nLoading Assay Data took %.2f seconds" % (time.time()-start_time))
############################################################################################
        # reference pannel study is over
        '''
        if study_type == "Refrence_Pannel":
            check_serology_submissions(s3_client, bucket, assay_data, assay_target, success_msg, error_msg, study_type)
            vald_rules.check_serology_shipping(pd, s3_client, bucket, sql_tuple)
        '''
############################################################################################
# Creates Sub folders to place submissions in based on validation results
        file_folder_key = os.path.dirname(os.path.split(file_path)[0])
        print(file_folder_key)
        print(bucket)
        resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=file_folder_key)
        file_list = resp['Contents']
        print(resp)
#############################################################################################
        if len(file_list) == 0:
            print("\nThe Files_To_Validate Folder is empty, no Submissions Downloaded to Process\n")
#############################################################################################

        print("\n##    Starting Data Validation for " + curr_cbc + "    ##")
        #file_list = check_if_done(file_list)
        file_path, file_name = os.path.split(file_folder_key)         # gets file path and file name
        curr_date = os.path.split(file_path)[1]                    # trims date from file path
        list_of_files = [i['Key'] for i in file_list if "UnZipped_Files" in i['Key']]
        result_key = os.path.join(file_folder_key, "File_Validation_Results/Result_Message.txt")
        passing_msg = "File Passed File-Validation"
        result_message = check_passed_file_validation(result_key, file_list, bucket, s3_client)
        if result_message != passing_msg: #if the file fail the file validation
            #move_target_folder(curr_date, file_sep, file_path, "01_Failed_File_Validation")
            print("Analysis of the Zip File: Failed, " + result_message)
            error_message['message_type'] = 'file_validation'
            error_message['message_type'].append("Analysis of the Zip File: Failed, " + result_message)
            #  update excel file
        else:
            print("Analysis of the Zip File: Passed, " + result_message)
            if "submission" in file_name:
                current_sub_object = Submission_Object(file_name[15:])  # creates the Object
            else:
                current_sub_object = Submission_Object(file_name)  # creates the Object
            try:
                current_sub_object.initalize_parms(file_folder_key, template_df,
                                                    sql_tuple[0], sql_table_dict, bucket, s3_client)
            except PermissionError:
                print("One or more files needed is open, not able to proceed")
                #return?

            print("\n## Starting the Data Validation Proccess for " + current_sub_object.File_Name + " ##")

            try:
                current_sub_object, study_name = populate_object(current_sub_object, list_of_files, Support_Files, study_type)
                if study_name != study_type:
                    print(f"##  Submission not in {study_type}, correct and rerun ##")
                    error_message['message_type'] = "submission_validation_error"
                    error_message['message'] = error_message['message'].append("Submission not in {study_type}, correct and rerun")
                    #continue
                    return error_message
                col_error_message = ''
                col_err_count, col_error_message = current_sub_object.check_col_errors(file_folder_key)
                if col_err_count > 0:
                    print("Submission has Column Errors, Data Validation NOT Preformed")
                    error_message['message_type'] = "submission_validation_error"
                    error_message['message'].append(col_error_message)
                    error_message['message'].append("Submission has Column Errors, Data Validation NOT Preformed")
                    #continue
                    return error_message
                current_sub_object = zero_pad_ids(current_sub_object)
                current_sub_object.get_all_unique_ids(re)
                current_sub_object.rec_file_names = list(current_sub_object.Data_Object_Table.keys())
                current_sub_object.populate_missing_keys(sql_tuple)
                data_table = current_sub_object.Data_Object_Table
                empty_list = []

                for file_name in data_table:
                    current_sub_object.set_key_cols(file_name, study_type)
                    if len(data_table[file_name]["Data_Table"]) == 0 and "visit_info_sql.csv" not in file_name:
                        empty_list.append(file_name)
                for index in empty_list:
                    del current_sub_object.Data_Object_Table[index]
            except Exception as e:
                display_error_line(e)
                #continue
                return
            try:
                current_sub_object.create_visit_table("baseline.csv", study_type)
                current_sub_object.create_visit_table("follow_up.csv", study_type)
            except Exception as e:
                display_error_line(e)
                print("Submission does not match study type, visit info is missing")

            current_sub_object.update_object(assay_data, "assay.csv")
            current_sub_object.update_object(assay_target, "assay_target.csv")
            current_sub_object.update_object(study_design, "study_design.csv")

            data_table = current_sub_object.Data_Object_Table
            if "baseline_visit_date.csv" in data_table:
                current_sub_object.Data_Object_Table = {"baseline_visit_date.csv": data_table["baseline_visit_date.csv"]}

            valid_cbc_ids = str(current_sub_object.CBC_ID)
            for file_name in current_sub_object.Data_Object_Table:
                try:
                    if file_name in ignore_validation_list or "_sql.csv" in file_name:
                        continue
                    if "Data_Table" in current_sub_object.Data_Object_Table[file_name]:
                        data_table = current_sub_object.Data_Object_Table[file_name]['Data_Table']
                        data_table.fillna("N/A", inplace=True)
                        data_table = current_sub_object.correct_var_types(file_name, study_type)
                except Exception as e:
                    display_error_line(e)
            for file_name in current_sub_object.Data_Object_Table:
                tic = time.time()
                try:
                    if file_name in ignore_validation_list or "_sql.csv" in file_name:
                        continue
                    if "Data_Table" in current_sub_object.Data_Object_Table[file_name]:
                        data_table, drop_list = current_sub_object.merge_tables(file_name)
                        current_sub_object.Data_Object_Table[file_name]['Data_Table'] = data_table.drop(drop_list, axis=1)
                        current_sub_object.Data_Object_Table[file_name]['Data_Table'].drop_duplicates(inplace=True)
                        current_sub_object = vald_rules.Validation_Rules(re, datetime, current_sub_object, data_table,
                                                                            file_name, valid_cbc_ids, drop_list, study_type)
                        if file_name in current_sub_object.rec_file_names:
                            current_sub_object.check_dup_visit(pd, data_table, drop_list, file_name)
                        toc = time.time()
                        print(f"{file_name} took %.2f seconds" % (toc-tic))
                    else:
                        print(file_name + " was not included in the submission")
                except Exception as e:
                    display_error_line(e)
            vald_rules.check_ID_Cross_Sheet(current_sub_object, re, file_sep, study_name)
            if file_name in ["baseline_visit_date.csv"]:
                vald_rules.check_baseline_date(current_sub_object, pd, sql_tuple, parse)
            elif study_type == "Vaccine_Response":
                current_sub_object.compare_visits("baseline")
                current_sub_object.compare_visits("followup")
                vald_rules.check_comorbid_hist(pd, sql_tuple, current_sub_object)
                vald_rules.check_vacc_hist(pd, sql_tuple, current_sub_object)
                current_sub_object.check_comorbid_dict(pd, sql_tuple[2])
            elif study_type == "Refrence_Pannel":
                vald_rules.compare_SARS_tests(current_sub_object, pd, sql_tuple[2])
            vald_rules.check_shipping(current_sub_object, pd, sql_tuple[2])
            try:
                dup_visits = current_sub_object.dup_visits
                if len(dup_visits) > 0:
                    dup_visits = dup_visits.query("File_Name in ['baseline.csv', 'follow_up.csv']")
                    dup_count = len(dup_visits)
                else:
                    dup_count = 0
                err_count = len(current_sub_object.Error_list)
                current_sub_object.write_error_file(file_sep)

                if dup_count > 0:
                    print("Duplicate Visit Information was found")
                    #dup_visits.to_csv(current_sub_object.Data_Validation_Path + file_sep + "Duplicate_Visit_ID_Errors.csv", index=False)
                    to_s3_csv(dup_visits, current_sub_object.Data_Validation_Path + file_sep + "Duplicate_Visit_ID_Errors.csv", False, bucket, s3_client)
                if err_count == 0 and dup_count == 0:
                    error_str = "No Errors were Found during Data Validation"
    #                        move_target_folder(curr_date, file_sep, file_path, "02_Passed_Data_Validation")
                    print(error_str)
                else:
                    error_str = ("Data Validation found " + str(len(current_sub_object.Error_list)) +
                                    " errors in the submitted files")
                    error_message['message_type'] = 'data_validation_error'
                    uni_name = list(set(current_sub_object.Error_list["CSV_Sheet_Name"]))
                    for iterU in uni_name:
                        curr_table = current_sub_object.Error_list.query("CSV_Sheet_Name == @iterU")
                        error_message['message'].append(iterU + " has " + str(len(curr_table)) + " Errors")
                    error_message['message'].append("Data Validation found " + str(len(current_sub_object.Error_list)) + " errors in the submitted files")
                    # current_sub_object.split_into_error_files(os, file_sep)
    #                        move_target_folder(curr_date, file_sep, file_path, "04_Failed_Data_Validation")
                    print(error_str)
            except Exception as err:
                print("An Error Occured when trying to write output file")
                display_error_line(err)
    except Exception as e:
        print(e)
        display_error_line(e)
    finally:
        '''
        if study_type == "Refrence_Pannel":
            populate_md5_table(pd, sql_tuple, study_type)
        '''
        close_connections(dbname, sql_tuple)
    print("\nALl folders have been checked")
    print("Closing Validation Program")
    return error_message


def close_connections(file_name, conn_tuple):
    print("\n## Connection to " + file_name + " has been closed ##\n")
    conn_tuple[2].close()    # conn
    conn_tuple[1].dispose()  # engine




#def check_bio_repo_tables(s3_client, s3_resource, study_type):
#    print("\n## Checking Latest BSI report that was uploaded to S3 ##")
#    start_time = time.time()
#    dup_df = bio_repo_map(s3_client, s3_resource, study_type)
#    if len(dup_df) == 0:
#        print("## Biorepository_ID_map.xlsx file has been updated ## \n")
#    else:
##        print("## Duplicate IDs were found in the Biorepository.  Please Fix## \n")
#    print("Biorepository Report took %.2f seconds" % (time.time() - start_time))




def display_error_line(ex):
    trace = []
    tb = ex.__traceback__
    while tb is not None:
        trace.append({"filename": tb.tb_frame.f_code.co_filename,
                      "name": tb.tb_frame.f_code.co_name,
                      "lineno": tb.tb_lineno})
        tb = tb.tb_next
    print(str({'type': type(ex).__name__, 'message': str(ex), 'trace': trace}))



def check_passed_file_validation(result_key, file_list, bucket, s3_client):
    passing_msg = ("File is a valid Zipfile. No errors were found in submission. " +
                   "Files are good to proceed to Data Validation")
    result_message = check_result_message(result_key, file_list, passing_msg, bucket, s3_client)
    return result_message


def check_file(file, file_list):
    for file_key in file_list:
        if file in file_key['Key']:
            return True
    return False

def check_result_message(result_key, file_list, passing_msg, bucket, s3_client):
    result_message = "Result File Is Missing"
    if not check_file("File_Validation_Results", file_list):
        print("File-Validation has not been run on this submission\n")
    else:
        if check_file("Result_Message.txt", file_list):
            try:
                result_file = s3_client.get_object(Bucket=bucket, Key=result_key)
                result_message = result_file['Body'].read().decode('utf-8')
            except Exception as e:
                print(e)
                return result_message
            if result_message != passing_msg:
                print("Submitted File FAILED the File-Validation Process. With Error Message: " + result_message + "\n")
            else:
                result_message = "File Passed File-Validation"
    return result_message


def populate_object(current_sub_object, list_of_files, Support_Files, study_type):
    for file_key in list_of_files:
        current_sub_object.get_data_tables(file_key, study_type)
        file_name = os.path.basename(file_key)
        if file_name not in ["study_design.csv"]:   # do not check columns for this file
            current_sub_object.column_validation(file_name, Support_Files)
    study_name = current_sub_object.get_submission_metadata(Support_Files)
    return current_sub_object, study_name




def zero_pad_ids(curr_obj):
    data_dict = curr_obj.Data_Object_Table
    try:
        if "aliquot.csv" in data_dict:
            z = data_dict["aliquot.csv"]["Data_Table"]["Aliquot_ID"].tolist()
            z = [i[0:13] + "_0" + i[14] if i[-2] == '_' else i for i in z]
            data_dict["aliquot.csv"]["Data_Table"]["Aliquot_ID"] = z
        if "shipping_manifest.csv" in data_dict:
            z = data_dict["shipping_manifest.csv"]["Data_Table"]["Current Label"].tolist()
            for index in range(len(z)):
                if len(z[index]) <= 16:   # valid aliquot length
                    pass
                elif z[index][15].isnumeric():
                    z[index] = z[index][:16]
                elif z[index][14].isnumeric():
                    z[index] = z[index][:15]
                else:
                    print("unknown string")

            z = [i[0:13] + "_0" + i[14] if i[-2] == '_' else i for i in z]
            data_dict["shipping_manifest.csv"]["Data_Table"]["Current Label"] = z
    except Exception as e:
        print(e)
    curr_obj.Data_Object_Table = data_dict
    return curr_obj


def generate_rec_report(sql_tuple, s3_client, bucket):
    print("\n Generating Requestion Excel Files for BSI")
    start_time = time.time()
    curr_file = "Serology_Data_Files/biorepository_id_map/Biorepository_ID_map.xlsx"
    #parent_data = pd_s3.get_df_from_keys(s3_client, bucket, curr_file, suffix="xlsx", sheet_name="BSI_Parent_Aliquots", format="xlsx", na_filter=False, output_type="pandas")
    obj = s3_client.get_object(Bucket=bucket, Key= curr_file["Key"])
    parent_data = pd.read_excel(obj['Body'].read(), sheet_name="BSI_Parent_Aliquots", engine='openpyxl')
    #child_data = pd_s3.get_df_from_keys(s3_client, bucket, curr_file, suffix="xlsx", sheet_name="BSI_Child_Aliquots", format="xlsx", na_filter=False, output_type="pandas")
    child_data = pd.read_excel(obj['Body'].read(), sheet_name="BSI_Child_Aliquots", engine='openpyxl')
    parent_data = parent_data.query("`Material Type` == 'SERUM'")

    parent_data["Participant_ID"] = [i[:9] for i in parent_data["CBC_Biospecimen_Aliquot_ID"].tolist()]
    z = parent_data['Participant_ID'].value_counts()
    z = z.to_frame()
    z.reset_index(inplace=True)
    z.columns = ["Participant_ID", "Vial_Count"]

    # single_ids = parent_data.merge(z.query('Vial_Count == 1'))
    parent_data = parent_data.merge(z.query('Vial_Count > 1'))
#    parent_data = parent_data.query("`Vial Status` in ['In']")

    parent_data.sort_values(by="CBC_Biospecimen_Aliquot_ID", inplace=True)
    parent_data.drop_duplicates("Participant_ID", keep="first", inplace=True)

    child_data["Participant_ID"] = [i[:9] for i in child_data["CBC_Biospecimen_Aliquot_ID"].tolist()]
    child_data.drop_duplicates("Participant_ID", keep="first", inplace=True)

    z = parent_data.merge(child_data["Biorepository_ID"], on='Biorepository_ID',
                          how="outer", indicator=True)
    z = z.query("_merge == 'left_only'")
    z["CBC_ID"] = [i[:2] for i in z["Participant_ID"].tolist()]

    Mount_Sinai = z.query("CBC_ID == '14'")
    Minnesota = z.query("CBC_ID == '27'")
    Arizona = z.query("CBC_ID == '32'")
    Feinstein = z.query("CBC_ID == '41'")
    write_cgr_file(Mount_Sinai, "Mount_Sinai_to_CGR.xlsx", s3_client, bucket)
    write_cgr_file(Minnesota, "UMN_to_CGR.xlsx", s3_client, bucket)
    write_cgr_file(Arizona, "ASU_to_CGR.xlsx", s3_client, bucket)
    write_cgr_file(Feinstein, "Feinstein_to_CGR.xlsx", s3_client, bucket)
    print("Reports took %.2f seconds" % (time.time() - start_time))


def write_cgr_file(df, file_name, s3_client, bucket):
    df = df[~df["Vial Modifiers"].str.contains('Missing PCR Result')]
    if len(df) > 0:
        df.drop(["Participant_ID", "_merge", "CBC_ID", "Vial_Count"], axis=1, inplace=True)
        #writer = pd.ExcelWriter(f"C:\\Python_Code\\Serology_Reports\\{file_name}", engine='xlsxwriter')
        excel_buffer = df.to_excel(index=False, engine='xlsxwriter')
        s3_client.put_object(Body=excel_buffer, Bucket=bucket, Key=os.path.join("Serology_Data_Files/biorepository_id_map/Serology_Reports/", file_name))


def check_serology_submissions(s3_client, bucket, assay_data, assay_target, success_msg, error_msg, study_type):
    key = "Serology_Data_Files/serology_confirmation_test_result/"
    serology_code = '12'
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=key)
    file_name = "serology_confirmation_test_results.csv"
    for curr_serology in resp["Contents"]:
        if ("Test_Results_Passed" in curr_serology["Key"]) or ("Test_Results_Failed" in curr_serology["Key"]):
            pass
        elif ".xlsx" in curr_serology["Key"]:
            current_sub_object = Submission_Object("Serology")
            #serology_data = pd_s3.get_df_from_keys(s3_client, bucket, prefix=curr_serology["Key"], suffix="xlsx", format="xlsx", na_filter=False, output_type="pandas")
            obj = s3_client.get_object(Bucket=bucket, Key= curr_serology["Key"])
            serology_data = pd.read_excel(obj['Body'].read(), engine='openpyxl')
            data_table, drop_list = current_sub_object.validate_serology(file_name, serology_data,
                                                                         assay_data, assay_target, serology_code)
            current_sub_object = vald_rules.Validation_Rules(re, datetime, current_sub_object, data_table,
                                                  file_name, serology_code, drop_list, study_type)
            error_list = current_sub_object.Error_list
            error_count = len(error_list)
            if error_count == 0:
                print("\n## Serology File has No Errors, Submission is Valid\n")
                success_msg.append("\n## Serology File has No Errors, Submission is Valid\n")
            else:
                print(f"\n## Serology File has {error_count} Errors, Check submission\n")
                error_msg.append(f"\n## Serology File has {error_count} Errors, Check submission\n")
            return success_msg, error_msg


def populate_md5_table(pd, sql_tuple, study_type):
    convert_table = pd.read_sql(("SELECT * FROM Deidentifed_Conversion_Table"), sql_tuple[2])
    if study_type == "Refrence_Pannel":
        demo_ids = pd.read_sql(("SELECT Research_Participant_ID FROM Participant"), sql_tuple[2])
        bio_ids = pd.read_sql(("SELECT Biospecimen_ID FROM Biospecimen"), sql_tuple[2])
        aliquot_ids = pd.read_sql(("SELECT Aliquot_ID FROM Aliquot"), sql_tuple[2])
    elif study_type == "Vaccine_Response":
        pass
    all_ids = set_tables(demo_ids, "Participant_ID")
    all_ids = pd.concat([all_ids, set_tables(bio_ids, "Biospecimen_ID")])
    all_ids = pd.concat([all_ids, set_tables(aliquot_ids, "Aliquot_ID")])
    all_ids = all_ids.merge(convert_table, how="left", indicator=True)
    all_ids = all_ids.query("_merge == 'left_only'")
    if len(all_ids) > 0:  # new ids need to be added
        all_ids.drop("_merge", inplace=True, axis=1)
        all_ids["MD5_Value"] = [hashlib.md5(i.encode('utf-8')).hexdigest() for i in all_ids["ID_Value"].tolist()]
        all_ids.to_sql(name="Deidentifed_Conversion_Table", con=sql_tuple[1], if_exists="append", index=False)


def set_tables(df, id_type):
    df.columns = ["ID_Value"]
    df["ID_Type"] = id_type
    df = df[["ID_Type", "ID_Value"]]
    return df





def get_assay_data(s3_client, data_type, bucket_name):
    assay_dir = "CBC_Folders/"
    resp = s3_client.list_objects(Bucket=bucket_name, Prefix=assay_dir)
    file_list = resp['Contents']
    #print(file_list["Contents"][1])
    assay_paths = []
    path_dir = pd.DataFrame(columns=["Dir_Path", "File_Name", "Date_Created", "Date_Modified"])
    for file in file_list:
        if (file['Key'].endswith(".xlsx")) and ("20210402" not in file['Key']):
            file_path = file['Key']
            assay_paths.append(file_path)
            created = file['LastModified']
            modified =  file['LastModified']
            path_dir.loc[len(path_dir.index)] = [os.path.dirname(file['Key']), os.path.basename(file['Key']), created, modified]

    if data_type == "CBC_Data":
        all_assay_data = pd.DataFrame()
        all_target_data = pd.DataFrame()
        all_qc_data = pd.DataFrame()
        converion_file = pd.DataFrame()

        uni_path = list(set(path_dir["Dir_Path"]))
        for curr_path in uni_path:
            curr_folder = path_dir.query("Dir_Path == @curr_path")
            assay_file = curr_folder[curr_folder["File_Name"].apply(lambda x: 'assay' in x and "assay_qc" not in x
                                                                    and "assay_target" not in x)]
            all_assay_data = populate_df(all_assay_data, assay_file, bucket_name, s3_client)

            assay_file = curr_folder[curr_folder["File_Name"].apply(lambda x: "assay_qc" in x)]
            all_qc_data = populate_df(all_qc_data, assay_file, bucket_name, s3_client)

            assay_file = curr_folder[curr_folder["File_Name"].apply(lambda x: "assay_target_antigen" in x or
                                                                    "assay_target" in x)]
            all_target_data = populate_df(all_target_data, assay_file, bucket_name, s3_client)

            assay_file = curr_folder[curr_folder["File_Name"].apply(lambda x: "Assay_Target_Organism_Conversion.xlsx" in x)]
            converion_file = populate_df(converion_file, assay_file, bucket_name, s3_client)

        if len(all_assay_data) > 0:
            all_assay_data = box_clean_up_tables(all_assay_data, '[0-9]{2}[_]{1}[0-9]{3}$')
        if len(all_target_data) > 0:
            all_target_data = box_clean_up_tables(all_target_data, '[0-9]{2}[_]{1}[0-9]{3}$')
        if len(all_qc_data) > 0:
            all_qc_data = box_clean_up_tables(all_qc_data, '[0-9]{2}[_]{1}[0-9]{3}$')

        return all_assay_data, all_target_data, all_qc_data, converion_file
    elif data_type == "Validation":
        #print("x")
        assay_file = assay_dir + "Seronet_Reference_Panels/"+ "validation_panel_assays.xlsx"
        #curr_data = pd.read_excel(assay_file, na_filter=False, engine='openpyxl')
        obj = s3_client.get_object(Bucket=bucket_name, Key=assay_file)
        curr_data = pd.read_excel(obj['Body'].read(), na_filter=False, engine='openpyxl')
        return curr_data


def populate_df(curr_assay, assay_file, bucket_name, s3_client):
    if len(assay_file):
        curr_data = assay_file[assay_file["Date_Modified"] == max(assay_file["Date_Modified"])]
        file_path = os.path.join(curr_data["Dir_Path"].tolist()[0], curr_data["File_Name"].tolist()[0])
        #curr_data = pd.read_excel(file_path, na_filter=False, engine='openpyxl')
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_path)
        curr_data = pd.read_excel(obj['Body'].read(), na_filter=False, engine='openpyxl')
        curr_assay = pd.concat([curr_assay, curr_data])
    return curr_assay


def box_clean_up_tables(curr_table, ptrn_str):
    curr_table = curr_table[curr_table["Assay_ID"].apply(lambda x: re.compile(ptrn_str).match(str(x)) is not None)]
    curr_table = curr_table.dropna(axis=0, how="all", subset=None)
    if len(curr_table) > 0:
        missing_logic = curr_table.eq(curr_table.iloc[:, 0], axis=0).all(axis=1)
        curr_table = curr_table[[i is not True for i in missing_logic]]
        curr_table = curr_table.loc[:, ~curr_table .columns.str.startswith('Unnamed')]
        curr_table = curr_table.replace('–', '-')
        curr_table.columns = [i.replace("Assay_Target_Antigen", "Assay_Target") for i in curr_table.columns]
        curr_table.columns = [i.replace("lavage", "Lavage") for i in curr_table.columns]
    return curr_table



########################

def get_template_data(s3_client, study_name, bucket):
    if study_name == 'Refrence_Pannel':
        #template_dir = (box_dir + file_sep + "CBC Data Submission Documents" + file_sep + "Reference Panel Data Submission Templates")
        template_df = get_template_columns(s3_client, bucket, template_dir)
        dbname = "seronetdb-Validated"  # name of the SQL database where data is saved
    elif study_name == 'Vaccine_Response':
        #template_dir = (box_dir + file_sep + "CBC Data Submission Documents" + file_sep + "Vaccine_Response_Study_Templates" + file_sep + "Data_Submission_Templates")
        template_dir = "Data_Submissions_Need_To_Validate/Vaccine_Response_Study_Templates/"
        template_df = get_template_columns(s3_client, bucket, template_dir)
        dbname = "seronetdb-Vaccine_Response"  # name of the SQL database where data is saved
    else:
        template_df = []
    return template_df, dbname


def get_template_columns(s3_client,bucket_name, template_dir):
    template_data = {}

    resp = s3_client.list_objects(Bucket=bucket_name, Prefix=template_dir)
    file_list = resp['Contents']
    for file in file_list:
        if "Deprecated" in file['Key'] or "~" in file['Key'] or "$" in file['Key'] or "vaccine_response_data_model.xlsx" in file['Key']:
            pass
        elif file['Key'].endswith(".xlsx") or file['Key'].endswith(".xlsm"):
            file_path = file['Key']
            obj = s3_client.get_object(Bucket=bucket_name, Key=file_path)
            curr_data = pd.read_excel(obj['Body'].read(), na_filter=False, engine='openpyxl')
            curr_data.drop([i for i in curr_data.columns if "Unnamed" in i], axis=1, inplace=True)
            template_data[os.path.basename(file['Key'])] = {"Data_Table": curr_data}

    col_list = []
    sheet_name = []
    for file in template_data:
        sheet_name = sheet_name + [file]*len(template_data[file]["Data_Table"].columns.tolist())
        col_list = col_list + template_data[file]["Data_Table"].columns.tolist()
    return pd.DataFrame({"Sheet_Name": sheet_name, "Column_Name": col_list})


def get_summary_file():
    summary_file = pd.DataFrame(columns=["Submission_Status", "Date_Of_Last_Status", "Folder_Location",
                                            "CBC_Num", "Date_Timestamp", "Submission_Name", "Validation_Status"])
    return summary_file




def set_up_function(bucket):
    warnings.simplefilter("ignore")

    file_sep = os.path.sep
    eastern = dateutil.tz.gettz("US/Eastern")
    validation_date = datetime.datetime.now(tz=eastern).strftime("%Y-%m-%d")
    pd.options.mode.chained_assignment = None  # default='warn'

    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    cbc_codes = get_cbc_file("Data_Submissions_Need_To_Validate/12 SeroNet Data Submitter Information", s3_client, bucket)
    print(cbc_codes)
    return file_sep, s3_client, s3_resource, cbc_codes, validation_date

def get_cbc_file(cbc_folder_name, s3_client, bucket):
    file_path = []
    resp = s3_client.list_objects(Bucket=bucket, Prefix=cbc_folder_name)
    file_list = resp['Contents']
    for f in file_list:
        if (f['Key'].endswith(".xlsx")):
            file_path.append(f['Key'])
    return file_path



def get_sql_dict_ref(s3_client, bucket):
    sql_table_dict = {}
    obj = s3_client.get_object(Bucket=bucket, Key= "SQL_Dict/sql_dict_ref.yaml")
    sql_table_dict = yaml.safe_load(obj['Body'])
    '''
    sql_table_dict["assay_data.csv"] = ["Assay_Metadata", "Assay_Calibration", "Assay_Bio_Target"]
    sql_table_dict["assay_target.csv"] = ["Assay_Target"]
    sql_table_dict["assay_qc.csv"] = ["Assay_Quality_Controls"]
    sql_table_dict["assay_conversion.csv"] = ["Assay_Organism_Conversion"]
    sql_table_dict["validation_assay_data.csv"] = ["Validation_Panel_Assays"]
    sql_table_dict["aliquot.csv"] = ["Tube", "Aliquot"]
    sql_table_dict["biospecimen.csv"] = ["Tube", "Biospecimen"]
    sql_table_dict["confirmatory_clinical_test.csv"] = ["Confirmatory_Clinical_Test"]
    sql_table_dict["demographic.csv"] = ["Participant"]  # , "Prior_Covid_Outcome", "Participant_Comorbidity_Reported", "Comorbidity"]
    sql_table_dict["prior_clinical_test.csv"] = ["Participant_Prior_SARS_CoV2_PCR"]  # , "Participant_Prior_Infection_Reported"]
    sql_table_dict["consumable.csv"] = ["Consumable", "Biospecimen_Consumable"]
    sql_table_dict["reagent.csv"] = ["Reagent", "Biospecimen_Reagent"]
    sql_table_dict["equipment.csv"] = ["Equipment", "Biospecimen_Equipment"]
    sql_table_dict["secondary_confirmation_test_result.csv"] = ["Secondary_Confirmatory_Test"]
    sql_table_dict["bsi_child.csv"] = ["BSI_Child_Aliquots"]
    sql_table_dict["bsi_parent.csv"] = ["BSI_Parent_Aliquots"]
    sql_table_dict["submission.csv"] = ["Submission"]
    sql_table_dict["shipping_manifest.csv"] = ["Shipping_Manifest"]
    sql_table_dict["CDC_Data.csv"] = ["CDC_Confrimation_Results"]
    sql_table_dict["Blinded_Evaluation_Panels.csv"] = ["Blinded_Validation_Test_Results"]
    '''
    return sql_table_dict


def get_sql_dict_vacc(s3_client, bucket):
    sql_table_dict = {}
    sql_table_dict = {}
    obj = s3_client.get_object(Bucket=bucket, Key= "SQL_Dict/sql_dict_vacc.yaml")
    sql_table_dict = yaml.safe_load(obj['Body'])

    '''
    visit_list = ["Participant_Visit_Info", "Participant", "Specimens_Collected", "Participant_Comorbidities", "Comorbidities_Names",
                  "Participant_Other_Conditions", "Participant_Other_Condition_Names", "Drugs_And_Alcohol_Use", "Non_SARS_Covid_2_Vaccination_Status"]
    sql_table_dict["baseline.csv"] = visit_list
    sql_table_dict["follow_up.csv"] = [i for i in visit_list if i != "Participant"]  # Participant table does not exist in follow up
    sql_table_dict["aliquot.csv"] = ["Tube", "Aliquot"]
    sql_table_dict["biospecimen.csv"] = ["Tube", "Biospecimen"]
    sql_table_dict["assay_data.csv"] = ["Assay_Metadata", "Assay_Calibration", "Assay_Bio_Target"]
    sql_table_dict["assay_target.csv"] = ["Assay_Target"]
    sql_table_dict["assay_qc.csv"] = ["Assay_Quality_Controls"]
    sql_table_dict["assay_conversion.csv"] = ["Assay_Organism_Conversion"]
    sql_table_dict["study_design.csv"] = ["Study_Design"]
    sql_table_dict["consumable.csv"] = ["Consumable", "Biospecimen_Consumable"]
    sql_table_dict["reagent.csv"] = ["Reagent", "Biospecimen_Reagent"]
    sql_table_dict["equipment.csv"] = ["Equipment", "Biospecimen_Equipment"]
    sql_table_dict["shipping_manifest.csv"] = ["Shipping_Manifest"]
    sql_table_dict["covid_history.csv"] = ["Covid_History"]
    sql_table_dict["covid_hist_sql.csv"] = ["Covid_History"]
    sql_table_dict["covid_vaccination_status.csv"] = ["Covid_Vaccination_Status"]
    sql_table_dict["vac_status_sql.csv"] = ["Covid_Vaccination_Status"]
    sql_table_dict["biospecimen_test_result.csv"] = ["Biospecimen_Test_Results"]
    sql_table_dict["test_results_sql.csv"] = ["Biospecimen_Test_Results"]
    sql_table_dict["treatment_history.csv"] = ["Treatment_History"]
    sql_table_dict["autoimmune_cohort.csv"] = ["AutoImmune_Cohort"]
    sql_table_dict["cancer_cohort.csv"] = ["Cancer_Cohort"]
    sql_table_dict["hiv_cohort.csv"] = ["HIV_Cohort"]
    sql_table_dict["organ_transplant_cohort.csv"] = ["Organ_Transplant_Cohort"]
    sql_table_dict["visit_info_sql.csv"] = ["Participant_Visit_Info"]
    sql_table_dict["submission.csv"] = ["Submission"]
    sql_table_dict["bsi_child.csv"] = ["BSI_Child_Aliquots"]
    sql_table_dict["bsi_parent.csv"] = ["BSI_Parent_Aliquots"]
    '''

    return sql_table_dict


def connect_to_sql_db(host_client, user_name, user_password, file_dbname):

    sql_column_df = pd.DataFrame(columns=["Table_Name", "Column_Name", "Var_Type", "Primary_Key", "Autoincrement",
                                          "Foreign_Key_Table", "Foreign_Key_Column"])
    creds = {'usr': user_name, 'pwd': user_password, 'hst': host_client, "prt": 3306, 'dbn': file_dbname}
    connstr = "mysql+mysqlconnector://{usr}:{pwd}@{hst}:{prt}/{dbn}"
    engine = sd.create_engine(connstr.format(**creds))
    engine = engine.execution_options(autocommit=False)
    conn = engine.connect()
    metadata = sd.MetaData()
    metadata.reflect(engine)

    for t in metadata.tables:
        try:
            curr_table = metadata.tables[t]
            curr_table = curr_table.columns.values()
            for curr_row in range(len(curr_table)):
                curr_dict = {"Table_Name": t, "Column_Name": str(curr_table[curr_row].name),
                             "Var_Type": str(curr_table[curr_row].type),
                             "Primary_Key": str(curr_table[curr_row].primary_key),
                             "Autoincrement": False,
                             "Foreign_Key_Count": 0,
                             "Foreign_Key_Table": 'None',
                             "Foreign_Key_Column": 'None'}
                curr_dict["Foreign_Key_Count"] = len(curr_table[curr_row].foreign_keys)
                if curr_table[curr_row].autoincrement is True:
                    curr_dict["Autoincrement"] = True
                if len(curr_table[curr_row].foreign_keys) == 1:
                    key_relation = list(curr_table[curr_row].foreign_keys)[0].target_fullname
                    key_relation = key_relation.split(".")
                    curr_dict["Foreign_Key_Table"] = key_relation[0]
                    curr_dict["Foreign_Key_Column"] = key_relation[1]


                sql_column_df = pd.concat([sql_column_df, pd.DataFrame.from_records([curr_dict])])
        except Exception as e:
            display_error_line(e)
    print("## Sucessfully Connected to " + file_dbname + " ##")
    sql_column_df.reset_index(inplace=True, drop=True)
    return sql_column_df, engine, conn

def to_s3_csv(file_key, df, index_bool, bucket, s3_client):
    csv_buffer = df.to_csv(index=index_bool).encode()
    s3_client.put_object(Body=csv_buffer, Bucket=bucket, Key=file_key)

def write_to_slack(message_slack, slack_chanel):
    http = urllib3.PoolManager()
    data={"text": message_slack}
    r=http.request("POST", slack_chanel, body=json.dumps(data), headers={"Content-Type":"application/json"})

def move_submission(curr_bucket, new_bucket, file_path, s3_client, s3_resource, site_name, curr_cbc, study_type):
    # curr_bucket = seronet-demo-cbc-destination
    # file_path = "Accrual_Need_To_Validate/cbc02/2023-05-09-10-27-51/submission_007_accrual_submission_5_9_23.zip/"
    print(curr_bucket)
    #study_type = "Refrence_Pannel"
    if study_type == "Vaccine_Response":
        study_sub_folder = "Vaccine Response Submissions/"
    else:
        study_sub_folder = "Reference Panel Submissions/"
    "Reference Panel Submissions/"
    all_files = s3_client.list_objects_v2(Bucket=curr_bucket, Prefix=os.path.dirname(os.path.split(file_path)[0]))["Contents"]
    all_files = [i["Key"] for i in all_files]

    for curr_key in all_files:
        new_key = curr_key.replace("Data_Submissions_Need_To_Validate", site_name)
        new_key = new_key.replace(curr_cbc+'/', '')
        new_key = study_sub_folder + new_key
        source = {'Bucket': curr_bucket, 'Key': curr_key}               # files to copy
        try:
            s3_resource.meta.client.copy(source, new_bucket, new_key)
            print(f"atempting to delete {curr_bucket} / {curr_key}")
            #s3_client.delete_object(Bucket=curr_bucket, Key=curr_key)
        except Exception as error:
            print('Error Message: {}'.format(error))


def send_error_email(ssm, file_name, error_list, email_msg):
    http = urllib3.PoolManager()
    USERNAME_SMTP = ssm.get_parameter(Name="USERNAME_SMTP", WithDecryption=True).get("Parameter").get("Value")
    PASSWORD_SMTP = ssm.get_parameter(Name="PASSWORD_SMTP", WithDecryption=True).get("Parameter").get("Value")
    HOST = "email-smtp.us-east-1.amazonaws.com"
    PORT = 587

    try:
        RECIPIENT_RAW = ssm.get_parameter(Name="Shipping_Manifest_Recipents", WithDecryption=True).get("Parameter").get("Value")
        RECIPIENT = RECIPIENT_RAW.replace(" ", "")
        RECIPIENT_LIST = RECIPIENT.split(",")
        SUBJECT = f'Data Submission Feedback: {file_name}'
        SENDERNAME = 'SeroNet Data Team (Data Curation)'
        SENDER = ssm.get_parameter(Name="sender-email", WithDecryption=True).get("Parameter").get("Value")

        for recipient in RECIPIENT_LIST:
            print(recipient)
            msg_text = ""
            msg_text += "Your accrual submission has been analyzed by the validation software. \n"
            msg_text += email_msg

            msg = MIMEMultipart('alternative')
            msg['Subject'] = SUBJECT
            msg['From'] = email.utils.formataddr((SENDERNAME, SENDER))
            if len(error_list) > 0:
                #msg.attach(attachment)
                #msg_text += f"\n\nAn Error file was created and attached to this email"
                msg_text += f"\nLet me know if you have any questions\n"
            msg['To'] = recipient
            part1 = MIMEText(msg_text, "plain")
            msg.attach(part1)

            send_email_func(HOST, PORT, USERNAME_SMTP, PASSWORD_SMTP, SENDER, recipient, msg)
            print("email has been sent")

    except Exception as e:
        print(e)
        #data={"text": display_error_line(e)}
        #r=http.request("POST", slack_fail, body=json.dumps(data), headers={"Content-Type":"application/json"})

def send_email_func(HOST, PORT, USERNAME_SMTP, PASSWORD_SMTP, SENDER, recipient, msg):
    server = smtplib.SMTP(HOST, PORT)
    server.ehlo()
    server.starttls()
    #stmplib docs recommend calling ehlo() before & after starttls()
    server.ehlo()
    server.login(USERNAME_SMTP, PASSWORD_SMTP)

    server.sendmail(SENDER, recipient, msg.as_string())
    #print(msg)
    server.close()
