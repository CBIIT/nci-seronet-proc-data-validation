import icd10
import pandas as pd
from dateutil.parser import parse
import datetime
from itertools import compress
from collections import Counter
import boto3
import os
######################################################################################################################


def clean_up_column_names(header_name):
    header_name = header_name.replace(" (cells/mL)", "")
    header_name = header_name.replace(" (mL)", "")
    header_name = header_name.replace(" (Years)", "")
    header_name = header_name.replace(" (Days)", "")
    header_name = header_name.replace(" (min)", "")
    header_name = header_name.replace(" (hrs)", "")
    header_name = header_name.replace("°C", "")
    header_name = header_name.replace("-80", "80")
    header_name = header_name.replace("-", "_")
    return header_name


def convert_data_type(v):
    if isinstance(v, (datetime.datetime, datetime.time, datetime.date)):
        return v
    if str(v).find('_') > 0:
        return v
    try:
        float(v)
        if (float(v) * 10) % 10 == 0:
            return int(float(v))
        return float(v)
    except ValueError:
        try:
            v = parse(v)
            return v
        except ValueError:
            return v
        except TypeError:
            str(v)


def check_multi_rule(data_table, depend_col, depend_val):
    try:
        if len(data_table) == 0:
            error_str = depend_col + " is not found, unable to validate Data "
            return data_table, error_str
        if depend_col not in data_table.columns.to_list():
            error_str = depend_col + " is not found, unable to validate Data "
            data_table = -1
            return data_table, error_str
        if depend_val == "> 0":
            data_table.replace("More than 2", 100, inplace=True)
            data_table = data_table[data_table[depend_col].apply(lambda x: x > 0)]
            error_str = depend_col + " is a Number greater then 0"
        if depend_val == "Is A Number":
            data_table = data_table[data_table[depend_col].apply(lambda x: isinstance(x, (float, int)))]
            error_str = depend_col + " is a Number "
        elif depend_val == "Is A Date":
            data_table = data_table[data_table[depend_col].apply(lambda x: isinstance(x, pd.Timestamp))]
            error_str = depend_col + " is a Date "
        elif depend_val in ["Not Other", "Not Unvaccinated"]:
            error_str = ""
        else:
            valid_data = data_table[depend_col].apply(lambda x: [i for i in str(x).split("|") if i in depend_val])
            data_table = data_table.loc[valid_data[[len(i) > 0 for i in valid_data]].index.tolist()]
#            data_table = data_table.query("`{0}` in @depend_val".format(depend_col))
            error_str = depend_col + " is " + str(depend_val) + " "
    except Exception as e:
        print(e)
    return data_table, error_str


def check_if_col(curr_table, col_name, ids_df):
    if col_name in curr_table.columns and len(ids_df) > 0:
        id_list = ids_df[col_name].tolist()
        curr_table = curr_table.query("{0} in @id_list".format(col_name))
    return curr_table


def display_error_line(ex):
    trace = []
    tb = ex.__traceback__
    while tb is not None:
        trace.append({"filename": tb.tb_frame.f_code.co_filename,
                      "name": tb.tb_frame.f_code.co_name,
                      "lineno": tb.tb_lineno})
        tb = tb.tb_next
    print(str({'type': type(ex).__name__, 'message': str(ex), 'trace': trace}))
######################################################################################################################


class Submission_Object:
    def __init__(self, file_name):
        """An Object that contains information for each Submitted File that Passed File_Validation."""
        self.File_Name = file_name
        self.Data_Object_Table = {}
        self.All_Part_ids = []
        self.All_Bio_ids = []
        self.All_Ali_ids = []
        self.Template_Cols = []
        self.sql_col_df = []
        self.sql_table_dict = []
        self.Data_Validation_Path = []
        self.dup_visits = []
        self.bucket = ""
        self.s3_client = boto3.client('s3')

        self.Column_error_count = pd.DataFrame(columns=["Message_Type", "CSV_Sheet_Name", "Column_Name", "Error_Message"])
        self.Curr_col_errors = pd.DataFrame(columns=["Message_Type", "CSV_Sheet_Name", "Column_Name", "Error_Message"])
        self.Error_list = pd.DataFrame(columns=["Message_Type", "CSV_Sheet_Name", "Row_Index", "Column_Name",
                                                "Column_Value", "Error_Message"])

####################################################################################################
    def initalize_parms(self, curr_file, template_df, sql_col_df, sql_table_dict, bucket, s3_client):
        self.Template_Cols = template_df
        self.sql_col_df = sql_col_df
        self.sql_table_dict = sql_table_dict
        self.Data_Validation_Path = os.path.join(curr_file, "Data_Validation_Results")
        self.s3_client = s3_client
        self.bucket = bucket
        self.remove_validation()  # if Data_Validation already exists, remove folder

    def remove_validation(self):
        if self.check_s3_folder_exists(self.Data_Validation_Path):
            self.delete_s3_folder(self.Data_Validation_Path)

    def check_validation_folder(self):
        if self.check_s3_folder_exists(self.Data_Validation_Path):
            pass  # if folder exist do nothing
        else:
            self.create_s3_folder(self.Data_Validation_Path)

    def create_s3_folder(self, folder_key):
        response = self.s3_client.put_object(Bucket=self.bucket, Key=folder_key + '/')
        print(response)

    def delete_s3_folder(self, folder_key):
        response = self.s3_client.delete_object(Bucket=self.bucket, Key=folder_key)
        print(response)

    def check_s3_folder_exists(self, folder_key):
        response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=folder_key)
        if 'Contents' in response:
            return True
        return False

    def get_data_tables(self, file_path, study_type):
        file_name = os.path.basename(file_path)
        file_name = file_name.replace(".xlsx", ".csv")
        self.Data_Object_Table[file_name] = {"Data_Table": [], "Column_List": [], "Key_Cols": []}
        try:
            if ".csv" in file_path:
                resp = self.s3_client.get_object(Bucket=self.bucket, Key=file_path)
                Data_Table = pd.read_csv(resp['Body'], na_filter=False)
            elif ".xlsx" in file_path:
                resp = self.s3_client.get_object(Bucket=self.bucket, Key=file_path)
                Data_Table = pd.read_excel(resp['Body'].read(), na_filter=False, engine='openpyxl')
            else:
                print("Unknown file extension")
                Data_Table = pd.DataFrame()
        except Exception:
            Data_Table = pd.DataFrame()
        self.Data_Object_Table[file_name]["Data_Table"].append(Data_Table)
        if file_name not in ["submission.csv"]:
            self.cleanup_table(file_name)
        self.set_key_cols(file_name, study_type)

    def set_key_cols(self, file_name, study_type):  # sql primary keys?
        if file_name in self.sql_table_dict:
            table_name = self.sql_table_dict[file_name]
            col_list = self.sql_col_df.query("Table_Name == @table_name and Primary_Key == 'True'")
            col_list = col_list["Column_Name"].tolist()
        else:
            col_list = ["Biospecimen_ID"]
        col_list = [i.replace("Test_Result", "SARS_CoV_2_PCR_Test_Result") for i in col_list]
        if "Visit_Info_ID" in col_list and file_name not in ["vist_info_sql.csv"]:
            col_list.remove("Visit_Info_ID")
            try:
                curr_cols = z = self.Data_Object_Table[file_name]["Data_Table"][0].columns
            except Exception:
                curr_cols = z = self.Data_Object_Table[file_name]["Data_Table"].columns
            if "Research_Participant_ID" in curr_cols:
                col_list = col_list + ["Research_Participant_ID"]
            if "Visit" in curr_cols:
                col_list = col_list + ["Visit"]
            if "Visit_Number" in curr_cols:
                col_list = col_list + ["Visit_Number"]
        self.Data_Object_Table[file_name]["Key_Cols"] = col_list
        if (study_type == "Vaccine_Response"):
            if "Column_List" in self.Data_Object_Table[file_name]:
                self.add_Visit_ID(file_name)

    def cleanup_table(self, file_name):
        try:
            curr_table = self.Data_Object_Table[file_name]["Data_Table"][0]
        except Exception as e:
            print(e)
        curr_table.dropna(axis=0, how="all", subset=None, inplace=True)
        curr_table = self.remove_blank_rows(curr_table, "Research_Participant_ID")
        curr_table = self.remove_blank_rows(curr_table, "Biospecimen_ID")
        curr_table = self.remove_blank_rows(curr_table, "Aliquot_ID")

        if len(curr_table) > 0:
            missing_logic = curr_table.eq(curr_table.iloc[:, 0], axis=0).all(axis=1)
            curr_table = curr_table[[i is not True for i in missing_logic]]
            curr_table = curr_table .loc[:, ~curr_table .columns.str.startswith('Unnamed')]
            for iterC in curr_table.columns:
                try:
                    curr_table[iterC] = curr_table[iterC].apply(lambda x: x.replace('–', '-'))
                except Exception:
                    pass
        self.Data_Object_Table[file_name]["File_Size"] = len(curr_table)
        self.Data_Object_Table[file_name]["Data_Table"] = curr_table

    def remove_blank_rows(self, curr_table, test_str):
        if test_str in curr_table.columns:
            curr_table = curr_table.query("{0} not in ['']".format(test_str))
        return curr_table

    def create_visit_table(self, curr_table, study_type):
        if curr_table not in self.Data_Object_Table:
            return
        if study_type == "Vaccine_Response":
            if "visit_info_sql.csv" in self.Data_Object_Table:
                visit_info = self.Data_Object_Table["visit_info_sql.csv"]["Data_Table"]
            else:
                self.Data_Object_Table["visit_info_sql.csv"] = {"Data_Table": []}
                visit_info = pd.DataFrame(columns=["Research_Participant_ID", "Visit_Info_ID", "Visit_Number", "Visit_Type"])

            if (len(self.Data_Object_Table[curr_table]["Data_Table"].columns) == 1 and
               "Visit_Info_ID" in self.Data_Object_Table[curr_table]["Data_Table"].columns):
                visit_info = self.Data_Object_Table[curr_table]["Data_Table"]
                self.Data_Object_Table["visit_info_sql.csv"]["Data_Table"] = visit_info.drop_duplicates()
            else:
                if curr_table in self.Data_Object_Table:
                    base_line = self.Data_Object_Table[curr_table]["Data_Table"]
                    base_line.rename(columns={"Visit": "Visit_Number"}, inplace=True)
                    if curr_table == "baseline.csv":
                        base_line["Visit_Number"] = 1
                        base_line["Visit_Type"] = "Baseline"
                    else:
                        base_line["Visit_Type"] = "Follow_Up"
                        base_line["Visit_Number"] = [i[-2:] if i[-1] in ["A", "B", "C", "D"] else int(i[-2:]) for i in base_line["Visit_Info_ID"]]
                    visit_info = visit_info.merge(base_line[["Research_Participant_ID", "Visit_Info_ID",
                                                             "Visit_Number", "Visit_Type"]], how="outer")
                    self.Data_Object_Table["visit_info_sql.csv"]["Data_Table"] = visit_info.drop_duplicates()

    def correct_var_types(self, file_name, study_type):
        data_table = self.Data_Object_Table[file_name]['Data_Table']
        col_names = data_table.columns
        for curr_col in col_names:
            if (("Batch_ID" in curr_col) or ("Catalog_Number" in curr_col) or ("Lot_Number" in curr_col) or
               (curr_col in ["Visit", "Visit_Number", "Derived_Result", "Equipment_ID", "Instrument_ID",
                             "Other_Comorbidity"])):
                data_table[curr_col] = [str(i) for i in data_table[curr_col]]
            else:
                try:
                    data_table[curr_col] = [convert_data_type(c) for c in data_table[curr_col]]
                except Exception as e:
                    print(e)
                    display_error_line(e)
        data_table.columns = col_names
        return data_table

######################################################################################################
    def add_Visit_ID(self, file_name):
        if "_sql" in file_name:
            return
        try:
            data_table = self.Data_Object_Table[file_name]["Data_Table"][0]
        except KeyError:
            data_table = self.Data_Object_Table[file_name]["Data_Table"]
        if "Research_Participant_ID" not in data_table.columns:
            return data_table
        if file_name == 'baseline.csv':
            visit_id = [i + " : " + "B01" for i in data_table["Research_Participant_ID"]]
        else:
            if "Visit" in data_table.columns:
                extend = ["B01" if i == "Baseline(1)" else "F" + str(i).zfill(2) for i in data_table["Visit"]]
            elif "Visit_Number" in data_table.columns:
                extend = ["B01" if i == "Baseline(1)" else "F" + str(i).zfill(2) for i in data_table["Visit_Number"]]
            else:
                extend = "unkonwn"
            visit_id = [a + " : " + b for a, b in zip(data_table["Research_Participant_ID"], extend)]
        if len(visit_id) != len(data_table):
            return
        data_table["Visit_Info_ID"] = visit_id
        return data_table

    def merge_tables(self, file_name):
        self_table = self.Data_Object_Table
        curr_table = self_table[file_name]["Data_Table"]
        curr_table.rename(columns={"Target_Organism": "Assay_Target_Organism"}, inplace=True)
        self_table[file_name]["Column_List"] = curr_table.columns
        if file_name in ["submission.csv", "shipping_manifest.csv", "baseline_visit_date.csv"]:
            return curr_table, []

        key_cols = self_table[file_name]["Key_Cols"]
        key_cols = [i.replace("Consumable_", "") for i in key_cols]
        key_cols = [i.replace("Equipment_", "") for i in key_cols]
        key_cols = [i.replace("Reagent_", "") for i in key_cols]

        for test_table in self_table:
            if test_table in ["submission.csv"]:
                continue
            try:
                test_logic = [i for i in key_cols if i in self_table[test_table]["Data_Table"].columns]
                if len(test_logic) > 0:
                    merge_table = self_table[test_table]["Data_Table"]
                    key_cols = self_table[test_table]["Key_Cols"]
                    key_cols = [i for i in key_cols if i in merge_table.columns]
                    merge_table = merge_table[key_cols]
                    merge_table.rename(columns={"Target_Organism": "Assay_Target_Organism"}, inplace=True)
                    curr_table = curr_table.merge(merge_table, how='left')
                    curr_table.drop_duplicates(inplace=True)
            except Exception as e:
                pass
#                print(i + " was not found in the schema")
        drop_list = [i for i in curr_table.columns if i not in self_table[file_name]["Column_List"]]
        return curr_table, drop_list

    def check_tables(self, table_name, merge_data):
        if table_name in self.Data_Object_Table:
            merge_col = self.Data_Object_Table[table_name]["Key_Cols"]
            if len(merge_data) == 0:
                return self.Data_Object_Table[table_name]["Data_Table"][merge_col]
            else:
                return pd.concat([merge_data, self.Data_Object_Table[table_name]["Data_Table"][merge_col]])
        else:
            return merge_data

######################################################################################################
    def get_submission_metadata(self, Support_Files):
        study_name = "Unknown_Study"
        if "submission.csv" not in self.Data_Object_Table:
            print("Submission File was not included in the list of files to validate")
        else:
            try:
                submit_table = self.Data_Object_Table['submission.csv']['Data_Table'][0]
                id_list = [i for i in Support_Files if "SeroNet_Org_IDs.xlsx" in i]
                resp = self.s3_client.get_object(Bucket=self.bucket, Key=id_list[0])
                id_conv = pd.read_excel(resp['Body'].read(), na_filter=False, engine='openpyxl')
                submit_name = submit_table.columns.values[1]
                #ref_test = submit_table.query("`Submitting Center` in ['confirmatory_clinical_test.csv']")
                #vac_test = submit_table.query("`Submitting Center` in ['biospecimen_test_result.csv']")
                self.Intent = submit_table.query("`Submitting Center` == 'Submission Intent'").iloc[0, 1]
                '''
                if len(ref_test) == 1:
                    study_name = "Refrence_Pannel"
                elif len(vac_test) == 1:
                    study_name = "Vaccine_Response"
                '''
                study_name = "Vaccine_Response"
                self.CBC_ID = id_conv.query("Institution == @submit_name")["Org ID"].tolist()[0]
                self.Submitted_Name = submit_name
                if len(str(self.CBC_ID)) == 0:
                    self.CBC_ID = -1
                self.Submit_Participant_IDs = self.Data_Object_Table['submission.csv']['Data_Table'][0].iloc[1][1]
                self.Submit_Biospecimen_IDs = self.Data_Object_Table['submission.csv']['Data_Table'][0].iloc[2][1]
            except Exception as e:
                print(e)
                display_error_line(e)
                self.CBC_ID = -1
                self.Submit_Participant_IDs = "00"
                self.Submit_Biospecimen_IDs = "00"
            if len(self.Submit_Participant_IDs) == 0:
                self.Submit_Participant_IDs = "0"
            if len(self.Submit_Biospecimen_IDs) == 0:
                self.Submit_Biospecimen_IDs = "0"
        if self.CBC_ID > 0:
            print("The CBC Code for " + self.Submitted_Name + " Is: " + str(self.CBC_ID) + "\n")
        else:
            print("The Submitted CBC name: " + self.Submitted_Name + " does NOT exist in the Database")
        return study_name

    def column_validation(self, file_name, Support_Files):
        template_data = self.Template_Cols
        file_name = file_name.replace(".xlsx", ".csv")
        template_data["Sheet_Name"] = [i.replace(".xlsx", ".csv") for i in template_data["Sheet_Name"]]
        template_data["Sheet_Name"] = [i.replace(".xlsm", ".csv") for i in template_data["Sheet_Name"]]

        if file_name in ["submission.csv"]:
            return
        header_list = self.Data_Object_Table[file_name]['Data_Table'].columns.tolist()
        check_file = template_data.query("Sheet_Name == @file_name")
        self.Data_Object_Table[file_name]['Data_Table'].columns = header_list

        if len(check_file) == 0:
            return
        col_list = check_file["Column_Name"].tolist()
        if "Vaccination_Lot_Number" in col_list:
            col_list.remove("Vaccination_Lot_Number")

        in_csv_not_excel = [i for i in header_list if i not in col_list]
        in_excel_not_csv = [i for i in col_list if i not in header_list]
        if "Visit_Info_ID" in in_csv_not_excel:
            in_csv_not_excel.remove("Visit_Info_ID")

        csv_errors = ["Column Found in CSV is not Expected"] * len(in_csv_not_excel)
        excel_errors = ["This Column is Expected and is missing from CSV File"] * len(in_excel_not_csv)
        name_list = [file_name] * (len(in_csv_not_excel) + len(in_excel_not_csv))

        if len(name_list) > 0:
            self.Curr_col_errors["Message_Type"] = ["Error"]*len(name_list)
            self.Curr_col_errors["CSV_Sheet_Name"] = name_list
            self.Curr_col_errors["Column_Name"] = (in_csv_not_excel + in_excel_not_csv)
            self.Curr_col_errors["Error_Message"] = (csv_errors+excel_errors)
            #self.Column_error_count = self.Column_error_count.append(self.Curr_col_errors)
            self.Column_error_count = pd.concat([self.Column_error_count, self.Curr_col_errors], ignore_index=True)
            self.Curr_col_errors.drop(labels=range(0, len(name_list)), axis=0, inplace=True)

######################################################################################################
    def validate_serology(self, file_name, serology_data, assay_data, assay_target, serology_id):
        self.update_object(serology_data, file_name)
        self.update_object(assay_data, "assay.csv")
        self.update_object(assay_target, "assay_target.csv")
        data_table = self.correct_var_types(file_name, "Serology")
        data_table, drop_list = self.merge_tables(file_name)

        self.CBC_ID = serology_id
        return data_table, drop_list

    def update_object(self, assay_df, file_name):
        Data_Table = assay_df.rename(columns={"Assay Target": "Assay_Target"})
        self.Data_Object_Table[file_name] = {"Data_Table": [], "Column_List": [], "Key_Cols": []}
        self.Data_Object_Table[file_name]["Data_Table"].append(Data_Table)
        if isinstance(self.Data_Object_Table[file_name]["Data_Table"], list):
            self.Data_Object_Table[file_name]["Data_Table"] = self.Data_Object_Table[file_name]["Data_Table"][0]
        self.set_key_cols(file_name, "update_object")

######################################################################################################
    def get_all_unique_ids(self, re):
        for iterF in self.Data_Object_Table:
            if iterF not in ['submission.csv']:
                header_list = self.Data_Object_Table[iterF]["Data_Table"].columns.tolist()
                self.All_Part_ids = self.All_Part_ids + self.add_ids_to_list(iterF, header_list, "Research_Participant_ID")
                self.All_Bio_ids = self.All_Bio_ids + self.add_ids_to_list(iterF, header_list, "Biospecimen_ID")
                self.All_Ali_ids = self.All_Ali_ids + self.add_ids_to_list(iterF, header_list, "Aliquot_ID")
                self.All_Ali_ids = self.All_Ali_ids + self.add_ids_to_list(iterF, header_list, "Current Label")
        self.All_Part_ids = self.clean_up_ids(re, "Research_Participant_ID", '[_]{1}[A-Z, 0-9]{6}$')
        self.All_Bio_ids = self.clean_up_ids(re, "Biospecimen_ID", '[_]{1}[A-Z, 0-9]{6}[_]{1}[0-9]{3}$')
        self.All_Ali_ids = self.clean_up_ids(re, "Aliquot_ID", '[_]{1}[A-Z, 0-9]{6}[_]{1}[0-9]{3}[_]{1}[0-9]{1,2}$')

    def add_ids_to_list(self, curr_table, header_list, test_str):
        curr_ids = []
        if test_str in header_list:
            curr_ids = self.Data_Object_Table[curr_table]["Data_Table"][test_str].tolist()
        return curr_ids

    def get_passing_part_ids(self):
        if (int(self.Submit_Participant_IDs) != len(self.All_Part_ids)):
            error_msg = "After validation only " + str(len(self.All_Part_ids)) + " Participat IDS are valid"
            self.add_error_values("Error", "submission.csv", -5, "submit_Participant_IDs",
                                  self.Submit_Participant_IDs, error_msg)
        elif (int(self.Submit_Biospecimen_IDs) != len(self.All_Bio_ids)):
            error_msg = "After validation only " + str(len(self.All_Bio_ids)) + " Biospecimen IDS are valid"
            self.add_error_values("Error", "submission.csv", -5, "submit_Biospecimen_IDs",
                                  self.Submit_Biospecimen_IDs, error_msg)
        else:
            error_msg = "ID match, do not do anything"

    def clean_up_ids(self, re, col_name, test_str):
        if col_name == "Research_Participant_ID":
            curr_list = self.All_Part_ids
        elif col_name == "Biospecimen_ID":
            curr_list = self.All_Bio_ids
        elif col_name == "Aliquot_ID":
            curr_list = self.All_Ali_ids
        if len(curr_list) > 0:
            curr_list = list(set(curr_list))
            curr_list = [i for i in curr_list if (re.compile('^' + str(self.CBC_ID) + test_str).match(i) is not None)]
            curr_list = pd.DataFrame(curr_list, columns=[col_name])
        return curr_list

    def check_for_dup_ids(self, sheet_name, field_name):
        if sheet_name in self.Data_Object_Table:
            data_table = self.Data_Object_Table[sheet_name]['Data_Table']
            if "Visit_Info_ID" in data_table.columns:
                data_table = data_table.sort_values(["Research_Participant_ID", "Visit_Number"])
                data_table.drop_duplicates(["Research_Participant_ID", "Visit_Number"], inplace=True,keep="first")
            data_table.drop_duplicates(inplace=True)
            if field_name in data_table.columns:
                data_table = data_table[data_table[field_name].apply(lambda x: x not in ["N/A"])]
                if len(data_table) > 0:
                    #table_counts = data_table[field_name].value_counts(dropna=False).to_frame()
                    table_counts = data_table.groupby(field_name).size().reset_index(name='Count')
                    #dup_id_count = table_counts[table_counts[field_name] > 1]
                    dup_id_count = table_counts[field_name][table_counts['Count'] > 1]
                    for i in dup_id_count.index:
                        error_msg = "Id is repeated " + str(dup_id_count[field_name][i]) + " times, Multiple repeats are not allowed"
                        self.add_error_values("Error", sheet_name, -3, field_name, i, error_msg)

    def check_dup_visit(self, pd, data_table, drop_list, file_name):
        col_list = data_table.columns
        col_list = [i for i in col_list if i not in drop_list]
        filt_list = []
        filt_list = self.create_filt_list(filt_list, col_list, "Research_Participant_ID")
        filt_list = self.create_filt_list(filt_list, col_list, "Visit_Number")
        filt_list = self.create_filt_list(filt_list, col_list, "Cohort")
        filt_list = self.create_filt_list(filt_list, col_list, "Vaccination_Status")
        filt_list = self.create_filt_list(filt_list, col_list, "SARS-CoV-2_Vaccine_Type")
#        filt_list = self.create_filt_list(filt_list, col_list, "Biospecimen_ID")
#        filt_list = self.create_filt_list(filt_list, col_list, "Aliquot_ID")
#        filt_list = self.create_filt_list(filt_list, col_list, "Consumable_Name")
#        filt_list = self.create_filt_list(filt_list, col_list, "Equipment_ID")
#        filt_list = self.create_filt_list(filt_list, col_list, "Reagent_Name")

        if len(filt_list) > 0:  # only check for dups if columns were found
            test_table = data_table[filt_list]
            count_table = test_table.groupby(test_table.columns.tolist()).size().reset_index().\
                rename(columns={0: 'counts'})
            dup_visits = count_table.query("counts > 1")
            if len(dup_visits) > 0:
                dup_visits["File_Name"] = file_name
                if len(self.dup_visits) == 0:
                    self.dup_visits = dup_visits
                else:
                    self.dup_visits = pd.concat([self.dup_visits, dup_visits])

    def create_filt_list(self, filt_list, col_list, test_str):
        if test_str in col_list and test_str:
            filt_list.append(test_str)
        return filt_list

    def check_if_substr(self, data_table, id_1, id_2, file_name, header_name):
        id_compare = data_table[data_table.apply(lambda x: x[id_1] not in x[id_2], axis=1)]
        Error_Message = id_1 + " is not a substring of " + id_2 + ".  Data is not Valid, please check data"
        self.update_error_table("Error", id_compare, file_name, header_name, Error_Message)

    def check_if_substr_2(self, data_table, id_1, id_2, file_name, header_name):
        id_compare = data_table[data_table.apply(lambda x: str(x[id_1])[0:6] not in str(x[id_2])[0:6], axis=1)]
        Error_Message = id_1 + " is not a substring of " + id_2 + ".  Data is not Valid, please check data"
        id_compare = id_compare.query("Subaliquot_ID not in ['N/A']")
        self.update_error_table("Error", id_compare, file_name, header_name, Error_Message)

    def make_id_str(self, id_list, col_name):
        part_str = []
        if len(id_list) > 0:
            part_str = id_list[col_name]
            part_str = "'" + "', '".join(part_str) + "'"
        return part_str

    def populate_missing_keys(self, connection_tuple): #generate basline.csv
        part_str = self.make_id_str(self.All_Part_ids, "Research_Participant_ID")
        bio_str = self.make_id_str(self.All_Bio_ids, "Biospecimen_ID")
        aliquot_str = self.make_id_str(self.All_Ali_ids, "Aliquot_ID")

        self.update_keys(pd, 'Research_Participant_ID', part_str, connection_tuple)
        self.update_keys(pd, 'Biospecimen_ID', bio_str, connection_tuple)
        self.update_keys(pd, 'Aliquot_ID', aliquot_str, connection_tuple)
        for z in self.Data_Object_Table.keys():
            try:
                self.Data_Object_Table[z]["Data_Table"].drop_duplicates(inplace=True)
            except Exception as e:
                pass

    def update_keys(self, pd, header_name, id_str, connection_tuple):
        conn = connection_tuple[2]
        if len(id_str) > 0:
            part_table = self.sql_col_df.query("Column_Name == @header_name")
            for i in part_table["Table_Name"].tolist():
                z = self.sql_col_df.query("Table_Name == @i and Primary_Key == 'True'")
                select_var = ", ".join(z["Column_Name"].tolist())
                if i in "Aliquot":
                    select_var = select_var + ", Aliquot_Volume"
                test_qry = f"SELECT {select_var} FROM {i} WHERE {header_name} IN ({id_str});"
                if i in ["Participant_Prior_SARS_CoV2_PCR"]:
                    table_names = ["prior_clinical_test.csv"]
                else:
                    table_names = [z for z in self.sql_table_dict if i in self.sql_table_dict[z]]
                    if "visit_info_sql.csv" in table_names:
                        table_names.remove("visit_info_sql.csv")
                if len(table_names) > 0:
                    if "baseline.csv" in table_names:
                        print("x")
                    self.add_keys_to_tables(table_names, test_qry, pd, conn, header_name)

    def add_keys_to_tables(self, file_name, query_str, pd, conn,header_name):
        for curr_table in file_name:
            if curr_table not in self.Data_Object_Table:
                self.add_new_dict_data(curr_table, query_str, pd, conn, header_name)
            elif curr_table in self.Data_Object_Table:
                self.add_new_dict_data(curr_table, query_str, pd, conn, header_name)

    def add_new_dict_data(self, file_name, query_str, pd, conn, header_name, **kwargs):
        if file_name not in self.Data_Object_Table:
            self.Data_Object_Table[file_name] = {"Data_Table": []}
            old_data = []
        else:
            old_data = self.Data_Object_Table[file_name]["Data_Table"]
        curr_table = pd.read_sql((query_str), conn)
        x = [i for i in curr_table.columns.tolist() if ("Comments" in i) or ("Index" in i)]
        curr_table.drop(x, inplace=True, axis=1)

        #curr_table = check_if_col(curr_table, "Research_Participant_ID", self.All_Part_ids)
        #curr_table = check_if_col(curr_table, "Biospecimen_ID", self.All_Bio_ids)
        #curr_table = check_if_col(curr_table, "Aliquot_ID", self.All_Ali_ids)

        curr_table.rename(columns={"Test_Result": "SARS_CoV_2_PCR_Test_Result"}, inplace=True)
        if "col_list" in kwargs:
            curr_table == curr_table[kwargs["col_list"]]
        if len(old_data) == 0:
            self.Data_Object_Table[file_name]["Data_Table"] = curr_table
        elif len(old_data.columns) == len(curr_table.columns):
            #self.Data_Object_Table[file_name]["Data_Table"] = pd.concat([old_data, curr_table])
            self.Data_Object_Table[file_name]["Data_Table"] = pd.merge(old_data, curr_table, on=header_name, how="left")

######################################################################################################
    def check_col_errors(self, Subpath):
        col_err_count = len(self.Column_error_count)
        error_message = ''
        if col_err_count > 0:
            print("There are (" + str(col_err_count) + ") Column Names in the submission that are wrong/missing")
            print("Not able to process this submission, please correct and resubmit \n")
            #self.Column_error_count.to_csv(os.path.join(Subpath, "All_Column_Errors_Found.csv"), index=False)
            self.to_s3_csv(self.Column_error_count, os.path.join(Subpath, "All_Column_Errors_Found.csv"), False)
            error_message = "There are (" + str(col_err_count) + ") Column Names in the submission that are wrong/missing\n" + "Not able to process this submission, please correct and resubmit"
        return col_err_count, error_message

    def check_if_cbc_num(self, sheet_name, field_name, data_table, cbc_list):
        data_table[field_name] = [str(int(i)) for i in data_table[field_name]]
        wrong_code = data_table[data_table[field_name].apply(lambda x: x not in cbc_list)]
        for i in wrong_code.index:
            error_msg = "Lab ID is not valid, please check against list of approved ID values"
            self.add_error_values("Error", sheet_name, i+2, field_name, wrong_code[field_name][i], error_msg)

    def check_id_field(self, sheet_name, data_table, re, field_name, pattern_str, id_list, pattern_error):
        if field_name in ["Biorepository_ID", "Parent_Biorepository_ID", "Subaliquot_ID"]:
            wrong_cbc_id = []   # these are BSI/ FNL IDS, no CBC code found
            invalid_id = data_table[data_table[field_name].apply(
                lambda x: re.compile(pattern_str).match(str(x)) is None)]
        else:
            wrong_cbc_id = data_table[data_table[field_name].apply(lambda x: x[:2] not in [str(self.CBC_ID)])]
            invalid_id = data_table[data_table[field_name].apply(
                lambda x: re.compile('^' + str(self.CBC_ID) + pattern_str).match(str(x)) is None)]
        error_msg = "CBC code found is wrong. Expecting CBC Code (" + str(self.CBC_ID) + ")"
        self.add_id_errors(wrong_cbc_id, error_msg, field_name, sheet_name)
        error_msg = "ID is Not Valid Format, Expecting " + pattern_error
        self.add_id_errors(invalid_id, error_msg, field_name, sheet_name)
        self.sort_and_drop(field_name)

    def add_id_errors(self, cbc_df, error_msg, field_name, sheet_name):
        if len(cbc_df) > 0:
            for i in cbc_df.index:
                self.add_error_values("Error", sheet_name, i+2, field_name, cbc_df[field_name][i], error_msg)

    def check_assay_special(self, data_table, header_name, file_name, sheet_name, re):
        assay_table = self.Data_Object_Table[file_name]["Data_Table"]
        assay_table.rename(columns={"Target_Organism": "Assay_Target_Organism"}, inplace=True)
        data_table.replace('EBV Nuclear antigen � 1', 'EBV Nuclear antigen - 1', inplace=True)

        error_data = data_table.merge(assay_table, on=header_name, indicator=True, how="left")
        error_data = error_data.query("_merge in ['left_only']")
        error_msg = "Value is not found in the database"
        self.update_error_table("Error", error_data, sheet_name, header_name, error_msg, keep_blank=False)

    def check_in_meta(self, file_name, data_table, header_name, meta_table, meta_col):
        meta_data = self.Data_Object_Table[meta_table]["Data_Table"]
        meta_data.drop_duplicates(meta_col, inplace=True)
        z = data_table.merge(meta_data, how="left", left_on=header_name, right_on=meta_col, indicator=True)
        z = z.query("_merge == 'left_only'")
        if len(z) > 0:
            error_msg = f"Cohort is not found in {meta_table}"
            self.update_error_table("Error", z, file_name, header_name, error_msg, keep_blank=False)
######################################################################################################

    def check_for_dependancy(self, data_table, depend_col, depend_val, sheet_name, header_name):
        error_str = "Unexpected Value. "
        if depend_col != "None":
            data_table, error_str = check_multi_rule(data_table, depend_col, depend_val)
        if isinstance(data_table, (int, float)):
            self.add_error_values("Error", sheet_name, 0, header_name, "Entire Column", error_str)
            data_table = []
        return data_table, error_str

    def unknown_list_dependancy(self, sheet_name, header_name, data_table, depend_col, depend_list):
        if depend_col not in data_table.columns:
            print(f"{depend_col} is missing, can not evaluate {header_name}")
        else:
            error_data = data_table.query("{0} not in {1}".format(depend_col, depend_list))
            if len(error_data) > 0:
                for err_idx in error_data.index:
                    split_list = str(error_data[depend_col][err_idx]).split("|")
                    split_list = [i.strip() for i in split_list]
                    check_list = [i for i in split_list if i not in depend_list]
                    if len(check_list) > 0:
                        df = error_data[error_data.index == err_idx]
                        self.add_unknown_warnings(df, sheet_name, header_name, depend_col)

    def unknow_number_dependancy(self, sheet_name, header_name, data_table, depend_col, depend_list):
        data_table = data_table[data_table[depend_col].apply(lambda x: not isinstance(x, (float, int)))]
        error_data = data_table[data_table[depend_col].apply(lambda x: x not in depend_list)]
        self.add_unknown_warnings(error_data, sheet_name, header_name, depend_col)

    def get_error_type(self, kwargs):
        Error_type = "Error"
        if "required" in kwargs:
            Error_type = kwargs["required"]
        return Error_type

######################################################################################################
    def check_no_vaccine_status(self, sheet_name, data_table, header_name, depend_col, depend_val):
        if depend_val in ["Not Unvaccinated"]:
            for z in data_table.index:
                if (data_table.loc[z, depend_col] not in ["No vaccination event reported", "Unvaccinated"] and
                   data_table.loc[z, header_name] in ["N/A"]):
                    err_msg = f"{depend_col} has a Vaccine Status, but {header_name} is N/A"
                elif (data_table.loc[z, depend_col] in ["No vaccination event reported", "Unvaccinated"] and
                      data_table.loc[z, header_name] not in ["N/A"]):
                    err_msg = f"{header_name} has value that is not N/A, but {depend_col} is NO/Unknown Vaccine Status"
                elif (data_table.loc[z, depend_col] in ["No vaccination event reported", "Unvaccinated"] and
                      data_table.loc[z, header_name] in ["N/A"]):
                    data_table.drop(z, axis=0, inplace=True)
                    continue
                else:
                    continue
                self.add_error_values("Error", sheet_name, z+2, header_name, data_table.loc[z, header_name], err_msg)
                data_table.drop(z, axis=0, inplace=True)
        return data_table

    def check_in_list(self, sheet_name, data_table, header_name, depend_col, depend_val, list_values):
        data_table, error_str = self.check_for_dependancy(data_table, depend_col, depend_val, sheet_name, header_name)
        if depend_val in ["Not Other"]:
            for z in data_table.index:
                if "Other" in data_table.loc[z, depend_col] and data_table.loc[z, header_name] in list_values:
                    err_msg = f"{depend_col} has Other, but {header_name} is N/A"
                    self.add_error_values("Error", sheet_name, z+2, header_name, data_table.loc[z, header_name], err_msg)
                elif "Other" not in data_table.loc[z, depend_col] and data_table.loc[z, header_name] not in ["N/A"]:
                    err_msg = f"{header_name} has value but Other is missing from {depend_col}"
                    self.add_error_values("Error", sheet_name, z+2, header_name, data_table.loc[z, header_name], err_msg)
                elif "Other" in data_table.loc[z, depend_col] and data_table.loc[z, header_name] not in list_values:
                    pass
                else:
                    continue
                data_table.drop(z, axis=0, inplace=True)

        if len(data_table) > 0:
            if isinstance(list_values, list):
                new_list_values = list(set(list_values + [str(i).lower() for i in list_values]))
            else:
                new_list_values = list(set([list_values, list_values.lower()]))
            for i in data_table.index:
                try:
                    curr_data = data_table.loc[i, header_name].split("|")
                    curr_data = [i.strip() for i in curr_data]
                except AttributeError:
                    curr_data = data_table.loc[i, header_name]
                    curr_data = [curr_data]
                if depend_col not in ["None"]:
                    error_str = depend_col + " is " + str(data_table.loc[i, depend_col]) + ". "
                if depend_col in ["None"] and depend_val not in ["None"]:
                    error_str = f"Participant is {depend_val} for COVID_Status.  "
                error_msg = error_str + "Value must be one of the following: " + str(list_values)
                logic_match = [str(i).lower() not in new_list_values for i in curr_data]
                error_values = list(compress(curr_data, logic_match))
                if len(error_values) > 0:
                    for curr_error in error_values:
                        self.add_error_values("Error", sheet_name, i+2, header_name, curr_error, error_msg)

    def compare_list_sizes(self, file_name, main_col, depend_col):
        data_table = self.Data_Object_Table[file_name]["Data_Table"]
        data_table.reset_index(inplace=True, drop=True)
        for curr_row in data_table.index:
            col_1 = data_table.loc[curr_row, main_col].split("|")
            col_2 = data_table.loc[curr_row, depend_col].split("|")
            if len(col_1) != len(col_2):
                error_msg = (main_col + " has " + str(len(col_1)) + " entries " + ", but " + depend_col +
                             " has " + str(len(col_2)) + " entries")
                self.add_error_values("Error", file_name, curr_row+2, main_col, depend_col, error_msg)

    def check_list_errors(self, file_name, data_table, header_name, list_values, no_tests):
        for curr_idx in data_table.index:
            split_data = data_table.loc[curr_idx, header_name].split("|")
            if len(split_data) > 1:  # multiple selections were made
                logic_match = [i for i in split_data if i in no_tests]
                if len(logic_match) > 0:  # multiple selctions, one is no test (not allowed to have both)
                    error_msg = "Value has both a Test and a Non-Test selected, not valid combination"
                    self.add_error_values("Error", file_name, curr_idx+2, header_name, str(split_data), error_msg)

    def check_pike_dups(self, file_name, header_name):
        data_table = self.Data_Object_Table[file_name]["Data_Table"]
        for curr_row in data_table.index:
            col_1 = data_table.loc[curr_row, header_name].split("|")
            x = Counter(col_1)
            for index in x:
                if x[index] > 1:  # duplicate found
                    self.add_error_values("Error", file_name, curr_row+2, header_name, index,
                                          f"Value is not unique, was found {x[index]} times for same visit")

    def check_if_number(self, sheet_name, data_table, header_name, depend_col, depend_val, allowed_values,
                        lower_lim, upper_lim, num_type, **kwargs):
        data_table, error_str = self.check_for_dependancy(data_table, depend_col, depend_val, sheet_name, header_name)
        data_table = self.check_no_vaccine_status(sheet_name, data_table, header_name, depend_col, depend_val)

        Error_Type = self.get_error_type(kwargs)
        if len(data_table) == 0:
            return{}
        error_msg = error_str + "Value must be a number between " + str(lower_lim) + " and " + str(upper_lim)
        data_list = data_table[header_name].tolist()
        for iterD in enumerate(data_list):
            if isinstance(iterD[1], pd.Timestamp):
                time_conv = iterD[1].hour + (iterD[1].minute)/60
                data_table.at[iterD[0], header_name] = time_conv
        number_only = data_table[header_name].apply(lambda x: isinstance(x, (int, float)))
        good_data = data_table[number_only]

        good_logic = data_table[header_name].apply(lambda x: isinstance(x, (int, float)) or x in [''])
        to_low = good_data[header_name].apply(lambda x: x < lower_lim)
        to_high = good_data[header_name].apply(lambda x: x > upper_lim)

        if num_type == "int":
            #  is_float = good_data[header_name].apply(lambda x: str(x).is_integer() is False)
            is_float = good_data[header_name].apply(lambda x: isinstance(x, (int)) is False)
            error_msg = (error_str + "Value must be an interger between " + str(lower_lim) + " and " +
                         str(upper_lim))
            self.update_error_table("Error", good_data[is_float], sheet_name, header_name, error_msg)
        elif num_type == "float":
            is_number = good_data[header_name].apply(lambda x: isinstance(x, (int, float)) is False)
            error_msg = (error_str + "Value must be a number between " + str(lower_lim) + " and " + str(upper_lim))
            self.update_error_table("Error", good_data[is_number], sheet_name, header_name, error_msg)

        if len(allowed_values) > 0:
            good_logic = data_table[header_name].apply(lambda x: isinstance(x, (int, float)) or x in allowed_values)
            error_msg = error_msg + " Or in " + str(allowed_values)

        error_data = data_table[[not x for x in good_logic]]
        if depend_col == "None":
            self.update_error_table(Error_Type, error_data, sheet_name, header_name, error_msg)
            self.update_error_table(Error_Type, good_data[to_low], sheet_name, header_name, error_msg)
            self.update_error_table(Error_Type, good_data[to_high], sheet_name, header_name, error_msg)
        else:
            for curr_err in error_data.index:
                test_col = str(error_data.loc[curr_err][depend_col]).split("|")
                check_logic = [i for i in test_col if i in depend_val]
                new_error = error_msg.replace(error_str, f"{depend_col} is {check_logic}. ")
                self.update_error_table(Error_Type, error_data.loc[[curr_err], :], sheet_name, header_name, new_error)

        if ('Duration_of' in header_name) and (('infection' in header_name) or ("HAART_Therapy" in header_name)):
            warn_data = data_table.query("{0} == 'N/A'".format(header_name))
            warn_msg = f"{depend_col} is in {depend_val} and {header_name} is N/A"
            self.update_error_table("Warning", warn_data, sheet_name, header_name, warn_msg)

    def check_duration_rules(self, file_name, data_table, header_name, depend_col, depend_val,
                             max_date, curr_year, Duration_Rules):
        if (header_name in [Duration_Rules[0]]):
            self.check_if_number(file_name, data_table, header_name, depend_col, depend_val, ["N/A"], -1e6, 1e6, "float")
            self.compare_dates_to_curr(file_name, data_table, header_name,
                                       (header_name + "_Unit"), Duration_Rules[2], max_date)
        elif (header_name in [Duration_Rules[1]]):
            if Duration_Rules[1] in data_table.columns:
                self.check_in_list(file_name, data_table, header_name, Duration_Rules[0], ["N/A"], ["N/A"])
                self.check_in_list(file_name, data_table, header_name, Duration_Rules[0], "Is A Number",
                                   ["Day", "Week", "Month", "Year"])
                self.unknow_number_dependancy(file_name, header_name, data_table, Duration_Rules[0], ["N/A"])
        elif (header_name in [Duration_Rules[2]]):
            self.check_in_list(file_name, data_table, header_name, Duration_Rules[0], ["N/A"], ["N/A"])
            self.check_if_number(file_name, data_table, header_name, Duration_Rules[0], "Is A Number",
                                 [], 1900, curr_year, "int")
            self.unknow_number_dependancy(file_name, header_name, data_table, Duration_Rules[0], ["N/A"])

    def get_test_dur(self, file_name, data_table, header_name, list_values, test_str):
        has_test = [i for i in list_values if test_str in i]
        missing_test = [i for i in list_values if test_str not in i]

        has_data = []
        for test in has_test:
            z = data_table[[test in i.split("|") for i in data_table["COVID_Status"].tolist()]]
            self.check_if_number(file_name, z, header_name, "COVID_Status", has_test, ["Not Reported"], -1000, 1000, "int")
            if len(has_data) == 0:
                has_data = z
            else:
                has_data = pd.concat([has_data, z])

        y = data_table.merge(has_data, how="outer", indicator=True)
        y = y.query("_merge == 'left_only'")
        self.check_in_list(file_name, y, header_name, "COVID_Status", missing_test, ["N/A"])

    def compare_dates_to_curr(self, sheet_name, data_table, header_name, unit_name, year_name, curr_date):
        curr_year = curr_date.year
        curr_month = curr_date.month
        test_data = data_table[data_table[year_name].apply(lambda x: isinstance(x, (int, float)))]
        if unit_name in data_table.columns:
            year_data = test_data.query("{0} == 'Year' or {0} == 'year'".format(unit_name))
            month_data = test_data.query("{0} == 'Month' or {0} == 'month'".format(unit_name))
            day_data = test_data.query("{0} == 'Day' or {0} == 'day'".format(unit_name))

            bad_month = month_data[month_data[header_name] + month_data[year_name]*12 > (curr_year*12 + curr_month)]
            bad_year = year_data[year_data[header_name] + year_data[year_name] > curr_year]
            day_dur = day_data[year_name].apply(lambda x: (curr_date - (datetime.date(int(x), 1, 1))).days)
            bad_day = day_data[day_data[header_name] > day_dur]
            bad_data = pd.concat([bad_day, bad_month, bad_year])
        else:
            unit_name = "days"
            bad_index = test_data.query("{0} > @curr_year".format(year_name)).index
            wrong_data = test_data.loc[bad_index]
            test_data.drop(bad_index, inplace=True)
            day_dur = test_data[year_name].apply(lambda x: (curr_date - (datetime.date(int(x), 1, 1))).days)

            try:
                bad_data = test_data[test_data[header_name] > day_dur]
                if len(wrong_data) > 0:
                    bad_data = pd.concat([bad_data, wrong_data])
            except Exception as e:
                bad_data = wrong_data
                print(e)
        for iterZ in bad_data.index:
            error_msg = header_name + " Exists in the Future, not valid combination, Check Duration Units"
            if unit_name == "days":
                error_unit = "Days"
            else:
                error_unit = bad_data.loc[iterZ][unit_name]
            error_val = (error_unit + ": " + str(bad_data.loc[iterZ][header_name]) +
                         ", Year: " + str(bad_data.loc[iterZ][year_name]))
            self.add_error_values("Error", sheet_name, iterZ+2, header_name, error_val, error_msg)

    def check_if_string(self, sheet_name, data_table, header_name, depend_col, depend_val, na_allowed, **kwargs):
        data_table, error_str = self.check_for_dependancy(data_table, depend_col, depend_val, sheet_name, header_name)
        data_table = self.check_no_vaccine_status(sheet_name, data_table, header_name, depend_col, depend_val)
        Error_type = self.get_error_type(kwargs)
        if len(data_table) > 0:
            if depend_col == "None":
                error_msg = "Value must be a string and NOT N/A "
            else:
                error_msg = error_str + ".  Value must be a string and NOT N/A"
            # value can be a string but can not be a string of spaces
            good_logic = data_table[header_name].apply(lambda x: (isinstance(x, (int, float, str)) or x in [''] or
                                                                  len(str(x).strip()) > 0) and (x not in ['N/A']))
            if len(na_allowed) > 0:
                error_msg.replace("and NOT N/A", "OR in " + str(na_allowed))
                good_logic = data_table[header_name].apply(lambda x: (isinstance(x, (int, float, str)) or x in [''] or
                                                           len(str(x).strip()) > 0) or (x not in na_allowed))
            error_data = data_table[[not x for x in good_logic]]
            if header_name in ["Comments"]:
                error_msg = "Value must be a non empty string and NOT N/A ('  ') not allowed"
                self.update_error_table("Warning", error_data, sheet_name, header_name, error_msg)
            else:
                for i in error_data.index:
                    if depend_col != "None":
                        error_msg = error_msg.replace(error_str, f"{depend_col} is {error_data.loc[i][depend_col]}")
                    self.add_error_values(Error_type, sheet_name, i+2, header_name,
                                          error_data.loc[i][header_name], error_msg)

    def check_date(self, datetime, sheet_name, data_table, header_name, depend_col, depend_val,
                   na_allowed, time_check, lower_lim=0, upper_lim=24):
        data_table, error_str = self.check_for_dependancy(data_table, depend_col, depend_val, sheet_name, header_name)
        if len(data_table) == 0:
            return{}
        date_only = data_table[header_name].apply(lambda x: isinstance(x, (datetime.datetime, datetime.date)))
        good_date = data_table[date_only]
        if time_check == "Date":
            error_msg = error_str + "Value must be a Valid Date MM/DD/YYYY"
        else:
            error_msg = error_str + "Value must be a Valid Time HH:MM:SS"
        if na_allowed is False:
            date_logic = data_table[header_name].apply(lambda x: isinstance(x, (datetime.datetime, datetime.date)) or x in [''])
        else:
            date_logic = data_table[header_name].apply(lambda x: isinstance(x, (datetime.datetime, datetime.date)) or x in ['N/A', ''])
            error_msg = error_msg + " Or N/A"
        error_data = data_table[[not x for x in date_logic]]
        self.update_error_table("Error", error_data, sheet_name, header_name, error_msg)
        if time_check == "Date" and len(good_date) > 0:
            date_data = [i.date() if isinstance(i, datetime.datetime) else i for i in good_date[header_name]]
            to_early = [i < lower_lim for i in date_data]
            to_late = [i > upper_lim for i in date_data]
            if "Expiration_Date" in header_name:
                error_msg = "Expiration Date has already passed, check to make sure date is correct"
#                self.update_error_table("Warning", good_date[to_early], sheet_name, header_name, error_msg)
            elif "Calibration_Due_Date" in header_name:
                error_msg = "Calibration Date has already passed, check to make sure date is correct"
#                self.update_error_table("Warning", good_date[to_early], sheet_name, header_name, error_msg)
            else:
                error_msg = "Date is valid however must be between " + str(lower_lim) + " and " + str(upper_lim)
                self.update_error_table("Error", good_date[to_early], sheet_name, header_name, error_msg)
            error_msg = "Date is valid however must be between " + str(lower_lim) + " and " + str(upper_lim)
            self.update_error_table("Error", good_date[to_late], sheet_name, header_name, error_msg)

    def check_interpertation(self, sheet_name, data_table, header_name, list_values):
        error_msg = "Value must contain of the following options: " + str(list_values)
        curr_data = data_table[header_name]
        row_index = []
        for iterC in curr_data.index:
            logic_list = [i for i in list_values if i in curr_data[iterC].lower()]
            if len(logic_list) == 0:
                row_index.append(iterC)
        error_data = data_table.loc[row_index]
        self.update_error_table("Error", error_data, sheet_name, header_name, error_msg)

    def check_icd10(self, sheet_name, data_table, header_name):
        number_data = data_table[data_table[header_name].apply(lambda x: not isinstance(x, str))]
        data_table = data_table[data_table[header_name].apply(lambda x: isinstance(x, str))]
        data_table[header_name] = [i.split(',') for i in data_table[header_name]]

        error_index = []
        for curr_index in data_table.index:
            logic_list = [(icd10.exists(i.strip()) or i in ["N/A"]) for i in data_table[header_name][curr_index]]
            if False in logic_list:
                error_index.append(curr_index)
                error_list = [i for (i, v) in zip(data_table[header_name][curr_index], logic_list) if not v]
                data_table[header_name][curr_index] = error_list[0]

        error_data = data_table.loc[error_index]
        Error_Message = "Invalid or unknown ICD10 code, Value must be Valid ICD10 code or N/A"
        self.update_error_table("Error", error_data, sheet_name, header_name, Error_Message)
        self.update_error_table("Error", number_data, sheet_name, header_name, Error_Message)

######################################################################################################
    def compare_total_to_live(self, sheet_name, data_table, header_name):
        second_col = header_name.replace('Total_Cells', 'Live_Cells')
        data_table, error_str = self.check_for_dependancy(data_table, header_name, "Is A Number", sheet_name, header_name)
        data_table, error_str = self.check_for_dependancy(data_table, second_col, "Is A Number", sheet_name, header_name)
        if len(data_table) == 0:
            return
        error_data = data_table.query("{0} > {1}".format(second_col, header_name))
        for iterZ in error_data.index:
            error_msg = "Total Cell Count must be greater then Live Cell Count (" + str(error_data[second_col][iterZ]) + ")"
            self.add_error_values("Error", sheet_name, iterZ+2, header_name, error_data.loc[iterZ][header_name], error_msg)

    def compare_viability(self, sheet_name, data_table, header_name):
        live_col = header_name.replace('Viability', 'Live_Cells')
        total_col = header_name.replace('Viability', 'Total_Cells')
        data_table, error_str = self.check_for_dependancy(data_table, header_name, "Is A Number", sheet_name, header_name)
        data_table, error_str = self.check_for_dependancy(data_table, live_col, "Is A Number", sheet_name, header_name)
        data_table, error_str = self.check_for_dependancy(data_table, total_col, "Is A Number", sheet_name, header_name)
        if len(data_table) == 0:
            return

        error_data = data_table[data_table.apply(lambda x: x[total_col] == 0 and x[header_name] not in ['N/A'], axis=1)]
        error_msg = "Total Count is 0, Viability_Count should be N/A"
        self.update_error_table("Warning", error_data, sheet_name, header_name, error_msg)

        data_table = data_table[data_table.apply(lambda x: x[total_col] > 0, axis=1)]
        error_data = data_table[data_table.apply(lambda x: round((x[live_col]/x[total_col])*100, 1) != x[header_name],
                                                 axis=1)]

        for iterZ in error_data.index:
            via_pct = round((error_data[live_col][iterZ] / error_data[total_col][iterZ])*100, 1)
            error_msg = "Viability Count must be (" + str(via_pct) + ") which is (Live_Count / Total_Count) * 100"
            self.add_error_values("Error", sheet_name, iterZ+2, header_name, error_data.loc[iterZ][header_name], error_msg)

######################################################################################################
    def get_missing_values(self, sheet_name, data_table, header_name, Required_column):
        if header_name in ["Comments"]:  # comments can be left blank, no need to warn
            return
        try:
            missing_data = data_table.query("{0} == '' ".format(header_name))
        except Exception:
            missing_data = data_table[data_table[header_name].apply(lambda x: x == '')]
        if len(missing_data) == 0 or header_name in ["Comments"]:
            return  # comments can be left blank, no need to warn

        if len(missing_data) > 0:
            if Required_column == "Yes":
                error_msg = "Missing Values are not allowed for this column.  Please recheck data"
                self.update_error_table("Error", missing_data, sheet_name, header_name, error_msg, True)
            elif Required_column == "No":
                error_msg = "Missing Values where found, this is a warning.  Please recheck data"
                self.update_error_table("Warning", missing_data, sheet_name, header_name, error_msg, True)

    def add_error_values(self, msg_type, sheet_name, row_index, col_name, col_value, error_msg):
        new_row = {"Message_Type": msg_type, "CSV_Sheet_Name": sheet_name, "Row_Index": row_index,
                   "Column_Name": col_name, "Column_Value": col_value, "Error_Message": error_msg}
        #self.Error_list = self.Error_list.append(new_row, ignore_index=True) outdated
        self.Error_list = pd.concat([self.Error_list, pd.DataFrame([new_row])], ignore_index=True)

    def update_error_table(self, msg_type, error_data, sheet_name, header_name, error_msg, keep_blank=False):
        try:
            for i in error_data.index:
                self.add_error_values(msg_type, sheet_name, i+2, header_name, error_data.loc[i][header_name], error_msg)
            self.sort_and_drop(header_name, keep_blank)
        except Exception as e:
            print(e)

    def sort_and_drop(self, header_name, keep_blank=False):
        self.Error_list.drop_duplicates(["CSV_Sheet_Name", "Row_Index", "Column_Name", "Column_Value"], inplace=True)

    def add_unknown_warnings(self, error_data, sheet_name, header_name, depend_col):
        error_msg = depend_col + " is a dependant column and has an invalid value for this record, unable to validate value for " + header_name + " "
        self.update_error_table("Not Validated", error_data, sheet_name, header_name, error_msg, keep_blank=False)

######################################################################################################
    def validate_child_panel(self, data_table, valid_child, sheet_name):
        z = data_table.merge(valid_child, left_on="Subaliquot_ID", right_on="CGR_Child_ID", indicator=True, how="outer")
        z = z.query("_merge not in ['both']")
        z = z[["Subaliquot_ID", "CGR_Child_ID"]]
        if len(z) > 0:
            extra_ids = z.query("Subaliquot_ID == Subaliquot_ID")
            error_msg = "Ids are found that were not part of the shipped panel, check for typos"
            self.update_error_table("Error", extra_ids, sheet_name, "Subaliquot_ID", error_msg, keep_blank=False)

            missing_child = z.query("CGR_Child_ID == CGR_Child_ID")
            error_msg = "This subaliquot ID was part of the panel, but missing from results"
            self.update_error_table("Error", missing_child, sheet_name, "CGR_Child_ID", error_msg, keep_blank=False)

    def get_cross_sheet_ID(self, re, field_name, file_sep):
        if field_name == "Biospecimen_ID":
            file_list = self.All_Bio_ids
            error_table = "Cross_Biospecimen_ID.csv"
        elif field_name == "Research_Participant_ID":
            file_list = self.All_Part_ids
            error_table = "Cross_Participant_ID.csv"
        if len(file_list) == 0:
            return

        merge_names = self.Template_Cols.query("Column_Name == @field_name")["Sheet_Name"].tolist()
        merge_names = [i.replace("xlsx", "csv") for i in merge_names]
        merge_names = [i.replace("xlsm", "csv") for i in merge_names]

        all_merge = file_list
        for iterF in merge_names:
            try:
                curr_col = self.Data_Object_Table[iterF]["Key_Cols"]
                all_merge = all_merge.merge(self.Data_Object_Table[iterF]["Data_Table"][curr_col],
                                            how="outer", indicator=iterF)
                all_merge.drop_duplicates(inplace=True)
            except Exception as e:
                pass  # table not found in submission, no need to check

        all_merge = all_merge[all_merge.isna().any(axis=1)]
        if len(all_merge) == 0:  # all ids match across all sheets
            return
        all_merge = all_merge[[i for i in all_merge.columns if i == "Biospecimen_ID" or '.csv' in i]]
        all_merge.drop_duplicates(inplace=True)
        all_merge.replace({"both": "Found", "left_only": "Missing"}, inplace=True)

        self.check_validation_folder(os)
        all_merge.to_csv(self.Data_Validation_Path + file_sep + error_table, index=False)

    def compare_assay_data(self, sheet_1, sheet_2, key_list, error_msg):
        if ((sheet_1 in self.Data_Object_Table) and (sheet_2 in self.Data_Object_Table)):
            data_set_1 = (self.Data_Object_Table[sheet_1]["Data_Table"][key_list])
            data_set_2 = (self.Data_Object_Table[sheet_2]["Data_Table"][key_list])

            data_set_1.replace("", "N/A", inplace=True)
            data_set_2.replace("", "N/A", inplace=True)
            data_set_1.fillna("N/A", inplace=True)
            data_set_2.fillna("N/A", inplace=True)

            compare_data = pd.merge(data_set_1, data_set_2, indicator=True, how="left")
            compare_data = compare_data.query("_merge not in ['both']")
            if len(compare_data) > 0:
                print("Errors are found here, double check")
                for curr_row in compare_data.index:
                    list_val = compare_data.loc[curr_row, key_list].tolist()
                    str_val = ", ".join(list_val)
                    self.add_error_values("Error", sheet_1, curr_row+2, "Multiple Assay Columns",
                                          str_val, error_msg)

    def check_comorbid_dict(self, pd, conn):
        norm_table = pd.read_sql(("SELECT * FROM Normalized_Comorbidity_Dictionary;"), conn)
        norm_table.fillna("N/A", inplace=True)
        error_table = pd.DataFrame(columns=["Sheet_Name", "Comorbidity_Catagory", "Comorbidity_Description"])
        miss_terms = []

        if "baseline.csv" in self.Data_Object_Table:
            base_table = self.Data_Object_Table["baseline.csv"]["Data_Table"]
            miss_terms = self.find_missing_terms(base_table, norm_table, error_table, "baseline.csv")
        if "follow_up.csv" in self.Data_Object_Table:
            follow_table = self.Data_Object_Table["follow_up.csv"]["Data_Table"]
            miss_terms = self.find_missing_terms(follow_table, norm_table, error_table, "follow_up.csv")
        if len(miss_terms) > 0:
            miss_terms.drop_duplicates(inplace=True)
            #print(miss_terms)

    def find_missing_terms(self, df, norm_table, error_table, table_name):
        uni_cond = list(set(norm_table["Comorbid_Name"]))
        if len(df.columns) < len(uni_cond):  # file was not included in submission, code below will error
            return []
        for curr_cond in uni_cond:
            if curr_cond in ["Autoimmune_Disorder_Description_Or_ICD10_codes", "Cancer_Description_Or_ICD10_codes", 
                             "Viral_Infection_ICD10_codes_Or_Agents"]:
                continue    #should not exist, already have Autoimmune_Disorder in list
            filt_table = df[[i for i in df.columns if curr_cond in i]]
            try:
                filt_table.drop(curr_cond, axis=1, inplace=True)
            except Exception:
                print(f"{curr_cond} does not exist")
            if filt_table.shape[1] != 1:
                print("error")
            else:
                try:
                    filt_table[filt_table.columns[0]] = [i.lower() for i in filt_table[filt_table.columns[0]]]
                except Exception as e:
                    print(e)
                x = filt_table.merge(norm_table, left_on=filt_table.columns[0],
                                     right_on="Orgional_Description_Or_ICD10_codes", how="left", indicator=True)
                x = x.query("_merge == 'left_only'")
                x = x[filt_table.columns]
                x = x.merge(norm_table, left_on=x.columns[0], right_on="Normalized_Description", how="left", indicator=True)
                x = x.query("_merge == 'left_only'")

                for curr_idx in x.index:
                    error_table.loc[len(error_table.index)] = [table_name, x.columns[0], x.loc[curr_idx][x.columns[0]]]
        return error_table

######################################################################################################
    def add_miss_vac_errors(self, curr_part, curr_id, miss_d1, miss_d2, miss_d2a):
        if miss_d1 is True:
            error_msg = "Participant has 'Dose 2 of 2' but is missing a record for 'Dose 1 of 2'"
            self.add_error_values("Error", "Dosage_Errors.csv", 1, "Research_Participant_ID", curr_id, error_msg)
        if miss_d2 is True:
            error_msg = "Participant has 'Dose 3' but is missing a record for 'Dose 2 of 2'"
            self.add_error_values("Error", "Dosage_Errors.csv", 1, "Research_Participant_ID", curr_id, error_msg)
        if miss_d2a is True:
            error_msg = "Participant has 'Booster 1' but is missing a record for 'Dose 2 of 2'"
            self.add_error_values("Error", "Dosage_Errors.csv", 1, "Research_Participant_ID", curr_id, error_msg)
        for curr_boost in range(2, 5):
            miss_boost = "Booster " + str(curr_boost) in curr_part["Vaccination_Status"] and ("Booster " + str(curr_boost-1) not in
                                                                                              curr_part["Vaccination_Status"])
            if miss_boost is True:
                error_msg = ("Participant has 'Booster " + str(curr_boost) +
                             "' but is missing a record for 'Booster " + str(curr_boost-1) + "'")
                self.add_error_values("Error", "Dosage_Errors.csv", 1, "Research_Participant_ID", curr_id, error_msg)

    def compare_visits(self, visit_type):
        data_dict = self.Data_Object_Table
        if "visit_info_sql.csv" not in data_dict:
            return
        visit_info = data_dict["visit_info_sql.csv"]['Data_Table']
        #visit_info["Visit_Number"] = [int(i[-2:]) for i in visit_info["Visit_Info_ID"]]
        visit_info["Visit_Number"] = [i[-2:] if i[-1] in ["A", "B", "C", "D"] else int(i[-2:]) for i in visit_info["Visit_Info_ID"]]
        if visit_type == "baseline":
            query_str = "Visit_Number in ['Baseline(1)']"
        elif visit_type == "followup":
            query_str = "Visit_Number not in ['Baseline(1)']"
        x = self.Template_Cols.query("Column_Name in ['Visit', 'Visit_Number']")
        x["Sheet_Name"] = x["Sheet_Name"].replace({".xlsx": ".csv", ".xlsm": ".csv"}, regex=True)
        valid_sheets = x["Sheet_Name"].tolist()

        for iterZ in data_dict:
            if iterZ in valid_sheets:
                curr_data = data_dict[iterZ]['Data_Table']
                curr_data = curr_data.rename(columns={"Visit": "Visit_Number"})
                if "Visit_Info_ID" in curr_data.columns:
                    curr_data.drop("Visit_Info_ID", inplace=True, axis=1)
                if "Visit_Number" not in curr_data.columns:
                    continue  # iterZ is an sql sheet and only has partial columns
                baseline_visit = curr_data.query(query_str)
                baseline_visit = baseline_visit.replace("Baseline(1)", 1)
                try:
                    check_visit = baseline_visit.merge(visit_info, indicator=True, how="left")
                except ValueError:
                    baseline_visit["Visit_Number"] = [convert_data_type(c) for c in baseline_visit["Visit_Number"]]
                    visit_info["Visit_Number"] = [convert_data_type(c) for c in visit_info["Visit_Number"]]
                    check_visit = baseline_visit.merge(visit_info, indicator=True, how="left")
                finally:
                    check_visit = check_visit.query("_merge not in ['both']")
                    check_visit.drop_duplicates(inplace=True)
                    for i in check_visit.index:
                        visit_num = check_visit.loc[i]["Visit_Number"]
                        if visit_num == 1:
                            error_msg = f"Participant has baseline visit in {iterZ} but missing data from baseline.csv"
                        else:
                            error_msg = f"Participant has visit number {visit_num} in {iterZ} but missing coresponding visit in follow_up.csv"
                        self.add_error_values("Error", iterZ, i+2, "Research_Participant_ID",
                                              check_visit.loc[i]["Research_Participant_ID"], error_msg)

######################################################################################################
    def write_col_errors(self, Error_Path):
        #self.Column_error_count.to_csv(Error_Path + "All_Column_Errors_Found.csv", index=False)
        self.to_s3_csv(self.Column_error_count, Error_Path + "All_Column_Errors_Found.csv", False)

    def write_error_file(self, file_sep):
        uni_name = list(set(self.Error_list["CSV_Sheet_Name"]))
        if len(uni_name) == 0:
            print("No Errors were found in this submission")
        else:
            self.check_validation_folder()
        for iterU in uni_name:
            curr_table = self.Error_list.query("CSV_Sheet_Name == @iterU")
            curr_name = iterU.replace('.csv', '_Errors.csv')
            if uni_name in ["Cross_Participant_ID.csv", "Cross_Biospecimen_ID.csv", "submission.csv"]:
                curr_table = curr_table.sort_index()
            else:
                curr_table = curr_table.sort_values('Row_Index')
            curr_name = curr_name.replace(".xlsx", ".csv")
            #curr_table.to_csv(self.Data_Validation_Path + file_sep + curr_name, index=False)
            self.to_s3_csv(curr_table, self.Data_Validation_Path + file_sep + curr_name, False)
            print(iterU + " has " + str(len(curr_table)) + " Errors")

######################################################################################################
    def make_folder(self, folder):
        if os.path.isdir(folder):
            pass  # if folder exist do nothing
        else:
            os.makedirs(folder)
    def to_s3_csv(self, df, file_key, index_bool):
        csv_buffer = df.to_csv(index=index_bool).encode()
        self.s3_client.put_object(Body=csv_buffer, Bucket=self.bucket, Key=file_key)

    def split_into_error_files(self, file_sep):
        error_list = self.Error_list
        error_list = error_list.query("Column_Name not in ['Cohort'] and Column_Value not in ['NULL']")
        part_error_ids = []
        part_list = ['baseline.csv', 'follow_up.csv', 'covid_history.csv', 'covid_vaccination_status.csv', 'treatment_history.csv']
        part_errors = error_list.query("CSV_Sheet_Name in @part_list")
        part_errors = part_errors[["CSV_Sheet_Name", "Row_Index"]].drop_duplicates()
        for curr_file in part_list:
            if curr_file in self.Data_Object_Table:
                data_table = self.Data_Object_Table[curr_file]["Data_Table"]
                data_table.reset_index(inplace=True, drop=True)
                curr_error = part_errors.query("CSV_Sheet_Name == @curr_file")["Row_Index"].tolist()
                try:
                    if len(curr_error) > 0:
                        curr_error = [i-2 for i in curr_error]
                        part_error_ids = part_error_ids + data_table.iloc[curr_error]["Research_Participant_ID"].tolist()
                except Exception as e:
                    print(e)
        part_error_ids = list(set(part_error_ids))

        output_path = self.Data_Validation_Path.replace("Data_Validation_Results", "Split_Files")
        self.make_folder(output_path)
        for curr_file in part_list:
            if curr_file in self.Data_Object_Table:
                data_table = self.Data_Object_Table[curr_file]["Data_Table"]
                if "Research_Participant_ID" in data_table.columns:
                    good_data = data_table.query("Research_Participant_ID not in @part_error_ids")
                    bad_data = data_table.query("Research_Participant_ID in @part_error_ids")

                    #good_data.to_csv(output_path  + file_sep + "Good_" + curr_file, index=False)
                    #bad_data.to_csv(output_path + file_sep + "Bad_" + curr_file, index=False)
                    self.to_s3_csv(good_data, output_path  + file_sep + "Good_" + curr_file, False)
                    self.to_s3_csv(bad_data, output_path + file_sep + "Bad_" + curr_file, False)
